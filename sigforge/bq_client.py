from __future__ import annotations

import datetime
import logging
from typing import Type

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from sigforge.models import EventRow, FeatureRow, ProfileRow, RawBar
from sigforge.settings import settings

log = logging.getLogger(__name__)

# BQ type mapping for pydantic field annotations
_PY_TO_BQ: dict[str, str] = {
    "str": "STRING",
    "float": "FLOAT64",
    "int": "INT64",
    "bool": "BOOL",
    "date": "DATE",
    "datetime": "TIMESTAMP",
}

DAILY_TABLE = "ticker_daily"
PROFILE_TABLE = "ticker_profile"
FEATURES_TABLE = "ticker_features"
EVENTS_TABLE = "ticker_events"


def _schema_from_model(model_cls: Type) -> list[bigquery.SchemaField]:
    """Derive BQ schema from a pydantic model's field annotations."""
    import types

    schema: list[bigquery.SchemaField] = []
    for name, field_info in model_cls.model_fields.items():
        ann = field_info.annotation
        # Unwrap Optional[X] → X
        origin = getattr(ann, "__origin__", None)
        args = getattr(ann, "__args__", ())
        nullable = False
        union_origins = ("<class 'types.UnionType'>", "typing.Union")
        if origin is types.UnionType or str(origin) in union_origins:
            non_none = [a for a in args if a is not type(None)]
            nullable = type(None) in args
            ann = non_none[0] if non_none else ann
        elif origin is None and hasattr(ann, "__args__"):
            # typing.Optional
            non_none = [a for a in ann.__args__ if a is not type(None)]
            nullable = True
            ann = non_none[0] if non_none else ann

        # Literal[...] types should map to STRING
        import typing
        if getattr(ann, "__origin__", None) is typing.Literal:
            bq_type = "STRING"
        else:
            type_name = getattr(ann, "__name__", str(ann))
            bq_type = _PY_TO_BQ.get(type_name, "STRING")
        mode = "NULLABLE" if nullable else "REQUIRED"
        schema.append(bigquery.SchemaField(name, bq_type, mode=mode))
    return schema


class BQClient:
    def __init__(self) -> None:
        self._client = bigquery.Client(project=settings.gcp_project_id)
        self._project = settings.gcp_project_id
        self._dataset = settings.gcp_dataset_id
        self._location = settings.gcp_location

    @property
    def dataset_ref(self) -> str:
        return f"{self._project}.{self._dataset}"

    def _table(self, name: str) -> str:
        return f"{self.dataset_ref}.{name}"

    def _sql_ref(self, name: str) -> str:
        return f"`{self._table(name)}`"

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def ensure_tables(self) -> None:
        self._ensure_dataset()
        self._ensure_table(
            DAILY_TABLE,
            _schema_from_model(RawBar),
            partition_field="trade_date",
            clustering=["symbol"],
        )
        self._ensure_table(
            PROFILE_TABLE,
            _schema_from_model(ProfileRow),
            partition_field="snapshot_date",
            clustering=["symbol"],
        )
        self._ensure_table(
            FEATURES_TABLE,
            _schema_from_model(FeatureRow),
            partition_field="feature_date",
            clustering=["symbol"],
        )
        self._ensure_table(
            EVENTS_TABLE,
            _schema_from_model(EventRow),
            partition_field="event_date",
            clustering=["symbol", "event_type"],
        )
        log.info("BQ tables ready in %s", self.dataset_ref)

    def _ensure_dataset(self) -> None:
        try:
            self._client.get_dataset(self.dataset_ref)
            return
        except NotFound:
            pass
        ds = bigquery.Dataset(self.dataset_ref)
        ds.location = self._location
        self._client.create_dataset(ds, exists_ok=True)
        log.info("Created BQ dataset %s", self.dataset_ref)

    def _ensure_table(
        self,
        name: str,
        schema: list[bigquery.SchemaField],
        *,
        partition_field: str,
        clustering: list[str] | None = None,
    ) -> None:
        table_id = self._table(name)
        try:
            self._client.get_table(table_id)
            return
        except NotFound:
            pass
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        if clustering:
            table.clustering_fields = clustering
        self._client.create_table(table)
        log.info("Created BQ table %s", table_id)

    # ------------------------------------------------------------------
    # Upsert: ticker_daily
    # ------------------------------------------------------------------

    def upsert_daily(self, rows: list[RawBar]) -> int:
        if not rows:
            return 0
        dates = sorted({r.trade_date for r in rows})
        self._delete_by_dates(DAILY_TABLE, "trade_date", dates)
        errors = self._client.insert_rows_json(
            self._table(DAILY_TABLE),
            [r.to_bq_dict() for r in rows],
        )
        if errors:
            raise RuntimeError(f"BQ insert errors ticker_daily: {errors}")
        log.info("Upserted %d ticker_daily rows across %d dates", len(rows), len(dates))
        return len(rows)

    # ------------------------------------------------------------------
    # Upsert: ticker_profile
    # ------------------------------------------------------------------

    def upsert_profile(self, rows: list[ProfileRow]) -> int:
        if not rows:
            return 0
        dates = sorted({r.snapshot_date for r in rows})
        self._delete_by_dates(PROFILE_TABLE, "snapshot_date", dates)
        errors = self._client.insert_rows_json(
            self._table(PROFILE_TABLE),
            [r.to_bq_dict() for r in rows],
        )
        if errors:
            raise RuntimeError(f"BQ insert errors ticker_profile: {errors}")
        log.info("Upserted %d ticker_profile rows across %d dates", len(rows), len(dates))
        return len(rows)

    def get_previous_profile(self, before_date: datetime.date) -> dict[str, ProfileRow]:
        sql = f"""
        SELECT * EXCEPT (row_num)
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY symbol ORDER BY snapshot_date DESC
                 ) AS row_num
          FROM {self._sql_ref(PROFILE_TABLE)}
          WHERE snapshot_date < @before_date
        )
        WHERE row_num = 1
        """
        cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("before_date", "DATE", before_date.isoformat())
            ]
        )
        result: dict[str, ProfileRow] = {}
        for row in self._client.query(sql, job_config=cfg).result():
            d = dict(row)
            result[d["symbol"]] = ProfileRow(**d)
        return result

    # ------------------------------------------------------------------
    # Upsert: ticker_features
    # ------------------------------------------------------------------

    def upsert_features(self, rows: list[FeatureRow]) -> int:
        if not rows:
            return 0
        dates = sorted({r.feature_date for r in rows})
        self._delete_by_dates(FEATURES_TABLE, "feature_date", dates)
        errors = self._client.insert_rows_json(
            self._table(FEATURES_TABLE),
            [r.to_bq_dict() for r in rows],
        )
        if errors:
            raise RuntimeError(f"BQ insert errors ticker_features: {errors}")
        log.info("Upserted %d ticker_features rows across %d dates", len(rows), len(dates))
        return len(rows)

    # ------------------------------------------------------------------
    # Upsert: ticker_events
    # ------------------------------------------------------------------

    def upsert_events(self, rows: list[EventRow]) -> int:
        """Upsert by event_id: skip events whose ID already exists."""
        if not rows:
            return 0

        existing_ids = self._get_existing_event_ids({r.event_id for r in rows})
        new_rows = [r for r in rows if r.event_id not in existing_ids]

        if not new_rows:
            log.info("All %d events already exist — skipping", len(rows))
            return 0

        errors = self._client.insert_rows_json(
            self._table(EVENTS_TABLE),
            [r.to_bq_dict() for r in new_rows],
        )
        if errors:
            raise RuntimeError(f"BQ insert errors ticker_events: {errors}")
        log.info("Inserted %d ticker_events rows", len(new_rows))
        return len(new_rows)

    def insert_events_manual(self, rows: list[EventRow]) -> int:
        """Insert manual events without dedup."""
        if not rows:
            return 0
        errors = self._client.insert_rows_json(
            self._table(EVENTS_TABLE),
            [r.to_bq_dict() for r in rows],
        )
        if errors:
            raise RuntimeError(f"BQ insert errors (manual events): {errors}")
        log.info("Inserted %d manual events", len(rows))
        return len(rows)

    def _get_existing_event_ids(self, ids: set[str]) -> set[str]:
        if not ids:
            return set()
        sql = f"""
        SELECT event_id
        FROM {self._sql_ref(EVENTS_TABLE)}
        WHERE event_id IN UNNEST(@ids)
        """
        cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", list(ids))]
        )
        return {row["event_id"] for row in self._client.query(sql, job_config=cfg).result()}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _delete_by_dates(
        self,
        table: str,
        date_field: str,
        dates: list[datetime.date],
    ) -> None:
        if not dates:
            return
        sql = f"""
        DELETE FROM {self._sql_ref(table)}
        WHERE {date_field} IN UNNEST(@dates)
        """
        cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "dates", "DATE", [d.isoformat() for d in dates]
                )
            ]
        )
        self._client.query(sql, job_config=cfg).result()
        log.debug("Deleted %s rows for %d %s values", table, len(dates), date_field)
