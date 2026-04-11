from __future__ import annotations

import datetime
import logging
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from driftwatch.models import EventRow, OHLCVRow, ProfileRow
from driftwatch.settings import settings

log = logging.getLogger(__name__)


class BQClient:
    """Thin BigQuery wrapper for DriftWatch storage operations."""

    OHLCV_TABLE = "etf_daily_ohlcv"
    PROFILE_TABLE = "etf_profile_snapshot"
    EVENTS_TABLE = "etf_events"

    def __init__(self) -> None:
        self._client = bigquery.Client(project=settings.gcp_project_id)
        self._project = settings.gcp_project_id
        self._dataset = settings.gcp_dataset_id
        self._location = settings.gcp_location

    @property
    def dataset_id(self) -> str:
        return f"{self._project}.{self._dataset}"

    def _qualified_table_id(self, table_name: str) -> str:
        return f"{self.dataset_id}.{table_name}"

    def _sql_table_ref(self, table_name: str) -> str:
        return f"`{self._qualified_table_id(table_name)}`"

    # ------------------------------------------------------------------
    # Dataset / table bootstrap
    # ------------------------------------------------------------------

    def ensure_tables(self) -> None:
        self._ensure_dataset_exists()
        self._ensure_table_exists(
            table_id=self._qualified_table_id(self.OHLCV_TABLE),
            schema=_ohlcv_schema(),
            partition_field="trade_date",
            clustering_fields=["symbol"],
        )
        self._ensure_table_exists(
            table_id=self._qualified_table_id(self.PROFILE_TABLE),
            schema=_profile_schema(),
            partition_field="snapshot_date",
            clustering_fields=["symbol"],
        )
        self._ensure_table_exists(
            table_id=self._qualified_table_id(self.EVENTS_TABLE),
            schema=_events_schema(),
            partition_field="event_date",
            clustering_fields=["symbol", "event_type"],
        )
        log.info("Ensured dataset and tables in %s", self.dataset_id)

    def _ensure_dataset_exists(self) -> None:
        try:
            self._client.get_dataset(self.dataset_id)
            return
        except NotFound:
            pass

        dataset = bigquery.Dataset(self.dataset_id)
        dataset.location = self._location
        self._client.create_dataset(dataset, exists_ok=True)
        log.info("Created dataset %s", self.dataset_id)

    def _ensure_table_exists(
        self,
        table_id: str,
        schema: list[bigquery.SchemaField],
        *,
        partition_field: str,
        clustering_fields: list[str] | None = None,
    ) -> None:
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
        if clustering_fields:
            table.clustering_fields = clustering_fields

        self._client.create_table(table)
        log.info("Created table %s", table_id)


    # ------------------------------------------------------------------
    # Events insert
    # ------------------------------------------------------------------

    def insert_events(self, events: list[EventRow]) -> int:
        """Insert auto-detected events, deduping by symbol/date/type/source."""
        if not events:
            return 0

        filtered_events = [event for event in events if event.source == "claude_auto"]
        if not filtered_events:
            return 0

        distinct_dates = sorted({event.event_date.isoformat() for event in filtered_events})
        distinct_symbols = sorted({event.symbol for event in filtered_events})

        existing_sql = f"""
        SELECT symbol, event_date, event_type, source
        FROM {self._sql_table_ref(self.EVENTS_TABLE)}
        WHERE event_date IN UNNEST(@dates)
          AND symbol IN UNNEST(@symbols)
          AND source = 'claude_auto'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("dates", "DATE", distinct_dates),
                bigquery.ArrayQueryParameter("symbols", "STRING", distinct_symbols),
            ]
        )

        existing_keys: set[tuple[str, str, str, str]] = set()
        for row in self._client.query(existing_sql, job_config=job_config).result():
            existing_keys.add(
                (row.symbol, row.event_date.isoformat(), row.event_type, row.source)
            )

        rows_to_insert = [
            event
            for event in filtered_events
            if (
                event.symbol,
                event.event_date.isoformat(),
                event.event_type,
                event.source,
            ) not in existing_keys
        ]

        if not rows_to_insert:
            log.info("No new auto-detected events to insert")
            return 0

        errors = self._client.insert_rows_json(
            self._qualified_table_id(self.EVENTS_TABLE),
            [event.to_bq_dict() for event in rows_to_insert],
        )
        if errors:
            raise RuntimeError(f"BigQuery insert errors for events: {errors}")

        log.info("Inserted %d auto-detected events", len(rows_to_insert))
        return len(rows_to_insert)

    def insert_events_manual(self, events: list[EventRow]) -> int:
        """Insert manual events without dedupe."""
        if not events:
            return 0

        errors = self._client.insert_rows_json(
            self._qualified_table_id(self.EVENTS_TABLE),
            [event.to_bq_dict() for event in events],
        )
        if errors:
            raise RuntimeError(f"BigQuery insert errors for manual events: {errors}")

        log.info("Inserted %d manual events", len(events))
        return len(events)
    # -------------------------------
    # Replace (delete + insert)
    # -------------------------------
    def replace_ohlcv_rows(self, rows: list[OHLCVRow]) -> int:
        """Replace OHLCV rows for the affected trade dates."""
        if not rows:
            return 0

        trade_dates = sorted({row.trade_date for row in rows})
        self._delete_partitions_by_dates(
            table_name=self.OHLCV_TABLE,
            date_field="trade_date",
            dates=trade_dates,
        )

        errors = self._client.insert_rows_json(
            self._qualified_table_id(self.OHLCV_TABLE),
            [row.to_bq_dict() for row in rows],
        )
        if errors:
            raise RuntimeError(f"BigQuery insert errors for OHLCV: {errors}")

        log.info(
            "Replaced %d OHLCV rows across %d trade dates",
            len(rows),
            len(trade_dates),
        )
        return len(rows)


    def replace_profile_rows(self, rows: list[ProfileRow]) -> int:
        """Replace profile rows for the affected snapshot dates."""
        if not rows:
            return 0

        snapshot_dates = sorted({row.snapshot_date for row in rows})
        self._delete_partitions_by_dates(
            table_name=self.PROFILE_TABLE,
            date_field="snapshot_date",
            dates=snapshot_dates,
        )

        errors = self._client.insert_rows_json(
            self._qualified_table_id(self.PROFILE_TABLE),
            [row.to_bq_dict() for row in rows],
        )
        if errors:
            raise RuntimeError(f"BigQuery insert errors for profile rows: {errors}")

        log.info(
            "Replaced %d profile rows across %d snapshot dates",
            len(rows),
            len(snapshot_dates),
        )
        return len(rows)


    def _delete_partitions_by_dates(
        self,
        table_name: str,
        date_field: str,
        dates: list[datetime.date],
    ) -> None:
        """Delete existing rows for the supplied partition dates."""
        if not dates:
            return

        sql = f"""
        DELETE FROM {self._sql_table_ref(table_name)}
        WHERE {date_field} IN UNNEST(@dates)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "dates",
                    "DATE",
                    [date_value.isoformat() for date_value in dates],
                )
            ]
        )
        self._client.query(sql, job_config=job_config).result()

        log.info(
            "Deleted existing rows from %s for %d %s values",
            table_name,
            len(dates),
            date_field,
        )

    # ------------------------------------------------------------------
    # Query helpers used by pipelines
    # ------------------------------------------------------------------

    def get_ohlcv_for_date(self, trade_date: datetime.date) -> dict[str, OHLCVRow]:
        sql = f"""
        SELECT *
        FROM {self._sql_table_ref(self.OHLCV_TABLE)}
        WHERE trade_date = @trade_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("trade_date", "DATE", trade_date.isoformat())
            ]
        )
        result: dict[str, OHLCVRow] = {}
        for row in self._client.query(sql, job_config=job_config).result():
            result[row["symbol"]] = OHLCVRow(**dict(row))
        return result

    def get_previous_ohlcv(self, before_date: datetime.date) -> dict[str, OHLCVRow]:
        sql = f"""
        SELECT * EXCEPT (row_num)
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY symbol
                   ORDER BY trade_date DESC
                 ) AS row_num
          FROM {self._sql_table_ref(self.OHLCV_TABLE)}
          WHERE trade_date < @before_date
        )
        WHERE row_num = 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("before_date", "DATE", before_date.isoformat())
            ]
        )
        result: dict[str, OHLCVRow] = {}
        for row in self._client.query(sql, job_config=job_config).result():
            result[row["symbol"]] = OHLCVRow(**dict(row))
        return result

    def get_profile_for_date(self, snapshot_date: datetime.date) -> dict[str, ProfileRow]:
        sql = f"""
        SELECT *
        FROM {self._sql_table_ref(self.PROFILE_TABLE)}
        WHERE snapshot_date = @snapshot_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "snapshot_date",
                    "DATE",
                    snapshot_date.isoformat(),
                )
            ]
        )
        result: dict[str, ProfileRow] = {}
        for row in self._client.query(sql, job_config=job_config).result():
            result[row["symbol"]] = ProfileRow(**dict(row))
        return result

    def get_previous_profile(self, before_date: datetime.date) -> dict[str, ProfileRow]:
        sql = f"""
        SELECT * EXCEPT (row_num)
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY symbol
                   ORDER BY snapshot_date DESC
                 ) AS row_num
          FROM {self._sql_table_ref(self.PROFILE_TABLE)}
          WHERE snapshot_date < @before_date
        )
        WHERE row_num = 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("before_date", "DATE", before_date.isoformat())
            ]
        )
        result: dict[str, ProfileRow] = {}
        for row in self._client.query(sql, job_config=job_config).result():
            result[row["symbol"]] = ProfileRow(**dict(row))
        return result

    def get_last_snapshot_date(self) -> Optional[datetime.date]:
        sql = f"""
        SELECT MAX(snapshot_date) AS last_date
        FROM {self._sql_table_ref(self.PROFILE_TABLE)}
        """
        for row in self._client.query(sql).result():
            return row["last_date"]
        return None


def _ohlcv_schema() -> list[bigquery.SchemaField]:
    field = bigquery.SchemaField
    return [
        field("symbol", "STRING", mode="REQUIRED"),
        field("trade_date", "DATE", mode="REQUIRED"),
        field("open", "FLOAT64"),
        field("high", "FLOAT64"),
        field("low", "FLOAT64"),
        field("close", "FLOAT64"),
        field("adjusted_close", "FLOAT64"),
        field("volume", "INT64"),
        field("avg_volume_30d", "FLOAT64"),
        field("pe_ratio", "FLOAT64"),
        field("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        field("data_source", "STRING", mode="REQUIRED"),
    ]


def _profile_schema() -> list[bigquery.SchemaField]:
    field = bigquery.SchemaField
    return [
        field("symbol", "STRING", mode="REQUIRED"),
        field("snapshot_date", "DATE", mode="REQUIRED"),
        field("net_assets", "FLOAT64"),
        field("fifty_two_week_high", "FLOAT64"),
        field("fifty_two_week_low", "FLOAT64"),
        field("expense_ratio", "FLOAT64"),
        field("category", "STRING"),
        field("fund_family", "STRING"),
        field("benchmark", "STRING"),
        field("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        field("data_source", "STRING", mode="REQUIRED"),
    ]


def _events_schema() -> list[bigquery.SchemaField]:
    field = bigquery.SchemaField
    return [
        field("event_id", "STRING", mode="REQUIRED"),
        field("symbol", "STRING", mode="REQUIRED"),
        field("event_date", "DATE", mode="REQUIRED"),
        field("event_type", "STRING", mode="REQUIRED"),
        field("confidence_score", "FLOAT64"),
        field("details", "STRING"),
        field("source", "STRING", mode="REQUIRED"),
        field("detection_run_id", "STRING", mode="REQUIRED"),
        field("detected_at", "TIMESTAMP", mode="REQUIRED"),
        field("notes", "STRING"),
    ]


bq_client = BQClient()
