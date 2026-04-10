from __future__ import annotations

import datetime
import logging
import uuid
from typing import Optional

from google.cloud import bigquery

from driftwatch.models import EventRow, OHLCVRow, ProfileRow
from driftwatch.settings import settings

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_OHLCV_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.etf_ohlcv_daily`
(
  symbol          STRING    NOT NULL,
  trade_date      DATE      NOT NULL,
  open            FLOAT64,
  high            FLOAT64,
  low             FLOAT64,
  close           FLOAT64,
  volume          INT64,
  avg_volume_30d  FLOAT64,
  pe_ratio        FLOAT64,
  ingested_at     TIMESTAMP NOT NULL,
  data_source     STRING    NOT NULL
)
PARTITION BY trade_date
CLUSTER BY symbol
"""

_PROFILE_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.etf_profile_snapshot`
(
  symbol                STRING    NOT NULL,
  snapshot_date         DATE      NOT NULL,
  net_assets            FLOAT64,
  fifty_two_week_high   FLOAT64,
  fifty_two_week_low    FLOAT64,
  expense_ratio         FLOAT64,
  category              STRING,
  fund_family           STRING,
  benchmark             STRING,
  ingested_at           TIMESTAMP NOT NULL,
  data_source           STRING    NOT NULL
)
PARTITION BY snapshot_date
CLUSTER BY symbol
"""

_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.etf_events`
(
  event_id          STRING    NOT NULL,
  symbol            STRING    NOT NULL,
  event_date        DATE      NOT NULL,
  event_type        STRING    NOT NULL,
  confidence_score  FLOAT64,
  details           JSON,
  source            STRING    NOT NULL,
  detection_run_id  STRING    NOT NULL,
  detected_at       TIMESTAMP NOT NULL,
  notes             STRING
)
PARTITION BY event_date
CLUSTER BY symbol, event_type
"""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class BQClient:
    def __init__(self) -> None:
        self._client = bigquery.Client(project=settings.gcp_project_id)
        self._project = settings.gcp_project_id
        self._dataset = settings.gcp_dataset_id

    def _table(self, name: str) -> str:
        return f"`{self._project}.{self._dataset}.{name}`"

    def _tmp_table(self, name: str, run_id: str) -> str:
        short = run_id.replace("-", "")[:12]
        return f"`{self._project}.{self._dataset}._tmp_{name}_{short}`"

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def ensure_tables(self) -> None:
        """Create BQ dataset and all three tables if they don't exist."""
        dataset_ref = f"{self._project}.{self._dataset}"
        try:
            self._client.get_dataset(dataset_ref)
        except Exception:
            ds = bigquery.Dataset(dataset_ref)
            ds.location = settings.gcp_location
            self._client.create_dataset(ds, exists_ok=True)
            log.info("Created dataset %s", dataset_ref)

        for ddl in [_OHLCV_DDL, _PROFILE_DDL, _EVENTS_DDL]:
            sql = ddl.format(project=self._project, dataset=self._dataset)
            self._client.query(sql).result()
        log.info("Tables ensured in dataset %s", dataset_ref)

    # ------------------------------------------------------------------
    # OHLCV upsert
    # ------------------------------------------------------------------

    def upsert_ohlcv(self, rows: list[OHLCVRow], run_id: str) -> int:
        if not rows:
            return 0
        tmp = self._tmp_table("ohlcv", run_id)
        target = self._table("etf_ohlcv_daily")
        self._load_temp(tmp, [r.to_bq_dict() for r in rows], _ohlcv_schema())
        merge_sql = f"""
        MERGE {target} AS tgt
        USING {tmp} AS src
        ON tgt.symbol = src.symbol AND tgt.trade_date = src.trade_date
        WHEN MATCHED THEN UPDATE SET
          open = src.open, high = src.high, low = src.low, close = src.close,
          volume = src.volume, avg_volume_30d = src.avg_volume_30d,
          pe_ratio = src.pe_ratio, ingested_at = src.ingested_at,
          data_source = src.data_source
        WHEN NOT MATCHED THEN INSERT ROW
        """
        self._client.query(merge_sql).result()
        self._drop_temp(tmp)
        log.info("Upserted %d OHLCV rows", len(rows))
        return len(rows)

    # ------------------------------------------------------------------
    # Profile upsert
    # ------------------------------------------------------------------

    def upsert_profile(self, rows: list[ProfileRow], run_id: str) -> int:
        if not rows:
            return 0
        tmp = self._tmp_table("profile", run_id)
        target = self._table("etf_profile_snapshot")
        self._load_temp(tmp, [r.to_bq_dict() for r in rows], _profile_schema())
        merge_sql = f"""
        MERGE {target} AS tgt
        USING {tmp} AS src
        ON tgt.symbol = src.symbol AND tgt.snapshot_date = src.snapshot_date
        WHEN MATCHED THEN UPDATE SET
          net_assets = src.net_assets,
          fifty_two_week_high = src.fifty_two_week_high,
          fifty_two_week_low = src.fifty_two_week_low,
          expense_ratio = src.expense_ratio,
          category = src.category,
          fund_family = src.fund_family,
          benchmark = src.benchmark,
          ingested_at = src.ingested_at,
          data_source = src.data_source
        WHEN NOT MATCHED THEN INSERT ROW
        """
        self._client.query(merge_sql).result()
        self._drop_temp(tmp)
        log.info("Upserted %d profile rows", len(rows))
        return len(rows)

    # ------------------------------------------------------------------
    # Events insert (deduped)
    # ------------------------------------------------------------------

    def insert_events(self, events: list[EventRow]) -> int:
        if not events:
            return 0

        # Build dedup key set from existing rows for the same event dates
        dates = list({e.event_date.isoformat() for e in events})
        symbols = list({e.symbol for e in events})
        date_list = ", ".join(f"'{d}'" for d in dates)
        sym_list = ", ".join(f"'{s}'" for s in symbols)

        existing_sql = f"""
        SELECT symbol, event_date, event_type
        FROM {self._table("etf_events")}
        WHERE event_date IN ({date_list})
          AND symbol IN ({sym_list})
          AND source = 'claude_auto'
        """
        existing: set[tuple[str, str, str]] = set()
        for row in self._client.query(existing_sql).result():
            existing.add((row.symbol, str(row.event_date), row.event_type))

        new_events = [
            e for e in events
            if (e.symbol, e.event_date.isoformat(), e.event_type) not in existing
        ]
        if not new_events:
            log.info("No new events to insert (all duplicates)")
            return 0

        table_ref = self._client.dataset(self._dataset).table("etf_events")
        errors = self._client.insert_rows_json(
            table_ref, [e.to_bq_dict() for e in new_events]
        )
        if errors:
            log.error("BQ insert errors: %s", errors)
        log.info("Inserted %d events", len(new_events))
        return len(new_events)

    def insert_events_manual(self, events: list[EventRow]) -> int:
        """Insert manual (manager_note) events — no dedup check."""
        if not events:
            return 0
        table_ref = self._client.dataset(self._dataset).table("etf_events")
        errors = self._client.insert_rows_json(
            table_ref, [e.to_bq_dict() for e in events]
        )
        if errors:
            log.error("BQ insert errors: %s", errors)
        return len(events)

    # ------------------------------------------------------------------
    # Queries used by pipelines
    # ------------------------------------------------------------------

    def get_ohlcv_for_date(self, trade_date: datetime.date) -> dict[str, OHLCVRow]:
        sql = f"""
        SELECT * FROM {self._table("etf_ohlcv_daily")}
        WHERE trade_date = '{trade_date.isoformat()}'
        """
        result: dict[str, OHLCVRow] = {}
        for row in self._client.query(sql).result():
            d = dict(row)
            result[d["symbol"]] = OHLCVRow(**d)
        return result

    def get_previous_ohlcv(self, before_date: datetime.date) -> dict[str, OHLCVRow]:
        """Get the most recent OHLCV row per symbol before before_date."""
        sql = f"""
        SELECT * EXCEPT(rn) FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
          FROM {self._table("etf_ohlcv_daily")}
          WHERE trade_date < '{before_date.isoformat()}'
        ) WHERE rn = 1
        """
        result: dict[str, OHLCVRow] = {}
        for row in self._client.query(sql).result():
            d = dict(row)
            result[d["symbol"]] = OHLCVRow(**d)
        return result

    def get_profile_for_date(self, snapshot_date: datetime.date) -> dict[str, ProfileRow]:
        sql = f"""
        SELECT * FROM {self._table("etf_profile_snapshot")}
        WHERE snapshot_date = '{snapshot_date.isoformat()}'
        """
        result: dict[str, ProfileRow] = {}
        for row in self._client.query(sql).result():
            d = dict(row)
            result[d["symbol"]] = ProfileRow(**d)
        return result

    def get_previous_profile(self, before_date: datetime.date) -> dict[str, ProfileRow]:
        """Get the most recent profile snapshot per symbol before before_date."""
        sql = f"""
        SELECT * EXCEPT(rn) FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY snapshot_date DESC) AS rn
          FROM {self._table("etf_profile_snapshot")}
          WHERE snapshot_date < '{before_date.isoformat()}'
        ) WHERE rn = 1
        """
        result: dict[str, ProfileRow] = {}
        for row in self._client.query(sql).result():
            d = dict(row)
            result[d["symbol"]] = ProfileRow(**d)
        return result

    def get_last_snapshot_date(self) -> Optional[datetime.date]:
        sql = f"SELECT MAX(snapshot_date) AS last_date FROM {self._table('etf_profile_snapshot')}"
        for row in self._client.query(sql).result():
            return row.last_date
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_temp(
        self, tmp_table: str, rows: list[dict], schema: list[bigquery.SchemaField]
    ) -> None:
        # Strip backticks for the API reference
        table_id = tmp_table.strip("`")
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )
        self._client.load_table_from_json(rows, table_id, job_config=job_config).result()

    def _drop_temp(self, tmp_table: str) -> None:
        table_id = tmp_table.strip("`")
        self._client.delete_table(table_id, not_found_ok=True)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _ohlcv_schema() -> list[bigquery.SchemaField]:
    F = bigquery.SchemaField
    return [
        F("symbol", "STRING", mode="REQUIRED"),
        F("trade_date", "DATE", mode="REQUIRED"),
        F("open", "FLOAT64"),
        F("high", "FLOAT64"),
        F("low", "FLOAT64"),
        F("close", "FLOAT64"),
        F("volume", "INT64"),
        F("avg_volume_30d", "FLOAT64"),
        F("pe_ratio", "FLOAT64"),
        F("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        F("data_source", "STRING", mode="REQUIRED"),
    ]


def _profile_schema() -> list[bigquery.SchemaField]:
    F = bigquery.SchemaField
    return [
        F("symbol", "STRING", mode="REQUIRED"),
        F("snapshot_date", "DATE", mode="REQUIRED"),
        F("net_assets", "FLOAT64"),
        F("fifty_two_week_high", "FLOAT64"),
        F("fifty_two_week_low", "FLOAT64"),
        F("expense_ratio", "FLOAT64"),
        F("category", "STRING"),
        F("fund_family", "STRING"),
        F("benchmark", "STRING"),
        F("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        F("data_source", "STRING", mode="REQUIRED"),
    ]


# Module-level singleton
bq_client = BQClient()
