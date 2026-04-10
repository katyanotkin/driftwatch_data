from __future__ import annotations

import datetime
import logging

from driftwatch.bq_client import bq_client
from driftwatch.models import PipelineResult
from driftwatch.settings import load_tickers, settings
from driftwatch.yf_client import fetch_all_profiles

log = logging.getLogger(__name__)


def should_run(today: datetime.date) -> bool:
    """Return True if enough days have elapsed since the last snapshot."""
    last = bq_client.get_last_snapshot_date()
    if last is None:
        return True
    days_since = (today - last).days
    if days_since < settings.profile_snapshot_interval_days:
        log.info(
            "Profile snapshot skipped: %d days since last run (threshold: %d)",
            days_since,
            settings.profile_snapshot_interval_days,
        )
        return False
    return True


def run(run_date: datetime.date, run_id: str) -> PipelineResult:
    symbols = load_tickers()
    log.info("Fetching profile snapshots for %d symbols on %s", len(symbols), run_date)

    rows = fetch_all_profiles(symbols, run_date)
    errors: list[str] = []

    failed = set(symbols) - {r.symbol for r in rows}
    for sym in failed:
        errors.append(f"{sym}: no profile data for {run_date}")
        log.warning("%s: no profile data returned", sym)

    written = bq_client.upsert_profile(rows, run_id)
    return PipelineResult(rows_written=written, errors=errors)
