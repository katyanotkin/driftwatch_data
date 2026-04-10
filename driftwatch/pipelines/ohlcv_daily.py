from __future__ import annotations

import datetime
import logging

from driftwatch.bq_client import bq_client
from driftwatch.models import PipelineResult
from driftwatch.settings import load_tickers
from driftwatch.yf_client import fetch_all_ohlcv

log = logging.getLogger(__name__)


def run(run_date: datetime.date, run_id: str) -> PipelineResult:
    symbols = load_tickers()
    log.info("Fetching OHLCV for %d symbols on %s", len(symbols), run_date)

    rows = fetch_all_ohlcv(symbols, run_date)
    errors: list[str] = []

    failed = set(symbols) - {r.symbol for r in rows}
    for sym in failed:
        errors.append(f"{sym}: no OHLCV data for {run_date}")
        log.warning("%s: no OHLCV data returned", sym)

    written = bq_client.upsert_ohlcv(rows, run_id)
    return PipelineResult(rows_written=written, errors=errors)
