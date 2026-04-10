from __future__ import annotations

import datetime
import logging
from typing import Any

from driftwatch.bq_client import bq_client
from driftwatch.claude_client import claude_client
from driftwatch.models import EventRow, OHLCVRow, ProfileRow
from driftwatch.settings import load_tickers

log = logging.getLogger(__name__)


def run_ohlcv_detection(run_date: datetime.date, run_id: str) -> list[EventRow]:
    """Detect events by comparing today's OHLCV to the previous trading day."""
    symbols = load_tickers()
    today_rows = bq_client.get_ohlcv_for_date(run_date)
    prev_rows = bq_client.get_previous_ohlcv(run_date)

    if not prev_rows:
        log.info("No previous OHLCV data found — skipping event detection (first run?)")
        return []

    all_events: list[EventRow] = []
    for sym in symbols:
        current = today_rows.get(sym)
        previous = prev_rows.get(sym)
        if not current or not previous:
            log.debug("%s: missing current or previous OHLCV, skipping", sym)
            continue

        payload = _build_ohlcv_payload(sym, current, previous, run_date)
        events = claude_client.detect_events(payload, sym, run_id)
        all_events.extend(events)

    inserted = bq_client.insert_events(all_events)
    log.info("OHLCV detection: %d events detected, %d inserted", len(all_events), inserted)
    return all_events


def run_profile_detection(run_date: datetime.date, run_id: str) -> list[EventRow]:
    """Detect events by comparing current profile snapshot to the previous one."""
    symbols = load_tickers()
    current_rows = bq_client.get_profile_for_date(run_date)
    prev_rows = bq_client.get_previous_profile(run_date)

    if not prev_rows:
        log.info("No previous profile data found — skipping event detection (first run?)")
        return []

    all_events: list[EventRow] = []
    for sym in symbols:
        current = current_rows.get(sym)
        previous = prev_rows.get(sym)
        if not current or not previous:
            log.debug("%s: missing current or previous profile, skipping", sym)
            continue

        payload = _build_profile_payload(sym, current, previous, run_date)
        events = claude_client.detect_events(payload, sym, run_id)
        all_events.extend(events)

    inserted = bq_client.insert_events(all_events)
    log.info("Profile detection: %d events detected, %d inserted", len(all_events), inserted)
    return all_events


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def _build_ohlcv_payload(
    symbol: str,
    current: OHLCVRow,
    previous: OHLCVRow,
    run_date: datetime.date,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "detection_type": "ohlcv_daily",
        "current_date": run_date.isoformat(),
        "current": {
            "open": current.open,
            "high": current.high,
            "low": current.low,
            "close": current.close,
            "volume": current.volume,
            "avg_volume_30d": current.avg_volume_30d,
            "pe_ratio": current.pe_ratio,
        },
        "previous": {
            "trade_date": previous.trade_date.isoformat(),
            "open": previous.open,
            "high": previous.high,
            "low": previous.low,
            "close": previous.close,
            "volume": previous.volume,
            "avg_volume_30d": previous.avg_volume_30d,
            "pe_ratio": previous.pe_ratio,
        },
    }


def _build_profile_payload(
    symbol: str,
    current: ProfileRow,
    previous: ProfileRow,
    run_date: datetime.date,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "detection_type": "profile_snapshot",
        "current_date": run_date.isoformat(),
        "current_profile": {
            "net_assets": current.net_assets,
            "expense_ratio": current.expense_ratio,
            "benchmark": current.benchmark,
            "category": current.category,
            "fund_family": current.fund_family,
            "fifty_two_week_high": current.fifty_two_week_high,
            "fifty_two_week_low": current.fifty_two_week_low,
        },
        "previous_profile": {
            "snapshot_date": previous.snapshot_date.isoformat(),
            "net_assets": previous.net_assets,
            "expense_ratio": previous.expense_ratio,
            "benchmark": previous.benchmark,
            "category": previous.category,
            "fund_family": previous.fund_family,
            "fifty_two_week_high": previous.fifty_two_week_high,
            "fifty_two_week_low": previous.fifty_two_week_low,
        },
    }
