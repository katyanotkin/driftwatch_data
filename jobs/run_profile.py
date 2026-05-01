#!/usr/bin/env python3
"""Cloud Run Job: profile snapshot + GICS reclassification detection (~every 6 weeks)."""
from __future__ import annotations

import datetime
import json
import logging
import os
import sys
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("sigforge.profile")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigforge.bq_client import BQClient
from sigforge.models import EventRow
from sigforge.settings import load_symbols, load_tickers
from sigforge.yf_client import clear_cache, fetch_profile

_GICS_FIELDS = (
    "gics_sector",
    "gics_industry_group",
    "gics_industry",
    "gics_sub_industry",
)


def main() -> int:
    snapshot_date = datetime.date.today()
    run_id = str(uuid.uuid4())
    log.info("=== sigforge profile run | date=%s run_id=%s ===", snapshot_date, run_id)

    symbols = load_tickers()
    if not symbols:
        log.error("No symbols configured — check config/symbols.yaml")
        return 1

    bq = BQClient()
    bq.ensure_tables()

    # --- Fetch profiles ---
    all_symbols = load_symbols()
    profile_rows = []
    for sym in symbols:
        row = fetch_profile(sym, snapshot_date)
        if row:
            _backfill_gics(row, sym, all_symbols)
            profile_rows.append(row)

    if not profile_rows:
        log.error("No profile rows fetched")
        return 1

    bq.upsert_profile(profile_rows)
    log.info("Profile: %d rows written", len(profile_rows))

    # --- GICS reclassification detection ---
    prev_profiles = bq.get_previous_profile(before_date=snapshot_date)
    events: list[EventRow] = []

    for row in profile_rows:
        prev = prev_profiles.get(row.symbol)
        if prev is None:
            continue
        for field in _GICS_FIELDS:
            old_val = getattr(prev, field)
            new_val = getattr(row, field)
            if old_val and new_val and old_val != new_val:
                events.append(
                    EventRow(
                        symbol=row.symbol,
                        event_date=snapshot_date,
                        event_type="gics_reclassification",
                        confidence_score=1.0,
                        details=json.dumps(
                            {"field": field, "from": old_val, "to": new_val}
                        ),
                        source="claude_auto",
                        detection_run_id=run_id,
                    )
                )
                log.info(
                    "%s: GICS reclassification — %s: %s → %s",
                    row.symbol, field, old_val, new_val,
                )

    if events:
        bq.upsert_events(events)
        log.info("Events: %d gics_reclassification events written", len(events))

    clear_cache()
    log.info("=== sigforge profile run complete ===")
    return 0


def _backfill_gics(row, symbol: str, all_symbols: list[dict]) -> None:
    """Fill missing GICS fields from symbols.yaml when yfinance omits them."""
    sym_cfg = next((s for s in all_symbols if s["ticker"] == symbol), None)
    if not sym_cfg:
        return
    for field in _GICS_FIELDS:
        if not getattr(row, field) and sym_cfg.get(field):
            setattr(row, field, sym_cfg[field])


if __name__ == "__main__":
    sys.exit(main())
