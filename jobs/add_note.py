#!/usr/bin/env python3
"""CLI to add manual manager_note events to etf_events.

Usage:
    python jobs/add_note.py SPY "rebalancing expected next week"
    python jobs/add_note.py QQQ "expense ratio change rumored" --date 2026-04-01
"""
from __future__ import annotations

import argparse
import datetime
import logging
import sys
import uuid

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("driftwatch.add_note")

import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from driftwatch.bq_client import bq_client
from driftwatch.models import EventRow
from driftwatch.settings import load_tickers


def main() -> int:
    parser = argparse.ArgumentParser(description="Add a manager_note event to DriftWatch.")
    parser.add_argument("symbol", help="ETF ticker symbol (e.g. SPY)")
    parser.add_argument("note", help="Free-text annotation")
    parser.add_argument(
        "--date",
        default=None,
        help="Event date in YYYY-MM-DD format (default: today)",
    )
    args = parser.parse_args()

    symbol = args.symbol.upper()
    valid_symbols = load_tickers()
    if symbol not in valid_symbols:
        log.warning(
            "%s is not in the configured symbol list. Proceeding anyway.", symbol
        )

    event_date = (
        datetime.date.fromisoformat(args.date) if args.date else datetime.date.today()
    )

    event = EventRow(
        event_id=str(uuid.uuid4()),
        symbol=symbol,
        event_date=event_date,
        event_type="manager_note",
        confidence_score=1.0,
        details=None,
        source="manual",
        detection_run_id="manual",
        notes=args.note,
    )

    inserted = bq_client.insert_events_manual([event])
    if inserted:
        log.info("Inserted manager_note for %s on %s: %s", symbol, event_date, args.note)
    else:
        log.error("Failed to insert event")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
