#!/usr/bin/env python3
"""CLI: add a manager_note event to ticker_events.

Re-running with identical arguments is idempotent: the event_id is derived
deterministically from (symbol, date, note) so the second run is a no-op.
"""
from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("sigforge.add_note")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigforge.bq_client import BQClient
from sigforge.models import EventRow

_NAMESPACE = uuid.UUID("b7e2a1c0-d4f5-4e6b-8a3c-1f2e9d0b5c7a")


def _stable_event_id(symbol: str, event_date: datetime.date, note: str) -> str:
    """Deterministic UUID so re-running with the same args is idempotent."""
    key = f"{symbol}|{event_date.isoformat()}|{note}"
    return str(uuid.uuid5(_NAMESPACE, key))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Add a manager note to ticker_events.")
    p.add_argument("symbol", help="Ticker symbol (e.g. AAPL)")
    p.add_argument("note", help="Note text")
    p.add_argument(
        "--date",
        type=datetime.date.fromisoformat,
        default=datetime.date.today(),
        metavar="YYYY-MM-DD",
        help="Event date (default: today)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    symbol = args.symbol.upper()

    event = EventRow(
        event_id=_stable_event_id(symbol, args.date, args.note),
        symbol=symbol,
        event_date=args.date,
        event_type="manager_note",
        confidence_score=1.0,
        details=None,
        notes=args.note,
        source="manual",
        detection_run_id="manual",
    )

    bq = BQClient()
    bq.ensure_tables()
    written = bq.insert_events([event])

    if written:
        log.info(
            "Added manager_note for %s on %s (event_id=%s)",
            event.symbol, event.event_date, event.event_id,
        )
    else:
        log.info(
            "manager_note already exists for %s on %s — skipped",
            event.symbol, event.event_date,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
