#!/usr/bin/env python3
"""CLI: add a manager_note event to ticker_events."""
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

    event = EventRow(
        event_id=str(uuid.uuid4()),
        symbol=args.symbol.upper(),
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
    written = bq.insert_events_manual([event])

    log.info(
        "Added manager_note for %s on %s (event_id=%s)",
        event.symbol,
        event.event_date,
        event.event_id,
    )
    return 0 if written else 1


if __name__ == "__main__":
    sys.exit(main())
