#!/usr/bin/env python3
"""Cloud Run Job entrypoint: collect daily OHLCV + run Claude event detection."""
from __future__ import annotations

import datetime
import logging
import os
import sys
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("driftwatch.daily")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from driftwatch.bq_client import BQClient
from driftwatch.pipelines import event_detection, ohlcv_daily


def main() -> int:
    client = BQClient()
    client.ensure_tables()

    run_date = datetime.date.today()
    run_id = str(uuid.uuid4())

    log.info("=== Daily run started | run_id=%s | date=%s ===", run_id, run_date)

    ohlcv_result = ohlcv_daily.run(run_date)
    log.info(
        "OHLCV: %d rows written, %d errors",
        ohlcv_result.rows_written,
        len(ohlcv_result.errors),
    )

    try:
        events = event_detection.run_ohlcv_detection(run_date, run_id)
        log.info("Events detected: %d", len(events))
    except Exception:
        log.exception("Event detection failed")
        return 1 if ohlcv_result.has_critical_errors else 0

    log.info("=== Daily run complete ===")
    return 1 if ohlcv_result.has_critical_errors else 0


if __name__ == "__main__":
    sys.exit(main())
