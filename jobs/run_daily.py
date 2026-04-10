#!/usr/bin/env python3
"""Cloud Run Job entrypoint: collect daily OHLCV + run Claude event detection."""
from __future__ import annotations

import datetime
import logging
import sys
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("driftwatch.daily")

# Ensure repo root is on path when run directly
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from driftwatch.pipelines import event_detection, ohlcv_daily


def main() -> int:
    run_date = datetime.date.today()
    run_id = str(uuid.uuid4())
    log.info("=== Daily run started | run_id=%s | date=%s ===", run_id, run_date)

    ohlcv_result = ohlcv_daily.run(run_date, run_id)
    log.info("OHLCV: %d rows written, %d errors", ohlcv_result.rows_written, len(ohlcv_result.errors))

    events = event_detection.run_ohlcv_detection(run_date, run_id)
    log.info("Events detected: %d", len(events))

    log.info("=== Daily run complete ===")
    return 1 if ohlcv_result.has_critical_errors else 0


if __name__ == "__main__":
    sys.exit(main())
