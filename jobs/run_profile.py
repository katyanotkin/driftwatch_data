#!/usr/bin/env python3
"""Cloud Run Job entrypoint: collect ETF profile snapshots + run Claude event detection.

Runs weekly via Cloud Scheduler but internally gates to a 4-week cadence.
"""
from __future__ import annotations

import datetime
import logging
import sys
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("driftwatch.profile")

import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from driftwatch.pipelines import event_detection, profile_snapshot
from driftwatch.bq_client import BQClient

client = BQClient()
client.ensure_tables()


def main() -> int:
    run_date = datetime.date.today()
    run_id = str(uuid.uuid4())
    log.info("=== Profile run started | run_id=%s | date=%s ===", run_id, run_date)

    if not profile_snapshot.should_run(run_date):
        log.info("=== Profile run skipped (4-week gate) ===")
        return 0

    profile_result = profile_snapshot.run(run_date)
    log.info(
        "Profile: %d rows written, %d errors",
        profile_result.rows_written,
        len(profile_result.errors),
    )

    events = event_detection.run_profile_detection(run_date, run_id)
    log.info("Events detected: %d", len(events))

    log.info("=== Profile run complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
