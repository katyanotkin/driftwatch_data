#!/usr/bin/env python3
"""Backfill OHLCV history to CSV, BigQuery, or both."""
from __future__ import annotations

import argparse
import csv
import datetime
import logging
import os
import sys
from typing import Iterable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("driftwatch.backfill")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from driftwatch.bq_client import BQClient
from driftwatch.models import OHLCVRow
from driftwatch.settings import load_tickers
from driftwatch.yf_client import fetch_ohlcv_history_range_batch


_EXCLUDE = {"ingested_at", "data_source"}
_FIELDNAMES = [
    field_name for field_name in OHLCVRow.model_fields if field_name not in _EXCLUDE
]


def parse_args() -> argparse.Namespace:
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    default_start = yesterday - datetime.timedelta(days=30)

    parser = argparse.ArgumentParser(
        description="Backfill ETF OHLCV history to CSV, BigQuery, or both.",
    )
    parser.add_argument(
        "--start-date",
        type=datetime.date.fromisoformat,
        default=default_start,
        help="Inclusive start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        type=datetime.date.fromisoformat,
        default=yesterday,
        help="Inclusive end date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--target",
        choices=["csv", "bq", "both"],
        default="bq",
        help="Where to write the backfill output.",
    )
    parser.add_argument(
        "--out-csv",
        default="data/backfill.csv",
        help="CSV path when target is csv or both.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional explicit symbol list. If omitted, load from settings.",
    )
    return parser.parse_args()


def write_csv(rows: Iterable[OHLCVRow], out_csv: str) -> int:
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)

    row_count = 0
    with open(out_csv, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=_FIELDNAMES)
        writer.writeheader()

        for row in rows:
            writer.writerow(row.to_csv_dict())
            row_count += 1

    return row_count


def main() -> int:
    args = parse_args()

    if args.start_date > args.end_date:
        raise ValueError("start-date must be <= end-date")

    symbols = args.symbols or load_tickers()
    if not symbols:
        raise ValueError("No symbols provided and load_tickers() returned empty")

    log.info(
        "Backfill start | symbols=%d target=%s start=%s end=%s",
        len(symbols),
        args.target,
        args.start_date,
        args.end_date,
    )

    all_rows = fetch_ohlcv_history_range_batch(
        symbols=symbols,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    if not all_rows:
        log.error("No rows fetched for any symbol")
        return 1

    symbols_with_data = {row.symbol for row in all_rows}
    symbols_with_no_data = sorted(set(symbols) - symbols_with_data)
    total_symbols_with_no_data = len(symbols_with_no_data)

    if symbols_with_no_data:
        log.warning(
            "No data returned for %d symbols: %s",
            total_symbols_with_no_data,
            ", ".join(symbols_with_no_data),
        )

    log.info(
        "Fetched %d total rows across %d symbols",
        len(all_rows),
        len(symbols_with_data),
    )

    rows_written_csv = 0
    rows_written_bq = 0

    if args.target in {"csv", "both"}:
        rows_written_csv = write_csv(all_rows, args.out_csv)
        log.info("Wrote %d rows to CSV: %s", rows_written_csv, args.out_csv)

    if args.target in {"bq", "both"}:
        bq_client = BQClient()
        bq_client.ensure_tables()
        rows_written_bq = bq_client.replace_ohlcv_rows(all_rows)
        log.info("Added %d rows to BigQuery", rows_written_bq)

    log.info(
        "Backfill done | fetched_rows=%d symbols_with_no_data=%d csv_rows=%d bq_rows=%d",
        len(all_rows),
        total_symbols_with_no_data,
        rows_written_csv,
        rows_written_bq,
    )

    return 0

if __name__ == "__main__":
    sys.exit(main())
