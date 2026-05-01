#!/usr/bin/env python3
"""CLI: backfill ticker_daily + ticker_features for a date range."""
from __future__ import annotations

import argparse
import csv
import datetime
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("sigforge.backfill")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigforge.bq_client import BQClient
from sigforge.features import pipeline as feat_pipeline
from sigforge.models import FeatureRow, RawBar
from sigforge.settings import get_sector_map, load_tickers, settings
from sigforge.utils import safe_float, safe_int
from sigforge.yf_client import clear_cache, get_history, get_info

_DAILY_EXCLUDE = {"ingested_at", "data_source"}
_DAILY_FIELDS = [f for f in RawBar.model_fields if f not in _DAILY_EXCLUDE]

_FEATURE_EXCLUDE: set[str] = set()
_FEATURE_FIELDS = list(FeatureRow.model_fields)


def parse_args() -> argparse.Namespace:
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    default_start = yesterday - datetime.timedelta(days=30)

    p = argparse.ArgumentParser(description="Backfill sigforge OHLCV + features.")
    p.add_argument("--start", dest="start_date", type=datetime.date.fromisoformat,
                   default=default_start, metavar="YYYY-MM-DD")
    p.add_argument("--end", dest="end_date", type=datetime.date.fromisoformat,
                   default=yesterday, metavar="YYYY-MM-DD")
    p.add_argument("--env", choices=["prod", "stage"], default=None,
                   help="Override DW_ENV (set DW_ENV env var for persistent override)")
    p.add_argument("--out-csv", metavar="PATH",
                   help="Write CSV to PATH instead of BigQuery")
    p.add_argument("--symbols", nargs="*",
                   help="Explicit symbol list; defaults to config/symbols.yaml")
    return p.parse_args()


def trading_days(start: datetime.date, end: datetime.date):
    cur = start
    while cur <= end:
        if cur.weekday() < 5:   # Mon–Fri
            yield cur
        cur += datetime.timedelta(days=1)


def main() -> int:
    args = parse_args()

    if args.start_date > args.end_date:
        log.error("--start must be <= --end")
        return 1

    symbols = args.symbols or load_tickers()
    if not symbols:
        log.error("No symbols — check config/symbols.yaml")
        return 1

    log.info(
        "Backfill | symbols=%d start=%s end=%s target=%s",
        len(symbols),
        args.start_date,
        args.end_date,
        "csv" if args.out_csv else "bq",
    )

    # Pre-fetch 252-day history for all symbols (shared across all dates)
    log.info("Pre-fetching history for %d symbols …", len(symbols))
    raw_bars = {sym: get_history(sym, lookback_days=settings.history_days, end_date=args.end_date)
                for sym in symbols}
    spy_bars = get_history("SPY", lookback_days=settings.history_days, end_date=args.end_date)
    info_dict = {sym: get_info(sym) for sym in symbols}
    sector_map = get_sector_map()

    all_daily_rows: list[RawBar] = []
    all_feature_rows: list[FeatureRow] = []
    total_errors: list[str] = []

    for trade_date in trading_days(args.start_date, args.end_date):
        # Extract daily rows for this date from pre-fetched history
        day_daily: list[RawBar] = []
        for sym in symbols:
            df = raw_bars.get(sym)
            if df is None or df.empty or trade_date not in df.index:
                continue
            row_s = df.loc[trade_date]
            past = df[df.index <= trade_date].tail(30)
            avg_vol = float(past["Volume"].mean()) if len(past) > 0 else None
            day_daily.append(RawBar(
                symbol=sym,
                trade_date=trade_date,
                open=_sf(row_s.get("Open")),
                high=_sf(row_s.get("High")),
                low=_sf(row_s.get("Low")),
                close=_sf(row_s.get("Close")),
                volume=_si(row_s.get("Volume")),
                avg_volume_30d=avg_vol,
            ))

        day_features, feat_result = feat_pipeline.run(
            symbols=symbols,
            feature_date=trade_date,
            raw_bars=raw_bars,
            spy_bars=spy_bars,
            info_dict=info_dict,
            sector_map=sector_map,
        )

        all_daily_rows.extend(day_daily)
        all_feature_rows.extend(day_features)
        total_errors.extend(feat_result.errors)

        log.info(
            "%s | daily=%d features=%d errors=%d",
            trade_date,
            len(day_daily),
            len(day_features),
            len(feat_result.errors),
        )

    log.info(
        "Backfill totals | daily=%d features=%d errors=%d",
        len(all_daily_rows),
        len(all_feature_rows),
        len(total_errors),
    )

    if args.out_csv:
        _write_csv(all_daily_rows, all_feature_rows, args.out_csv)
    else:
        bq = BQClient()
        bq.ensure_tables()
        if all_daily_rows:
            bq.upsert_daily(all_daily_rows)
        if all_feature_rows:
            bq.upsert_features(all_feature_rows)

    clear_cache()
    return 0


def _write_csv(
    daily_rows: list[RawBar],
    feature_rows: list[FeatureRow],
    out_path: str,
) -> None:
    base, ext = os.path.splitext(out_path)
    ext = ext or ".csv"

    daily_path = f"{base}_daily{ext}"
    feat_path = f"{base}_features{ext}"

    os.makedirs(os.path.dirname(os.path.abspath(daily_path)), exist_ok=True)

    if daily_rows:
        with open(daily_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=_DAILY_FIELDS)
            writer.writeheader()
            writer.writerows(r.to_csv_dict() for r in daily_rows)
        log.info("Wrote %d daily rows → %s", len(daily_rows), daily_path)

    if feature_rows:
        feat_fieldnames = [f for f in FeatureRow.model_fields if f not in {"ingested_at", "run_id"}]
        with open(feat_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=feat_fieldnames)
            writer.writeheader()
            writer.writerows(r.to_csv_dict() for r in feature_rows)
        log.info("Wrote %d feature rows → %s", len(feature_rows), feat_path)


_sf = safe_float
_si = safe_int


if __name__ == "__main__":
    sys.exit(main())
