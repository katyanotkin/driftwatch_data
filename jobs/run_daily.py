#!/usr/bin/env python3
"""Cloud Run Job: OHLCV + features for today."""
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
log = logging.getLogger("sigforge.daily")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigforge.bq_client import BQClient
from sigforge.features import pipeline as feat_pipeline
from sigforge.settings import get_sector_map, load_tickers, settings
from sigforge.yf_client import clear_cache, fetch_daily_batch, get_history, get_info


def main() -> int:
    trade_date = datetime.date.today()
    run_id = str(uuid.uuid4())

    log.info("=== sigforge daily run | date=%s run_id=%s ===", trade_date, run_id)

    symbols = load_tickers()
    if not symbols:
        log.error("No symbols configured — check config/symbols.yaml")
        return 1

    bq = BQClient()
    bq.ensure_tables()

    # --- OHLCV ---
    log.info("Fetching OHLCV for %d symbols", len(symbols))
    daily_rows = fetch_daily_batch(symbols, trade_date)
    if not daily_rows:
        log.error("No OHLCV rows fetched")
        return 1

    bq.upsert_daily(daily_rows)
    log.info("OHLCV: %d rows written", len(daily_rows))

    # --- Features ---
    log.info("Computing features for %d symbols", len(symbols))
    lookback = settings.history_days

    raw_bars = {
        sym: get_history(sym, lookback_days=lookback, end_date=trade_date) for sym in symbols
    }
    spy_bars = get_history("SPY", lookback_days=lookback, end_date=trade_date)
    info_dict = {sym: get_info(sym) for sym in symbols}
    sector_map = get_sector_map()

    feature_rows, feat_result = feat_pipeline.run(
        symbols=symbols,
        feature_date=trade_date,
        raw_bars=raw_bars,
        spy_bars=spy_bars,
        info_dict=info_dict,
        sector_map=sector_map,
    )

    if feature_rows:
        bq.upsert_features(feature_rows)
        log.info(
            "Features: %d rows written, %d errors",
            len(feature_rows),
            len(feat_result.errors),
        )
    else:
        log.warning("No feature rows produced")

    clear_cache()
    log.info("=== sigforge daily run complete ===")
    return 1 if feat_result.has_critical_errors else 0


if __name__ == "__main__":
    sys.exit(main())
