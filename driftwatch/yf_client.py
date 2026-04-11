from __future__ import annotations

import datetime
import logging
import time
from typing import Optional

import pandas as pd
import yfinance as yf

from driftwatch.models import OHLCVRow, ProfileRow
from driftwatch.settings import get_benchmark_map, settings

log = logging.getLogger(__name__)

_BENCHMARK_OVERRIDES = get_benchmark_map()


# ---------------------------------------------------------------------
# Core: batched OHLCV fetch (THIS is your main workhorse)
# ---------------------------------------------------------------------

def fetch_ohlcv_history_range_batch(
    symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> list[OHLCVRow]:
    """
    Fetch OHLCV for multiple symbols in one request.

    This is MUCH faster than per-symbol calls.
    """

    if not symbols:
        return []

    log.info("Fetching OHLCV batch: %d symbols", len(symbols))

    df = yf.download(
        tickers=symbols,
        start=start_date.isoformat(),
        end=(end_date + datetime.timedelta(days=1)).isoformat(),
        auto_adjust=False,
        group_by="ticker",
        threads=True,
    )

    if df.empty:
        return []

    rows: list[OHLCVRow] = []

    # Handle single symbol vs multi-symbol shape
    if isinstance(df.columns, pd.MultiIndex):
        # Multi-symbol
        for symbol in symbols:
            if symbol not in df.columns.get_level_values(0):
                continue

            symbol_df = df[symbol].dropna(how="all")
            rows.extend(_rows_from_df(symbol, symbol_df))
    else:
        # Single symbol
        symbol = symbols[0]
        rows.extend(_rows_from_df(symbol, df))

    return rows


def _rows_from_df(symbol: str, df: pd.DataFrame) -> list[OHLCVRow]:
    rows: list[OHLCVRow] = []

    for idx, series in df.iterrows():
        trade_date = pd.Timestamp(idx).date()

        rows.append(
            OHLCVRow(
                symbol=symbol,
                trade_date=trade_date,
                open=_safe_float(series.get("Open")),
                high=_safe_float(series.get("High")),
                low=_safe_float(series.get("Low")),
                close=_safe_float(series.get("Close")),
                volume=_safe_int(series.get("Volume")),
                adjusted_close=_safe_float(series.get("Adj Close")),
            )
        )

    return rows


# ---------------------------------------------------------------------
# Single-symbol fallback (keep, but not primary path)
# ---------------------------------------------------------------------

def fetch_ohlcv_history_range(
    symbol: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> list[OHLCVRow]:
    """Fallback single-symbol fetch (used rarely)."""

    history = yf.Ticker(symbol).history(
        start=start_date.isoformat(),
        end=(end_date + datetime.timedelta(days=1)).isoformat(),
        auto_adjust=False,
    )

    if history.empty:
        return []

    return _rows_from_df(symbol, history)


# ---------------------------------------------------------------------
# Daily fetch (used in run_daily)
# ---------------------------------------------------------------------

def fetch_ohlcv(
    symbol: str,
    trade_date: datetime.date,
) -> Optional[OHLCVRow]:
    """Fetch OHLCV for a single symbol on a specific date."""

    try:
        ticker = yf.Ticker(symbol)

        end = trade_date + datetime.timedelta(days=1)
        start = trade_date - datetime.timedelta(days=settings.ohlcv_lookback_days + 10)

        hist = ticker.history(start=start, end=end, auto_adjust=True)

        if hist.empty:
            return None

        hist.index = hist.index.date

        if trade_date not in hist.index:
            return None

        row = hist.loc[trade_date]

        avg_vol = float(hist["Volume"].tail(30).mean()) if len(hist) else None

        info = ticker.info  # slow but acceptable for daily small N

        pe_ratio = info.get("trailingPE") or info.get("forwardPE")

        return OHLCVRow(
            symbol=symbol,
            trade_date=trade_date,
            open=_safe_float(row.get("Open")),
            high=_safe_float(row.get("High")),
            low=_safe_float(row.get("Low")),
            close=_safe_float(row.get("Close")),
            volume=_safe_int(row.get("Volume")),
            avg_volume_30d=avg_vol,
            pe_ratio=_safe_float(pe_ratio),
        )

    except Exception as exc:
        log.error("%s: OHLCV fetch failed: %s", symbol, exc)
        return None


# ---------------------------------------------------------------------
# Profiles (unchanged but slightly cleaner)
# ---------------------------------------------------------------------

def fetch_profile(symbol: str, snapshot_date: datetime.date) -> Optional[ProfileRow]:
    try:
        info = yf.Ticker(symbol).info

        benchmark = _BENCHMARK_OVERRIDES.get(symbol) or info.get("legalType")

        return ProfileRow(
            symbol=symbol,
            snapshot_date=snapshot_date,
            net_assets=_safe_float(info.get("totalAssets")),
            fifty_two_week_high=_safe_float(info.get("fiftyTwoWeekHigh")),
            fifty_two_week_low=_safe_float(info.get("fiftyTwoWeekLow")),
            expense_ratio=_safe_float(
                info.get("netExpenseRatio")
                or info.get("annualReportExpenseRatio") or info.get("expenseRatio")
            ),
            category=info.get("category"),
            fund_family=info.get("fundFamily"),
            benchmark=benchmark,
        )

    except Exception as exc:
        log.error("%s: profile fetch failed: %s", symbol, exc)
        return None


# ---------------------------------------------------------------------
# Batch helpers (daily jobs)
# ---------------------------------------------------------------------

def fetch_all_ohlcv(
    symbols: list[str],
    trade_date: datetime.date,
    delay_secs: float = 0.3,
) -> list[OHLCVRow]:
    rows: list[OHLCVRow] = []

    for sym in symbols:
        row = fetch_ohlcv(sym, trade_date)
        if row:
            rows.append(row)
        time.sleep(delay_secs)

    return rows


def fetch_all_profiles(
    symbols: list[str],
    snapshot_date: datetime.date,
    delay_secs: float = 0.3,
) -> list[ProfileRow]:
    rows: list[ProfileRow] = []

    for sym in symbols:
        row = fetch_profile(sym, snapshot_date)
        if row:
            rows.append(row)
        time.sleep(delay_secs)

    return rows


# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------

def _safe_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _safe_int(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
