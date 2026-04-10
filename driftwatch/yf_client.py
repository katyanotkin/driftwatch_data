from __future__ import annotations

import datetime
import logging
import time
from typing import Optional

import yfinance as yf

from driftwatch.models import OHLCVRow, ProfileRow
from driftwatch.settings import get_benchmark_map, settings

log = logging.getLogger(__name__)

_BENCHMARK_OVERRIDES = get_benchmark_map()


def fetch_ohlcv(symbol: str, trade_date: datetime.date) -> Optional[OHLCVRow]:
    """Fetch OHLCV data for a single symbol on trade_date.

    Downloads 35 days of history to compute avg_volume_30d.
    Returns None if the date is not found in the history (e.g. weekend/holiday).
    """
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{settings.ohlcv_lookback_days}d", auto_adjust=True)

        if hist.empty:
            log.warning("%s: empty history returned", symbol)
            return None

        hist.index = hist.index.date  # type: ignore[assignment]
        if trade_date not in hist.index:
            log.warning("%s: trade_date %s not in history", symbol, trade_date)
            return None

        row = hist.loc[trade_date]
        avg_vol = float(hist["Volume"].tail(30).mean()) if len(hist) >= 1 else None

        info = ticker.info
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


def fetch_profile(symbol: str, snapshot_date: datetime.date) -> Optional[ProfileRow]:
    """Fetch profile/info snapshot for a single symbol."""
    try:
        info = yf.Ticker(symbol).info

        # Use benchmark from symbols.yaml override, fall back to yfinance legalType
        benchmark = _BENCHMARK_OVERRIDES.get(symbol) or info.get("legalType")

        return ProfileRow(
            symbol=symbol,
            snapshot_date=snapshot_date,
            net_assets=_safe_float(info.get("totalAssets")),
            fifty_two_week_high=_safe_float(info.get("fiftyTwoWeekHigh")),
            fifty_two_week_low=_safe_float(info.get("fiftyTwoWeekLow")),
            expense_ratio=_safe_float(
                info.get("annualReportExpenseRatio") or info.get("expenseRatio")
            ),
            category=info.get("category"),
            fund_family=info.get("fundFamily"),
            benchmark=benchmark,
        )
    except Exception as exc:
        log.error("%s: profile fetch failed: %s", symbol, exc)
        return None


def fetch_all_ohlcv(
    symbols: list[str],
    trade_date: datetime.date,
    delay_secs: float = 0.3,
) -> list[OHLCVRow]:
    """Fetch OHLCV for all symbols sequentially with a small delay."""
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
    """Fetch profile for all symbols sequentially with a small delay."""
    rows: list[ProfileRow] = []
    for sym in symbols:
        row = fetch_profile(sym, snapshot_date)
        if row:
            rows.append(row)
        time.sleep(delay_secs)
    return rows


def _safe_float(val: object) -> Optional[float]:
    try:
        f = float(val)  # type: ignore[arg-type]
        return None if f != f else f  # filter NaN
    except (TypeError, ValueError):
        return None


def _safe_int(val: object) -> Optional[int]:
    try:
        return int(float(val))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
