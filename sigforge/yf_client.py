from __future__ import annotations

import datetime
import logging
from typing import Optional

import pandas as pd
import yfinance as yf

from sigforge.models import ProfileRow, RawBar
from sigforge.utils import safe_float, safe_int

log = logging.getLogger(__name__)

# In-memory cache per run — never re-fetch the same ticker twice
_history_cache: dict[str, pd.DataFrame] = {}
_info_cache: dict[str, dict] = {}


def get_history(
    symbol: str,
    lookback_days: int = 252,
    end_date: Optional[datetime.date] = None,
) -> pd.DataFrame:
    """
    Return up to lookback_days of daily adjusted OHLCV history for symbol, cached.
    DataFrame is indexed by datetime.date.
    Columns: Open High Low Close Volume Dividends Stock Splits (auto_adjust=True).
    Close is the split-and-dividend-adjusted price.
    """
    if symbol in _history_cache:
        return _history_cache[symbol]

    end = end_date or datetime.date.today()
    # Fetch extra calendar days to account for weekends/holidays
    start = end - datetime.timedelta(days=int(lookback_days * 1.6))

    try:
        df = yf.Ticker(symbol).history(
            start=start.isoformat(),
            end=(end + datetime.timedelta(days=1)).isoformat(),
            auto_adjust=True,
            actions=True,
        )
        if df.empty:
            log.warning("%s: empty history", symbol)
            _history_cache[symbol] = pd.DataFrame()
            return pd.DataFrame()

        df.index = pd.to_datetime(df.index).date
        df = df.sort_index().tail(lookback_days)
        _history_cache[symbol] = df
        return df

    except Exception as exc:
        log.error("%s: history fetch failed: %s", symbol, exc)
        _history_cache[symbol] = pd.DataFrame()
        return pd.DataFrame()


def get_info(symbol: str) -> dict:
    """Return ticker.info for symbol, cached. Empty dict on failure."""
    if symbol in _info_cache:
        return _info_cache[symbol]
    try:
        info = yf.Ticker(symbol).info or {}
        _info_cache[symbol] = info
        return info
    except Exception as exc:
        log.error("%s: info fetch failed: %s", symbol, exc)
        _info_cache[symbol] = {}
        return {}


def clear_cache() -> None:
    """Reset in-memory cache (call between independent runs)."""
    _history_cache.clear()
    _info_cache.clear()


def fetch_daily_batch(
    symbols: list[str],
    trade_date: datetime.date,
) -> list[RawBar]:
    """
    Batch-download OHLCV for all symbols on trade_date.
    Fetches 50 calendar days to compute avg_volume_30d.
    """
    if not symbols:
        return []

    start = trade_date - datetime.timedelta(days=50)
    end = trade_date + datetime.timedelta(days=1)

    log.info("Batch OHLCV fetch: %d symbols on %s", len(symbols), trade_date)
    try:
        raw = yf.download(
            tickers=symbols,
            start=start.isoformat(),
            end=end.isoformat(),
            auto_adjust=True,
            actions=True,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception as exc:
        log.error("Batch download failed: %s", exc)
        return []

    if raw.empty:
        return []

    rows: list[RawBar] = []
    for symbol in symbols:
        try:
            sym_df = _extract_symbol(raw, symbol, len(symbols))
            if sym_df is None or sym_df.empty:
                log.warning("%s: no data in batch response", symbol)
                continue

            sym_df.index = pd.to_datetime(sym_df.index).date
            sym_df = sym_df.sort_index()

            if trade_date not in sym_df.index:
                log.warning("%s: %s not in fetched range", symbol, trade_date)
                continue

            today = sym_df.loc[trade_date]
            past = sym_df[sym_df.index <= trade_date].tail(30)
            avg_vol = float(past["Volume"].mean()) if len(past) > 0 else None

            # Dividends/splits are sparse — store None rather than 0
            div = _safe_float(today.get("Dividends"))
            split = _safe_float(today.get("Stock Splits"))
            rows.append(
                RawBar(
                    symbol=symbol,
                    trade_date=trade_date,
                    open=_safe_float(today.get("Open")),
                    high=_safe_float(today.get("High")),
                    low=_safe_float(today.get("Low")),
                    adj_close=_safe_float(today.get("Close")),
                    volume=_safe_int(today.get("Volume")),
                    avg_volume_30d=avg_vol,
                    dividends=div if div else None,
                    split_ratio=split if split else None,
                )
            )
        except Exception as exc:
            log.error("%s: row extraction failed: %s", symbol, exc)

    return rows


def fetch_profile(symbol: str, snapshot_date: datetime.date) -> Optional[ProfileRow]:
    """Fetch ticker.info and return a ProfileRow."""
    info = get_info(symbol)
    if not info:
        return None
    try:
        return ProfileRow(
            symbol=symbol,
            snapshot_date=snapshot_date,
            name=info.get("longName") or info.get("shortName"),
            exchange=info.get("exchange"),
            gics_sector=info.get("sector"),
            gics_industry_group=None,   # not in yfinance
            gics_industry=info.get("industry"),
            gics_sub_industry=None,     # not in yfinance
            market_cap=_safe_float(info.get("marketCap")),
            enterprise_value=_safe_float(info.get("enterpriseValue")),
            pe_ratio=_safe_float(info.get("trailingPE")),
            forward_pe=_safe_float(info.get("forwardPE")),
            pb_ratio=_safe_float(info.get("priceToBook")),
            ps_ratio=_safe_float(info.get("priceToSalesTrailing12Months")),
            dividend_yield=_safe_float(info.get("dividendYield")),
            payout_ratio=_safe_float(info.get("payoutRatio")),
            fifty_two_week_high=_safe_float(info.get("fiftyTwoWeekHigh")),
            fifty_two_week_low=_safe_float(info.get("fiftyTwoWeekLow")),
            beta=_safe_float(info.get("beta")),
            shares_outstanding=_safe_float(info.get("sharesOutstanding")),
            float_shares=_safe_float(info.get("floatShares")),
            shares_short=_safe_float(info.get("sharesShort")),
            short_ratio=_safe_float(info.get("shortRatio")),
            short_pct_float=_safe_float(info.get("shortPercentOfFloat")),
            institutional_hold_pct=_safe_float(info.get("heldPercentInstitutions")),
            insider_hold_pct=_safe_float(info.get("heldPercentInsiders")),
            gross_margin=_safe_float(info.get("grossMargins")),
            operating_margin=_safe_float(info.get("operatingMargins")),
            profit_margin=_safe_float(info.get("profitMargins")),
            return_on_equity=_safe_float(info.get("returnOnEquity")),
            return_on_assets=_safe_float(info.get("returnOnAssets")),
            debt_to_equity=_safe_float(info.get("debtToEquity")),
            revenue_growth=_safe_float(info.get("revenueGrowth")),
            earnings_growth=_safe_float(info.get("earningsGrowth")),
            free_cash_flow=_safe_float(info.get("freeCashflow")),
            analyst_target_price=_safe_float(info.get("targetMeanPrice")),
            analyst_recommendation=info.get("recommendationKey"),
        )
    except Exception as exc:
        log.error("%s: ProfileRow construction failed: %s", symbol, exc)
        return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_symbol(
    df: pd.DataFrame,
    symbol: str,
    n_symbols: int,
) -> Optional[pd.DataFrame]:
    if n_symbols == 1:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        if symbol in df.columns.get_level_values(0):
            return df[symbol].dropna(how="all")
        return None
    # yfinance returned flat columns despite multiple symbols being requested
    # (happens when only one ticker had data). Check if Close column exists as
    # a best-effort fallback; caller must decide if this is the right symbol.
    if "Close" in df.columns:
        log.warning(
            "%s: batch response has flat columns — yfinance may have returned "
            "data for a different symbol; treating as this symbol's data",
            symbol,
        )
        return df.dropna(how="all")
    return None


_safe_float = safe_float
_safe_int = safe_int
