"""Microstructure features (ms_ prefix)."""
from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

AMIHUD_WINDOW = 21
REALIZED_VOL_WINDOW = 21
VOLUME_AVG_WINDOW = 30
ANNUALISE = math.sqrt(252)
MIN_OBS = 5


def compute(bars: pd.DataFrame) -> dict:
    """
    Args:
        bars: OHLCV DataFrame indexed by datetime.date, >=252-day history.
    Returns:
        Dict of ms_* feature values.
    """
    result: dict = {}

    required = {"Open", "High", "Low", "Close", "Volume"}
    if bars.empty or not required.issubset(bars.columns):
        return result
    if len(bars) < MIN_OBS:
        return result

    closes = bars["Close"]
    log_ret = np.log(closes / closes.shift(1)).dropna()

    # Amihud illiquidity: mean(|ret| / dollar_volume) over 21 days
    win = bars.tail(AMIHUD_WINDOW)
    if len(win) >= MIN_OBS:
        ret_abs = (win["Close"] / win["Close"].shift(1) - 1).abs().dropna()
        dollar_vol = (win["Close"] * win["Volume"]).reindex(ret_abs.index)
        valid = dollar_vol.replace(0, np.nan).dropna()
        if not valid.empty:
            amihud_vals = ret_abs.reindex(valid.index) / valid
            result["ms_amihud_illiquidity"] = float(amihud_vals.mean())

    # Volume ratio: today's volume / avg_volume_30d
    past_30 = bars.tail(VOLUME_AVG_WINDOW)
    avg_vol = past_30["Volume"].mean()
    today_vol = bars["Volume"].iloc[-1]
    if avg_vol and avg_vol > 0:
        result["ms_volume_ratio"] = float(today_vol) / float(avg_vol)

    # Realized volatility: 21-day std of log returns, annualised
    rv_window = log_ret.tail(REALIZED_VOL_WINDOW)
    if len(rv_window) >= MIN_OBS:
        result["ms_realized_volatility"] = float(rv_window.std()) * ANNUALISE

    # High-low range for today: (High - Low) / Close
    today = bars.iloc[-1]
    high, low, close = today.get("High"), today.get("Low"), today.get("Close")
    if high is not None and low is not None and close and close != 0:
        result["ms_high_low_range"] = (float(high) - float(low)) / float(close)

    # STUBS — require data beyond yfinance; do not implement, do not remove
    # ms_bid_ask_spread: TODO — needs Level 1 quote data
    # ms_implied_vol_spread: TODO — needs options chain data
    # ms_intraday_vol_pattern: TODO — needs intraday OHLCV

    return result
