"""Return-based features (rb_ prefix). Window: 63 trading days."""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

WINDOW = 63   # primary rolling window
MIN_OBS = 20  # minimum observations to compute anything


def compute(bars: pd.DataFrame, spy_bars: pd.DataFrame) -> dict:
    """
    Args:
        bars: OHLCV DataFrame indexed by datetime.date, 252-day history.
        spy_bars: Same structure for SPY (market proxy).
    Returns:
        Dict of rb_* feature values. Any feature that cannot be computed is absent.
    """
    result: dict = {}

    if bars.empty or "Close" not in bars.columns:
        return result
    if spy_bars.empty or "Close" not in spy_bars.columns:
        return result

    stock_ret = bars["Close"].pct_change().dropna()
    spy_ret = spy_bars["Close"].pct_change().dropna()

    # Align on common dates, take last WINDOW observations
    aligned = pd.concat([stock_ret, spy_ret], axis=1, join="inner")
    aligned.columns = ["stock", "spy"]
    aligned = aligned.tail(WINDOW).dropna()

    if len(aligned) < MIN_OBS:
        return result

    s = aligned["stock"].values
    m = aligned["spy"].values

    # OLS: stock = alpha + beta * spy
    coef = np.polyfit(m, s, deg=1)   # [beta, alpha]
    beta = float(coef[0])
    alpha = float(coef[1])
    result["rb_rolling_beta"] = beta
    result["rb_rolling_alpha"] = alpha

    # Residual return for the most recent day
    result["rb_residual_return"] = float(s[-1]) - beta * float(m[-1])

    # Lag-1 autocorrelation of the 63-day return series
    if len(s) >= 2:
        autocorr = _autocorr_lag1(s)
        if autocorr is not None:
            result["rb_return_autocorr"] = autocorr

    # Rolling skewness and kurtosis (Fisher definition, excess kurtosis)
    series = pd.Series(s)
    result["rb_rolling_skewness"] = float(series.skew())
    result["rb_rolling_kurtosis"] = float(series.kurtosis())

    # Max drawdown and drawdown duration (WINDOW-day window)
    closes = bars["Close"].tail(WINDOW + 1)
    if len(closes) >= 2:
        cum = (1 + closes.pct_change().dropna()).cumprod()
        roll_max = cum.cummax()
        dd = (cum - roll_max) / roll_max.replace(0, np.nan)
        result["rb_max_drawdown"] = float(dd.min())
        result["rb_drawdown_duration"] = _max_drawdown_duration(cum, roll_max)

    # STUB — Fama-French residual: TODO once FF factor data pipeline is available
    # result["rb_fama_french_residual"] = None

    return result


def _autocorr_lag1(arr: np.ndarray) -> Optional[float]:
    if len(arr) < 2:
        return None
    try:
        return float(pd.Series(arr).autocorr(lag=1))
    except Exception:
        return None


def _max_drawdown_duration(cum: pd.Series, roll_max: pd.Series) -> int:
    """Longest consecutive days spent below the previous peak."""
    below = (cum < roll_max).astype(int).values
    max_dur = cur = 0
    for v in below:
        if v:
            cur += 1
            if cur > max_dur:
                max_dur = cur
        else:
            cur = 0
    return max_dur
