"""Fundamental features (fu_ prefix). Point-in-time from ticker.info."""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from sigforge.utils import safe_float

log = logging.getLogger(__name__)


def compute(info: dict, bars: Optional[pd.DataFrame] = None) -> dict:
    """
    Args:
        info: dict from yf.Ticker(symbol).info
        bars: adjusted OHLCV history (used for float_turnover)
    Returns:
        Dict of fu_* feature values.
    """
    result: dict = {}

    result["fu_pe_ratio"] = safe_float(
        info.get("trailingPE") or info.get("forwardPE")
    )
    result["fu_short_interest_ratio"] = safe_float(info.get("shortRatio"))
    result["fu_short_pct_float"] = safe_float(info.get("shortPercentOfFloat"))

    # Float turnover: daily volume / float shares — high values signal unusual activity
    float_shares = safe_float(info.get("floatShares"))
    if float_shares and float_shares > 0 and bars is not None and not bars.empty:
        last_volume = safe_float(bars["Volume"].iloc[-1])
        if last_volume is not None:
            result["fu_float_turnover"] = last_volume / float_shares
        else:
            result["fu_float_turnover"] = None
    else:
        result["fu_float_turnover"] = None

    result["fu_gross_margin"] = safe_float(info.get("grossMargins"))
    result["fu_operating_margin"] = safe_float(info.get("operatingMargins"))
    result["fu_profit_margin"] = safe_float(info.get("profitMargins"))
    result["fu_return_on_equity"] = safe_float(info.get("returnOnEquity"))
    result["fu_return_on_assets"] = safe_float(info.get("returnOnAssets"))
    result["fu_debt_to_equity"] = safe_float(info.get("debtToEquity"))
    result["fu_revenue_growth"] = safe_float(info.get("revenueGrowth"))
    result["fu_earnings_growth"] = safe_float(info.get("earningsGrowth"))

    # STUBS — require paid data vendor (Refinitiv/Bloomberg); do not implement, do not remove
    # fu_earnings_revision_momentum: TODO — needs consensus estimate history
    # fu_analyst_estimate_dispersion: TODO — needs analyst estimate distribution

    return result
