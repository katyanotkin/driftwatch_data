"""Fundamental features (fu_ prefix). Point-in-time from ticker.info."""
from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)


def compute(info: dict) -> dict:
    """
    Args:
        info: dict from yf.Ticker(symbol).info
    Returns:
        Dict of fu_* feature values.
    """
    result: dict = {}

    result["fu_pe_ratio"] = _safe_float(
        info.get("trailingPE") or info.get("forwardPE")
    )
    result["fu_short_interest_ratio"] = _safe_float(info.get("shortRatio"))

    # STUBS — require paid data vendor (Refinitiv/Bloomberg); do not implement, do not remove
    # fu_earnings_revision_momentum: TODO — needs consensus estimate history
    # fu_analyst_estimate_dispersion: TODO — needs analyst estimate distribution

    return result


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)  # type: ignore[arg-type]
        return None if pd.isna(v) else v
    except (TypeError, ValueError):
        return None
