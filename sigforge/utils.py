"""Shared numeric conversion utilities."""
from __future__ import annotations

import math
from typing import Optional


def safe_float(value: object) -> Optional[float]:
    """Return float, or None for null / NaN / inf."""
    if value is None:
        return None
    try:
        v = float(value)  # type: ignore[arg-type]
        return None if (math.isnan(v) or math.isinf(v)) else v
    except (TypeError, ValueError):
        return None


def safe_int(value: object) -> Optional[int]:
    """Return int, or None for null / NaN / inf."""
    f = safe_float(value)
    return None if f is None else int(f)


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))
