"""Shared numeric conversion and date utilities."""
from __future__ import annotations

import datetime
import math
import zoneinfo
from typing import Optional

MARKET_TZ = zoneinfo.ZoneInfo("America/New_York")


def market_today() -> datetime.date:
    """
    Current date in US market time (America/New_York, DST-aware).

    Jobs scheduled after the 16:00 ET close (e.g. 22:00 ET) run when the UTC
    clock has already rolled to the next day — date.today() in a UTC container
    would stamp tomorrow's date and find no bar for it. Always derive the
    trading date from market time.
    """
    return datetime.datetime.now(MARKET_TZ).date()


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
