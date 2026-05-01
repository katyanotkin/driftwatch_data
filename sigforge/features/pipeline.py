"""Feature pipeline: orchestrates all modules → List[FeatureRow]."""
from __future__ import annotations

import datetime
import logging
import math
import uuid

import pandas as pd

from sigforge.features import correlation, fundamental, microstructure, return_based
from sigforge.models import FeatureRow, PipelineResult

log = logging.getLogger(__name__)

_MODULE_PREFIXES = {
    "return_based": "rb_",
    "microstructure": "ms_",
    "correlation": "cr_",
    "fundamental": "fu_",
}

# All feature field names on FeatureRow
_FEATURE_FIELDS = {
    f
    for f in FeatureRow.model_fields
    if any(f.startswith(p) for p in _MODULE_PREFIXES.values())
}


def run(
    symbols: list[str],
    feature_date: datetime.date,
    raw_bars: dict[str, pd.DataFrame],
    spy_bars: pd.DataFrame,
    info_dict: dict[str, dict],
    sector_map: dict[str, str],
) -> tuple[list[FeatureRow], PipelineResult]:
    """
    Compute all features for each symbol on feature_date.

    Args:
        symbols:      Tickers to compute.
        feature_date: The date features are computed for.
        raw_bars:     Pre-fetched 252-day history per symbol (symbol → DataFrame).
        spy_bars:     SPY history (market proxy for return-based features).
        info_dict:    ticker.info per symbol (symbol → dict).
        sector_map:   GICS sector per symbol (symbol → sector string).

    Returns:
        (feature_rows, pipeline_result)
    """
    result = PipelineResult()
    feature_rows: list[FeatureRow] = []
    run_id = _make_run_id(feature_date)

    # Group peers by sector for correlation module
    peers_by_sector: dict[str, dict[str, pd.DataFrame]] = {}
    for sym in symbols:
        sector = sector_map.get(sym, "")
        if sector not in peers_by_sector:
            peers_by_sector[sector] = {}
        if sym in raw_bars and not raw_bars[sym].empty:
            peers_by_sector[sector][sym] = raw_bars[sym]

    for symbol in symbols:
        bars = raw_bars.get(symbol, pd.DataFrame())
        if bars.empty:
            log.warning("%s: no bars — skipping feature computation", symbol)
            result.add_error(f"{symbol}: no bars")
            continue

        features: dict = {}
        module_errors: list[str] = []

        for mod_name, fn, args in [
            ("return_based", return_based.compute, (bars, spy_bars)),
            ("microstructure", microstructure.compute, (bars,)),
        ]:
            out, err = _run_module(mod_name, symbol, fn, *args)
            features.update(out)
            if err:
                module_errors.append(err)

        sector = sector_map.get(symbol, "")
        peer_map = {
            k: v for k, v in peers_by_sector.get(sector, {}).items() if k != symbol
        }
        out, err = _run_module("correlation", symbol, correlation.compute, symbol, bars, peer_map)
        features.update(out)
        if err:
            module_errors.append(err)

        info = info_dict.get(symbol, {})
        out, err = _run_module("fundamental", symbol, fundamental.compute, info)
        features.update(out)
        if err:
            module_errors.append(err)

        for error in module_errors:
            result.add_error(error)

        # Sanitise: replace any inf/nan that slipped through with None
        clean = _sanitize(features)

        row_data: dict = {
            "symbol": symbol,
            "feature_date": feature_date,
            "run_id": run_id,
        }
        row_data.update({k: v for k, v in clean.items() if k in _FEATURE_FIELDS})

        feature_rows.append(FeatureRow(**row_data))
        result.symbols_processed += 1

    return feature_rows, result


def _run_module(
    module_name: str, symbol: str, fn, *args, **kwargs
) -> tuple[dict, str | None]:
    """
    Call a feature module function.
    Returns (result_dict, error_message_or_None).
    """
    try:
        return fn(*args, **kwargs), None
    except Exception as exc:
        msg = f"{symbol}: module {module_name} failed: {exc}"
        log.error(msg)
        return {}, msg


def _sanitize(features: dict) -> dict:
    """Replace float inf/nan values with None."""
    out = {}
    for k, v in features.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            log.warning("Feature %s is %s — replacing with None", k, v)
            out[k] = None
        else:
            out[k] = v
    return out


def _make_run_id(feature_date: datetime.date) -> str:
    return f"features-{feature_date.isoformat()}-{uuid.uuid4().hex[:8]}"
