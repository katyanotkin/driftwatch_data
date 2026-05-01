"""Correlation / peer-group features (cr_ prefix)."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

CORR_WINDOW = 63    # days for rolling peer correlation and lead-lag
HIST_WINDOW = 252   # days for Mahalanobis covariance estimation
MAHAL_OBS = 21      # most-recent days used as observation vector
MIN_PEERS = 2
MIN_OBS = 20


def compute(
    symbol: str,
    bars: pd.DataFrame,
    peer_bars: dict[str, pd.DataFrame],
) -> dict:
    """
    Args:
        symbol: ticker being computed (excluded from peer set).
        bars: OHLCV DataFrame for symbol, indexed by datetime.date.
        peer_bars: {ticker: DataFrame} for all symbols in the same GICS sector.
    Returns:
        Dict of cr_* feature values.
    """
    result: dict = {}

    if bars.empty or "Close" not in bars.columns:
        return result

    stock_ret = bars["Close"].pct_change().dropna()
    if len(stock_ret) < MIN_OBS:
        return result

    # Build peer return matrix aligned with stock
    peer_rets: dict[str, pd.Series] = {}
    for peer_sym, peer_df in peer_bars.items():
        if peer_sym == symbol or peer_df.empty or "Close" not in peer_df.columns:
            continue
        pr = peer_df["Close"].pct_change().dropna()
        if len(pr) >= MIN_OBS:
            peer_rets[peer_sym] = pr

    if len(peer_rets) < MIN_PEERS:
        return result

    # Align all series on common dates
    all_rets = pd.DataFrame({"_stock": stock_ret, **peer_rets}).dropna()
    if len(all_rets) < MIN_OBS:
        return result

    recent = all_rets.tail(CORR_WINDOW)
    stock_recent = recent["_stock"]
    peers_recent = recent.drop(columns=["_stock"])

    # Rolling peer correlation: mean Pearson correlation with sector peers
    corrs = peers_recent.corrwith(stock_recent)
    result["cr_rolling_peer_correlation"] = float(corrs.mean())

    # Peer return deviation: today's stock return minus sector median return
    result["cr_peer_return_deviation"] = float(stock_recent.iloc[-1]) - float(
        peers_recent.iloc[-1].median()
    )

    # Correlation breakdown score (Mahalanobis distance)
    hist = all_rets.tail(HIST_WINDOW)
    peer_hist = hist.drop(columns=["_stock"])
    if len(peer_hist) >= MAHAL_OBS * 2:
        obs_window = peer_hist.tail(MAHAL_OBS).mean() - peer_hist.mean()
        cov = peer_hist.cov()
        try:
            cov_inv = np.linalg.pinv(cov.values)
            v = obs_window.values
            mahal_sq = float(v @ cov_inv @ v)
            result["cr_correlation_breakdown_score"] = float(max(0.0, mahal_sq) ** 0.5)
        except Exception as exc:
            log.debug("%s: Mahalanobis failed: %s", symbol, exc)

    # Lead-lag score: cross-correlation at lag ±1 vs sector centroid
    # Positive value → stock leads; negative → stock lags
    centroid = peers_recent.mean(axis=1)
    try:
        lag_pos = _safe_corr(stock_recent.values, _shift(centroid.values, -1))
        lag_neg = _safe_corr(stock_recent.values, _shift(centroid.values, +1))
        if lag_pos is not None and lag_neg is not None:
            result["cr_lead_lag_score"] = lag_pos - lag_neg
    except Exception as exc:
        log.debug("%s: lead-lag failed: %s", symbol, exc)

    return result


def _shift(arr: np.ndarray, lag: int) -> np.ndarray:
    """Shift array by lag positions, filling edge with NaN."""
    out = arr.astype(float).copy()
    if lag > 0:
        out[:lag] = np.nan
        out[lag:] = arr[:-lag]
    elif lag < 0:
        out[lag:] = np.nan
        out[:lag] = arr[-lag:]
    return out


def _safe_corr(a: np.ndarray, b: np.ndarray) -> float | None:
    mask = ~(np.isnan(a) | np.isnan(b))
    if mask.sum() < 5:
        return None
    try:
        return float(np.corrcoef(a[mask], b[mask])[0, 1])
    except Exception:
        return None
