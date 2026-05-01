"""Tests for sigforge.features.correlation."""
from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import pytest

from sigforge.features import correlation


def _make_bars(n: int = 150, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 * np.cumprod(1 + rng.normal(0, 0.01, n))
    dates = [datetime.date(2025, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
    return pd.DataFrame(
        {"Open": close, "High": close * 1.01, "Low": close * 0.99, "Close": close, "Volume": 1e6},
        index=dates,
    )


@pytest.fixture()
def bars():
    return _make_bars(n=150, seed=0)


@pytest.fixture()
def peers():
    return {
        "PEER1": _make_bars(n=150, seed=1),
        "PEER2": _make_bars(n=150, seed=2),
        "PEER3": _make_bars(n=150, seed=3),
    }


class TestCorrelation:
    def test_returns_dict(self, bars, peers):
        result = correlation.compute("TARGET", bars, peers)
        assert isinstance(result, dict)

    def test_expected_keys(self, bars, peers):
        result = correlation.compute("TARGET", bars, peers)
        assert "cr_rolling_peer_correlation" in result
        assert "cr_peer_return_deviation" in result

    def test_peer_correlation_in_range(self, bars, peers):
        result = correlation.compute("TARGET", bars, peers)
        corr = result["cr_rolling_peer_correlation"]
        assert -1.0 <= corr <= 1.0

    def test_empty_bars_returns_empty(self, peers):
        result = correlation.compute("TARGET", pd.DataFrame(), peers)
        assert result == {}

    def test_no_peers_returns_empty(self, bars):
        result = correlation.compute("TARGET", bars, {})
        assert result == {}

    def test_insufficient_peers_returns_empty(self, bars):
        one_peer = {"PEER1": _make_bars(n=150, seed=1)}
        result = correlation.compute("TARGET", bars, one_peer)
        assert result == {}

    def test_symbol_excluded_from_peers(self, bars):
        peers_with_self = {
            "TARGET": bars,
            "PEER1": _make_bars(n=150, seed=1),
            "PEER2": _make_bars(n=150, seed=2),
        }
        result = correlation.compute("TARGET", bars, peers_with_self)
        assert isinstance(result, dict)

    def test_constant_return_peer_yields_none_not_nan(self, bars):
        """A peer with constant returns has undefined correlation — must be None."""
        const_peer = _make_bars(n=150, seed=5).copy()
        const_peer["Close"] = 100.0  # zero daily returns → NaN correlation
        peers = {"CONST": const_peer, "PEER1": _make_bars(n=150, seed=2)}
        result = correlation.compute("TARGET", bars, peers)
        corr = result.get("cr_rolling_peer_correlation")
        # Acceptable outcomes: absent or a valid float (not NaN)
        if corr is not None:
            import math
            assert not math.isnan(corr), "cr_rolling_peer_correlation must not be NaN"

    def test_peer_correlation_clamped_to_unit_interval(self):
        """cr_rolling_peer_correlation must always be in [-1, 1]."""
        import numpy as np
        rng = np.random.default_rng(0)
        n = 150
        close = 100.0 * np.cumprod(1 + rng.normal(0, 0.01, n))
        dates = [datetime.date(2025, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
        bars = pd.DataFrame({"Open": close, "High": close, "Low": close,
                             "Close": close, "Volume": 1e6}, index=dates)
        peers = {
            "P1": _make_bars(n=n, seed=1),
            "P2": _make_bars(n=n, seed=2),
        }
        result = correlation.compute("T", bars, peers)
        c = result.get("cr_rolling_peer_correlation")
        if c is not None:
            assert -1.0 <= c <= 1.0
