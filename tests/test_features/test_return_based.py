"""Tests for sigforge.features.return_based."""
from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import pytest

from sigforge.features import return_based


def _make_bars(n: int = 100, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 * np.cumprod(1 + rng.normal(0, 0.01, n))
    dates = [datetime.date(2025, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
    return pd.DataFrame(
        {
            "Open": close * 0.99,
            "High": close * 1.01,
            "Low": close * 0.98,
            "Close": close,
            "Volume": rng.integers(1_000_000, 10_000_000, n).astype(float),
        },
        index=dates,
    )


@pytest.fixture()
def bars():
    return _make_bars(n=120)


@pytest.fixture()
def spy_bars():
    return _make_bars(n=120, seed=99)


class TestReturnBased:
    def test_returns_dict(self, bars, spy_bars):
        result = return_based.compute(bars, spy_bars)
        assert isinstance(result, dict)

    def test_expected_keys_present(self, bars, spy_bars):
        result = return_based.compute(bars, spy_bars)
        expected = {
            "rb_rolling_alpha",
            "rb_rolling_beta",
            "rb_residual_return",
            "rb_return_autocorr",
            "rb_rolling_skewness",
            "rb_rolling_kurtosis",
            "rb_max_drawdown",
            "rb_drawdown_duration",
        }
        assert expected.issubset(result.keys())

    def test_beta_is_float(self, bars, spy_bars):
        result = return_based.compute(bars, spy_bars)
        assert isinstance(result["rb_rolling_beta"], float)

    def test_max_drawdown_nonpositive(self, bars, spy_bars):
        result = return_based.compute(bars, spy_bars)
        assert result["rb_max_drawdown"] <= 0.0

    def test_drawdown_duration_nonnegative_int(self, bars, spy_bars):
        result = return_based.compute(bars, spy_bars)
        dur = result["rb_drawdown_duration"]
        assert isinstance(dur, int)
        assert dur >= 0

    def test_empty_bars_returns_empty(self, spy_bars):
        result = return_based.compute(pd.DataFrame(), spy_bars)
        assert result == {}

    def test_insufficient_data_returns_empty(self, spy_bars):
        tiny = _make_bars(n=5)
        result = return_based.compute(tiny, spy_bars)
        assert result == {}

    def test_empty_spy_returns_empty(self, bars):
        result = return_based.compute(bars, pd.DataFrame())
        assert result == {}
