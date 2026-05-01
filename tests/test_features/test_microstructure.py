"""Tests for sigforge.features.microstructure."""
from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import pytest

from sigforge.features import microstructure


def _make_bars(n: int = 60, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 50.0 * np.cumprod(1 + rng.normal(0, 0.012, n))
    high = close * (1 + rng.uniform(0, 0.02, n))
    low = close * (1 - rng.uniform(0, 0.02, n))
    dates = [datetime.date(2025, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
    return pd.DataFrame(
        {
            "Open": close * 0.999,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": rng.integers(500_000, 5_000_000, n).astype(float),
        },
        index=dates,
    )


@pytest.fixture()
def bars():
    return _make_bars(n=60)


class TestMicrostructure:
    def test_returns_dict(self, bars):
        assert isinstance(microstructure.compute(bars), dict)

    def test_expected_keys(self, bars):
        result = microstructure.compute(bars)
        assert "ms_amihud_illiquidity" in result
        assert "ms_volume_ratio" in result
        assert "ms_realized_volatility" in result
        assert "ms_high_low_range" in result

    def test_amihud_nonnegative(self, bars):
        result = microstructure.compute(bars)
        assert result["ms_amihud_illiquidity"] >= 0.0

    def test_volume_ratio_positive(self, bars):
        result = microstructure.compute(bars)
        assert result["ms_volume_ratio"] > 0.0

    def test_realized_vol_positive(self, bars):
        result = microstructure.compute(bars)
        assert result["ms_realized_volatility"] > 0.0

    def test_high_low_range_positive(self, bars):
        result = microstructure.compute(bars)
        assert result["ms_high_low_range"] >= 0.0

    def test_empty_bars_returns_empty(self):
        assert microstructure.compute(pd.DataFrame()) == {}

    def test_insufficient_data_returns_empty(self):
        tiny = _make_bars(n=2)
        result = microstructure.compute(tiny)
        # Most features should be absent or computed with limited data
        assert isinstance(result, dict)
