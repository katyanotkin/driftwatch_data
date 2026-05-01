"""Tests for sigforge.features.fundamental."""
from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import pytest

from sigforge.features import fundamental


def _make_bars(n: int = 30, volume: float = 1_000_000.0) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    close = 100.0 * np.cumprod(1 + rng.normal(0, 0.01, n))
    dates = [datetime.date(2025, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
    return pd.DataFrame(
        {"Open": close, "High": close, "Low": close, "Close": close,
         "Volume": np.full(n, volume)},
        index=dates,
    )


_FULL_INFO = {
    "trailingPE": 25.0,
    "forwardPE": 22.0,
    "shortRatio": 3.2,
    "shortPercentOfFloat": 0.05,
    "floatShares": 10_000_000.0,
    "grossMargins": 0.42,
    "operatingMargins": 0.28,
    "profitMargins": 0.21,
    "returnOnEquity": 0.38,
    "returnOnAssets": 0.15,
    "debtToEquity": 45.0,
    "revenueGrowth": 0.12,
    "earningsGrowth": 0.18,
}


class TestFundamental:
    def test_returns_dict(self):
        assert isinstance(fundamental.compute({}), dict)

    def test_no_bars_arg_accepted(self):
        result = fundamental.compute(_FULL_INFO)
        assert isinstance(result, dict)

    # --- pe ratio ---

    def test_pe_ratio_from_trailing(self):
        result = fundamental.compute({"trailingPE": 25.5})
        assert result["fu_pe_ratio"] == pytest.approx(25.5)

    def test_pe_ratio_fallback_to_forward(self):
        result = fundamental.compute({"forwardPE": 20.0})
        assert result["fu_pe_ratio"] == pytest.approx(20.0)

    def test_pe_ratio_none_when_missing(self):
        result = fundamental.compute({})
        assert result["fu_pe_ratio"] is None

    def test_handles_nan_gracefully(self):
        result = fundamental.compute({"trailingPE": float("nan")})
        assert result["fu_pe_ratio"] is None

    # --- short interest ---

    def test_short_ratio(self):
        result = fundamental.compute({"shortRatio": 3.2})
        assert result["fu_short_interest_ratio"] == pytest.approx(3.2)

    def test_short_pct_float(self):
        result = fundamental.compute({"shortPercentOfFloat": 0.05})
        assert result["fu_short_pct_float"] == pytest.approx(0.05)

    def test_short_fields_none_when_missing(self):
        result = fundamental.compute({})
        assert result["fu_short_interest_ratio"] is None
        assert result["fu_short_pct_float"] is None

    # --- float turnover ---

    def test_float_turnover_computed(self):
        bars = _make_bars(volume=2_000_000.0)
        result = fundamental.compute({"floatShares": 10_000_000.0}, bars)
        assert result["fu_float_turnover"] == pytest.approx(0.2)

    def test_float_turnover_none_without_bars(self):
        result = fundamental.compute({"floatShares": 10_000_000.0})
        assert result["fu_float_turnover"] is None

    def test_float_turnover_none_without_float_shares(self):
        bars = _make_bars(volume=1_000_000.0)
        result = fundamental.compute({}, bars)
        assert result["fu_float_turnover"] is None

    def test_float_turnover_none_when_zero_float(self):
        bars = _make_bars(volume=1_000_000.0)
        result = fundamental.compute({"floatShares": 0.0}, bars)
        assert result["fu_float_turnover"] is None

    # --- margin / profitability ---

    def test_margin_fields(self):
        result = fundamental.compute({
            "grossMargins": 0.42,
            "operatingMargins": 0.28,
            "profitMargins": 0.21,
        })
        assert result["fu_gross_margin"] == pytest.approx(0.42)
        assert result["fu_operating_margin"] == pytest.approx(0.28)
        assert result["fu_profit_margin"] == pytest.approx(0.21)

    def test_margin_fields_none_when_missing(self):
        result = fundamental.compute({})
        assert result["fu_gross_margin"] is None
        assert result["fu_operating_margin"] is None
        assert result["fu_profit_margin"] is None

    # --- capital efficiency / leverage ---

    def test_roe_roa(self):
        result = fundamental.compute({"returnOnEquity": 0.38, "returnOnAssets": 0.15})
        assert result["fu_return_on_equity"] == pytest.approx(0.38)
        assert result["fu_return_on_assets"] == pytest.approx(0.15)

    def test_debt_to_equity(self):
        result = fundamental.compute({"debtToEquity": 45.0})
        assert result["fu_debt_to_equity"] == pytest.approx(45.0)

    # --- growth ---

    def test_growth_fields(self):
        result = fundamental.compute({"revenueGrowth": 0.12, "earningsGrowth": 0.18})
        assert result["fu_revenue_growth"] == pytest.approx(0.12)
        assert result["fu_earnings_growth"] == pytest.approx(0.18)

    # --- stubs ---

    def test_stubs_not_present(self):
        result = fundamental.compute({"trailingPE": 10})
        assert "fu_earnings_revision_momentum" not in result
        assert "fu_analyst_estimate_dispersion" not in result

    # --- full info round-trip ---

    def test_full_info_no_nones_dropped(self):
        bars = _make_bars(volume=500_000.0)
        result = fundamental.compute(_FULL_INFO, bars)
        expected_keys = {
            "fu_pe_ratio", "fu_short_interest_ratio", "fu_short_pct_float",
            "fu_float_turnover", "fu_gross_margin", "fu_operating_margin",
            "fu_profit_margin", "fu_return_on_equity", "fu_return_on_assets",
            "fu_debt_to_equity", "fu_revenue_growth", "fu_earnings_growth",
        }
        assert expected_keys.issubset(result.keys())
        for k in expected_keys:
            assert result[k] is not None, f"{k} should not be None with full info"
