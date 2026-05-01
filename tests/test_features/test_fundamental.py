"""Tests for sigforge.features.fundamental."""
from __future__ import annotations

import pytest

from sigforge.features import fundamental


class TestFundamental:
    def test_returns_dict(self):
        assert isinstance(fundamental.compute({}), dict)

    def test_pe_ratio_from_trailing(self):
        result = fundamental.compute({"trailingPE": 25.5})
        assert result["fu_pe_ratio"] == pytest.approx(25.5)

    def test_pe_ratio_fallback_to_forward(self):
        result = fundamental.compute({"forwardPE": 20.0})
        assert result["fu_pe_ratio"] == pytest.approx(20.0)

    def test_pe_ratio_none_when_missing(self):
        result = fundamental.compute({})
        assert result["fu_pe_ratio"] is None

    def test_short_ratio(self):
        result = fundamental.compute({"shortRatio": 3.2})
        assert result["fu_short_interest_ratio"] == pytest.approx(3.2)

    def test_short_ratio_none_when_missing(self):
        result = fundamental.compute({})
        assert result["fu_short_interest_ratio"] is None

    def test_stubs_not_present(self):
        result = fundamental.compute({"trailingPE": 10})
        # Stubs should NOT appear as keys (they are not computed)
        assert "fu_earnings_revision_momentum" not in result
        assert "fu_analyst_estimate_dispersion" not in result

    def test_handles_nan_gracefully(self):
        result = fundamental.compute({"trailingPE": float("nan")})
        assert result["fu_pe_ratio"] is None
