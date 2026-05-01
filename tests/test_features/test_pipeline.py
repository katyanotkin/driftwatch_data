"""Tests for sigforge.features.pipeline."""
from __future__ import annotations

import datetime
import math

import numpy as np
import pandas as pd
import pytest

from sigforge.features import pipeline


def _make_bars(n: int = 120, seed: int = 0) -> pd.DataFrame:
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


_FEATURE_DATE = datetime.date(2025, 5, 1)
_SYMBOLS = ["AAPL", "MSFT"]
_SECTOR_MAP = {"AAPL": "Tech", "MSFT": "Tech"}


@pytest.fixture()
def raw_bars():
    return {s: _make_bars(seed=i) for i, s in enumerate(_SYMBOLS)}


@pytest.fixture()
def spy_bars():
    return _make_bars(seed=99)


class TestPipelineRun:
    def test_returns_feature_rows_and_result(self, raw_bars, spy_bars):
        rows, result = pipeline.run(
            symbols=_SYMBOLS,
            feature_date=_FEATURE_DATE,
            raw_bars=raw_bars,
            spy_bars=spy_bars,
            info_dict={s: {} for s in _SYMBOLS},
            sector_map=_SECTOR_MAP,
        )
        assert len(rows) == 2
        assert result.symbols_processed == 2

    def test_no_nan_or_inf_in_bq_dict(self, raw_bars, spy_bars):
        """NaN guard layer must prevent any float NaN/inf from reaching BQ."""
        rows, _ = pipeline.run(
            symbols=_SYMBOLS,
            feature_date=_FEATURE_DATE,
            raw_bars=raw_bars,
            spy_bars=spy_bars,
            info_dict={s: {} for s in _SYMBOLS},
            sector_map=_SECTOR_MAP,
        )
        for row in rows:
            for k, v in row.to_bq_dict().items():
                if isinstance(v, float):
                    assert not math.isnan(v), f"{k} is NaN"
                    assert not math.isinf(v), f"{k} is inf"

    def test_module_failure_recorded_in_errors(self, raw_bars, spy_bars, monkeypatch):
        """A module that raises must add an entry to result.errors."""
        def _boom(bars, spy):
            raise ValueError("synthetic failure")

        monkeypatch.setattr("sigforge.features.pipeline.return_based.compute", _boom)

        _, result = pipeline.run(
            symbols=["AAPL"],
            feature_date=_FEATURE_DATE,
            raw_bars=raw_bars,
            spy_bars=spy_bars,
            info_dict={"AAPL": {}},
            sector_map=_SECTOR_MAP,
        )
        assert any("return_based" in e for e in result.errors)

    def test_module_failure_still_writes_row(self, raw_bars, spy_bars, monkeypatch):
        """Partial failure must not drop the symbol — row still written with Nones."""
        def _boom(bars, spy):
            raise ValueError("synthetic failure")

        monkeypatch.setattr("sigforge.features.pipeline.return_based.compute", _boom)

        rows, _ = pipeline.run(
            symbols=["AAPL"],
            feature_date=_FEATURE_DATE,
            raw_bars=raw_bars,
            spy_bars=spy_bars,
            info_dict={"AAPL": {}},
            sector_map=_SECTOR_MAP,
        )
        assert len(rows) == 1
        assert rows[0].rb_rolling_beta is None

    def test_run_id_is_unique_per_call(self, raw_bars, spy_bars):
        """run_id must differ between two pipeline runs on the same date."""
        rows1, _ = pipeline.run(
            symbols=["AAPL"],
            feature_date=_FEATURE_DATE,
            raw_bars=raw_bars,
            spy_bars=spy_bars,
            info_dict={"AAPL": {}},
            sector_map=_SECTOR_MAP,
        )
        rows2, _ = pipeline.run(
            symbols=["AAPL"],
            feature_date=_FEATURE_DATE,
            raw_bars=raw_bars,
            spy_bars=spy_bars,
            info_dict={"AAPL": {}},
            sector_map=_SECTOR_MAP,
        )
        assert rows1[0].run_id != rows2[0].run_id

    def test_sanitize_replaces_inf_with_none(self):
        features = {"rb_rolling_beta": float("inf"), "ms_volume_ratio": 1.5}
        clean = pipeline._sanitize(features)
        assert clean["rb_rolling_beta"] is None
        assert clean["ms_volume_ratio"] == pytest.approx(1.5)

    def test_sanitize_replaces_nan_with_none(self):
        features = {"cr_rolling_peer_correlation": float("nan")}
        clean = pipeline._sanitize(features)
        assert clean["cr_rolling_peer_correlation"] is None
