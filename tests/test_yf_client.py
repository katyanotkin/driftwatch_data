"""Tests for sigforge.yf_client — all yfinance calls are mocked."""
from __future__ import annotations

import datetime

import numpy as np
import pandas as pd

from sigforge.yf_client import _extract_symbol


def _flat_df(n: int = 10) -> pd.DataFrame:
    """DataFrame with plain (non-MultiIndex) columns, as yfinance returns for 1 symbol."""
    close = np.linspace(100, 110, n)
    dates = [datetime.date(2025, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
    return pd.DataFrame(
        {"Open": close, "High": close * 1.01, "Low": close * 0.99,
         "Close": close, "Volume": 1e6},
        index=dates,
    )


def _multi_df(symbols: list[str], n: int = 10) -> pd.DataFrame:
    """DataFrame with MultiIndex columns as yfinance returns for multiple symbols."""
    frames = {}
    for sym in symbols:
        close = np.linspace(100, 110, n)
        frames[sym] = pd.DataFrame(
            {"Open": close, "High": close, "Low": close, "Close": close, "Volume": 1e6}
        )
    return pd.concat(frames, axis=1)


class TestExtractSymbol:
    def test_single_symbol_returns_full_df(self):
        df = _flat_df()
        out = _extract_symbol(df, "AAPL", n_symbols=1)
        assert out is not None
        assert "Close" in out.columns

    def test_multi_symbol_multiindex_found(self):
        df = _multi_df(["AAPL", "MSFT"])
        out = _extract_symbol(df, "AAPL", n_symbols=2)
        assert out is not None
        assert "Close" in out.columns

    def test_multi_symbol_multiindex_missing(self):
        df = _multi_df(["AAPL", "MSFT"])
        out = _extract_symbol(df, "NVDA", n_symbols=2)
        assert out is None

    def test_flat_columns_with_multi_symbol_request_falls_back(self):
        """When yfinance returns flat columns for a multi-symbol request,
        _extract_symbol should return the data (with a warning) rather than None."""
        df = _flat_df()
        out = _extract_symbol(df, "AAPL", n_symbols=3)
        assert out is not None
        assert "Close" in out.columns

    def test_no_close_column_returns_none(self):
        df = pd.DataFrame({"Foo": [1, 2, 3]})
        out = _extract_symbol(df, "AAPL", n_symbols=2)
        assert out is None


class TestCsvColumns:
    def test_raw_bar_csv_columns_match_model(self):
        """CSV fieldnames must match RawBar model fields minus excluded metadata."""
        from sigforge.models import RawBar
        exclude = {"ingested_at", "data_source"}
        expected = [f for f in RawBar.model_fields if f not in exclude]
        bar = RawBar(symbol="AAPL", trade_date=datetime.date(2025, 1, 1))
        assert list(bar.to_csv_dict().keys()) == expected

    def test_feature_row_csv_columns_match_model(self):
        """CSV fieldnames must match FeatureRow model fields minus excluded metadata."""
        from sigforge.models import FeatureRow
        exclude = {"ingested_at", "run_id"}
        expected = [f for f in FeatureRow.model_fields if f not in exclude]
        row = FeatureRow(
            symbol="AAPL", feature_date=datetime.date(2025, 1, 1), run_id="r"
        )
        assert list(row.to_csv_dict().keys()) == expected
