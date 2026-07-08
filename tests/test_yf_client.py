"""Tests for teamfish.yf_client — all yfinance calls are mocked."""
from __future__ import annotations

import datetime

import numpy as np
import pandas as pd

from teamfish.yf_client import _extract_symbol


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
        from teamfish.models import RawBar
        exclude = {"ingested_at", "data_source"}
        expected = [f for f in RawBar.model_fields if f not in exclude]
        bar = RawBar(symbol="AAPL", trade_date=datetime.date(2025, 1, 1))
        assert list(bar.to_csv_dict().keys()) == expected

    def test_feature_row_csv_columns_match_model(self):
        """CSV fieldnames must match FeatureRow model fields minus excluded metadata."""
        from teamfish.models import FeatureRow
        exclude = {"ingested_at", "run_id"}
        expected = [f for f in FeatureRow.model_fields if f not in exclude]
        row = FeatureRow(
            symbol="AAPL", feature_date=datetime.date(2025, 1, 1), run_id="r"
        )
        assert list(row.to_csv_dict().keys()) == expected


class TestHistoryCacheKey:
    def test_cache_distinguishes_fetch_arguments(self, monkeypatch):
        """Cache is keyed by (symbol, lookback_days, end_date) — a second call
        with a different end_date must trigger a fresh fetch, not return the
        previously cached window (regression for symbol-only cache key)."""
        from teamfish import yf_client

        calls: list[tuple[str, str]] = []

        class FakeTicker:
            def __init__(self, symbol: str):
                self.symbol = symbol

            def history(self, start, end, auto_adjust, actions):
                calls.append((self.symbol, end))
                n = 30
                dates = pd.date_range(end=pd.Timestamp(end) - pd.Timedelta(days=1), periods=n)
                close = np.linspace(100.0, 110.0, n)
                return pd.DataFrame(
                    {"Open": close, "High": close, "Low": close, "Close": close,
                     "Volume": 1e6, "Dividends": 0.0, "Stock Splits": 0.0},
                    index=dates,
                )

        monkeypatch.setattr(yf_client.yf, "Ticker", FakeTicker)
        yf_client.clear_cache()

        d1 = yf_client.get_history("AAPL", lookback_days=10, end_date=datetime.date(2025, 3, 1))
        d2 = yf_client.get_history("AAPL", lookback_days=10, end_date=datetime.date(2025, 6, 1))
        assert len(calls) == 2
        assert d1.index[-1] != d2.index[-1]

        # Identical arguments are served from cache — no third fetch
        yf_client.get_history("AAPL", lookback_days=10, end_date=datetime.date(2025, 3, 1))
        assert len(calls) == 2

        yf_client.clear_cache()
