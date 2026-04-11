import datetime
from unittest.mock import patch
import pandas as pd
from driftwatch.yf_client import fetch_ohlcv


TRADE_DATE = datetime.date(2026, 3, 10)


def _make_hist(include_date: bool) -> pd.DataFrame:
    dates = [TRADE_DATE] if include_date else [datetime.date(2026, 3, 9)]
    # Use Timestamp so hist.index.date works correctly
    idx = pd.to_datetime(dates)
    return pd.DataFrame(
        {"Open": [100.0], "High": [110.0], "Low": [90.0],
         "Close": [105.0], "Volume": [1_000_000]},
        index=idx,
    )

@patch("driftwatch.yf_client.yf.Ticker")
def test_fetch_ohlcv_returns_row(mock_ticker):
    hist = _make_hist(include_date=True)
    mock_ticker.return_value.history.return_value = hist
    mock_ticker.return_value.info = {"trailingPE": 22.5}

    row = fetch_ohlcv("SPY", TRADE_DATE)

    assert row is not None
    assert row.symbol == "SPY"
    assert row.trade_date == TRADE_DATE
    assert row.close == 105.0
    assert row.pe_ratio == 22.5


@patch("driftwatch.yf_client.yf.Ticker")
def test_fetch_ohlcv_missing_date_returns_none(mock_ticker):
    hist = _make_hist(include_date=False)
    mock_ticker.return_value.history.return_value = hist
    mock_ticker.return_value.info = {}

    row = fetch_ohlcv("SPY", TRADE_DATE)
    assert row is None


@patch("driftwatch.yf_client.yf.Ticker")
def test_fetch_ohlcv_empty_history_returns_none(mock_ticker):
    import pandas as pd
    mock_ticker.return_value.history.return_value = pd.DataFrame()

    row = fetch_ohlcv("SPY", TRADE_DATE)
    assert row is None


@patch("driftwatch.yf_client.yf.Ticker")
def test_fetch_ohlcv_exception_returns_none(mock_ticker):
    mock_ticker.return_value.history.side_effect = RuntimeError("network error")

    row = fetch_ohlcv("SPY", TRADE_DATE)
    assert row is None
