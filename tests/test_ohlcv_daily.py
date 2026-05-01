import datetime
from unittest.mock import patch

from driftwatch.models import OHLCVRow
from driftwatch.pipelines import ohlcv_daily

TRADE_DATE = datetime.date(2026, 3, 10)
RUN_ID = "test-run-id"


def _make_row(symbol: str) -> OHLCVRow:
    return OHLCVRow(
        symbol=symbol,
        trade_date=TRADE_DATE,
        open=100.0, high=110.0, low=90.0, close=105.0,
        volume=1_000_000,
    )


@patch("driftwatch.pipelines.ohlcv_daily.bq_client")
@patch("driftwatch.pipelines.ohlcv_daily.fetch_all_ohlcv")
@patch("driftwatch.pipelines.ohlcv_daily.load_tickers")
def test_run_success(mock_tickers, mock_fetch, mock_bq):
    mock_tickers.return_value = ["SPY", "QQQ"]
    mock_fetch.return_value = [_make_row("SPY"), _make_row("QQQ")]
    mock_bq.replace_ohlcv_rows.return_value = 2

    result = ohlcv_daily.run(TRADE_DATE)

    assert result.rows_written == 2
    assert result.errors == []
    assert not result.has_critical_errors


@patch("driftwatch.pipelines.ohlcv_daily.bq_client")
@patch("driftwatch.pipelines.ohlcv_daily.fetch_all_ohlcv")
@patch("driftwatch.pipelines.ohlcv_daily.load_tickers")
def test_run_partial_failure(mock_tickers, mock_fetch, mock_bq):
    # QQQ fails to fetch
    mock_tickers.return_value = ["SPY", "QQQ"]
    mock_fetch.return_value = [_make_row("SPY")]
    mock_bq.replace_ohlcv_rows.return_value = 1

    result = ohlcv_daily.run(TRADE_DATE)

    assert result.rows_written == 1
    assert len(result.errors) == 1
    assert "QQQ" in result.errors[0]
    assert result.has_critical_errors


@patch("driftwatch.pipelines.ohlcv_daily.bq_client")
@patch("driftwatch.pipelines.ohlcv_daily.fetch_all_ohlcv")
@patch("driftwatch.pipelines.ohlcv_daily.load_tickers")
def test_run_all_symbols_fail(mock_tickers, mock_fetch, mock_bq):
    mock_tickers.return_value = ["SPY", "QQQ"]
    mock_fetch.return_value = []
    mock_bq.replace_ohlcv_rows.return_value = 0

    result = ohlcv_daily.run(TRADE_DATE)

    assert result.rows_written == 0
    assert len(result.errors) == 2
    assert result.has_critical_errors
