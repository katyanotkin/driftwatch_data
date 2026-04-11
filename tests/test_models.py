import datetime
from driftwatch.models import OHLCVRow


def _make_row(**kwargs) -> OHLCVRow:
    defaults = dict(
        symbol="SPY",
        trade_date=datetime.date(2026, 3, 1),
        open=100.0, high=110.0, low=90.0, close=105.0,
        volume=1_000_000, avg_volume_30d=900_000.0, pe_ratio=22.5,
    )
    return OHLCVRow(**{**defaults, **kwargs})


def test_to_csv_dict_excludes_bq_fields():
    row = _make_row()
    d = row.to_csv_dict()
    assert "ingested_at" not in d
    assert "data_source" not in d

def test_to_csv_dict_trade_date_is_string():
    row = _make_row()
    d = row.to_csv_dict()
    assert isinstance(d["trade_date"], str)
    assert d["trade_date"] == "2026-03-01"

def test_to_csv_dict_values():
    row = _make_row()
    d = row.to_csv_dict()
    assert d["symbol"] == "SPY"
    assert d["close"] == 105.0

def test_to_csv_dict_optional_none():
    row = _make_row(pe_ratio=None)
    assert row.to_csv_dict()["pe_ratio"] is None
