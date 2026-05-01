"""Tests for sigforge.models."""
from __future__ import annotations

import datetime

import pytest

from sigforge.models import EventRow, FeatureRow, PipelineResult, ProfileRow, RawBar


class TestRawBar:
    def test_to_bq_dict_date_is_string(self):
        bar = RawBar(symbol="AAPL", trade_date=datetime.date(2025, 4, 1))
        d = bar.to_bq_dict()
        assert d["trade_date"] == "2025-04-01"
        assert isinstance(d["ingested_at"], str)

    def test_to_csv_dict_excludes_metadata(self):
        bar = RawBar(symbol="AAPL", trade_date=datetime.date(2025, 4, 1))
        d = bar.to_csv_dict()
        assert "ingested_at" not in d
        assert "data_source" not in d

    def test_optional_fields_default_none(self):
        bar = RawBar(symbol="MSFT", trade_date=datetime.date(2025, 1, 1))
        assert bar.open is None
        assert bar.volume is None


class TestProfileRow:
    def test_to_bq_dict_dates_as_strings(self):
        row = ProfileRow(symbol="AAPL", snapshot_date=datetime.date(2025, 3, 1))
        d = row.to_bq_dict()
        assert d["snapshot_date"] == "2025-03-01"
        assert isinstance(d["ingested_at"], str)

    def test_gics_fields_optional(self):
        row = ProfileRow(symbol="X", snapshot_date=datetime.date(2025, 1, 1))
        assert row.gics_sector is None
        assert row.gics_sub_industry is None


class TestFeatureRow:
    def test_to_bq_dict(self):
        row = FeatureRow(
            symbol="NVDA",
            feature_date=datetime.date(2025, 6, 1),
            run_id="test-run",
            rb_rolling_beta=1.2,
        )
        d = row.to_bq_dict()
        assert d["feature_date"] == "2025-06-01"
        assert d["rb_rolling_beta"] == pytest.approx(1.2)

    def test_to_csv_dict_excludes_metadata(self):
        row = FeatureRow(
            symbol="TSLA",
            feature_date=datetime.date(2025, 1, 1),
            run_id="r",
        )
        d = row.to_csv_dict()
        assert "ingested_at" not in d
        assert "run_id" not in d

    def test_all_feature_fields_nullable(self):
        row = FeatureRow(symbol="X", feature_date=datetime.date(2025, 1, 1), run_id="r")
        for field in FeatureRow.model_fields:
            if field.startswith(("rb_", "ms_", "cr_", "fu_")):
                assert getattr(row, field) is None


class TestEventRow:
    def test_auto_generated_event_id(self):
        kwargs = dict(
            symbol="AAPL",
            event_date=datetime.date(2025, 1, 1),
            event_type="dividend",
            source="manual",
            detection_run_id="r",
        )
        e1 = EventRow(**kwargs)
        e2 = EventRow(**kwargs)
        assert e1.event_id != e2.event_id

    def test_to_bq_dict_dates_as_strings(self):
        e = EventRow(
            symbol="AAPL",
            event_date=datetime.date(2025, 4, 1),
            event_type="split",
            source="claude_auto",
            detection_run_id="r",
        )
        d = e.to_bq_dict()
        assert d["event_date"] == "2025-04-01"
        assert isinstance(d["detected_at"], str)


class TestPipelineResult:
    def test_initial_state(self):
        r = PipelineResult()
        assert r.symbols_processed == 0
        assert r.rows_written == 0
        assert not r.has_critical_errors

    def test_add_error(self):
        r = PipelineResult()
        r.add_error("AAPL: module failed")
        assert r.has_critical_errors
        assert len(r.errors) == 1
