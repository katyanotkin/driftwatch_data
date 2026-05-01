"""Tests for sigforge.bq_client — all BigQuery calls are mocked."""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pytest

from sigforge.models import EventRow, FeatureRow, ProfileRow, RawBar


@pytest.fixture()
def mock_bq_client():
    """Return a BQClient with the google.cloud.bigquery.Client patched out."""
    with patch("sigforge.bq_client.bigquery.Client") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        # Avoid importing settings at module level triggering env reads
        with patch("sigforge.bq_client.settings") as mock_settings:
            mock_settings.gcp_project_id = "test-project"
            mock_settings.gcp_dataset_id = "sigforge_test"
            mock_settings.gcp_location = "US"
            from sigforge.bq_client import BQClient
            client = BQClient()
            client._client = mock_instance
            yield client, mock_instance


class TestUpsertDaily:
    def test_upserts_rows(self, mock_bq_client):
        client, bq = mock_bq_client
        bq.query.return_value.result.return_value = []
        bq.insert_rows_json.return_value = []

        rows = [
            RawBar(symbol="AAPL", trade_date=datetime.date(2025, 4, 1), adj_close=170.0),
            RawBar(symbol="MSFT", trade_date=datetime.date(2025, 4, 1), adj_close=400.0),
        ]
        n = client.upsert_daily(rows)
        assert n == 2
        assert bq.insert_rows_json.called

    def test_empty_list_returns_zero(self, mock_bq_client):
        client, _ = mock_bq_client
        assert client.upsert_daily([]) == 0


class TestUpsertProfile:
    def test_upserts_rows(self, mock_bq_client):
        client, bq = mock_bq_client
        bq.query.return_value.result.return_value = []
        bq.insert_rows_json.return_value = []

        rows = [ProfileRow(symbol="AAPL", snapshot_date=datetime.date(2025, 3, 1))]
        n = client.upsert_profile(rows)
        assert n == 1

    def test_empty_list_returns_zero(self, mock_bq_client):
        client, _ = mock_bq_client
        assert client.upsert_profile([]) == 0


class TestUpsertFeatures:
    def test_upserts_rows(self, mock_bq_client):
        client, bq = mock_bq_client
        bq.query.return_value.result.return_value = []
        bq.insert_rows_json.return_value = []

        rows = [
            FeatureRow(symbol="AAPL", feature_date=datetime.date(2025, 4, 1), run_id="r1")
        ]
        n = client.upsert_features(rows)
        assert n == 1

    def test_empty_list_returns_zero(self, mock_bq_client):
        client, _ = mock_bq_client
        assert client.upsert_features([]) == 0


class TestInsertEvents:
    def test_inserts_event(self, mock_bq_client):
        client, bq = mock_bq_client
        bq.insert_rows_json.return_value = []

        event = EventRow(
            event_id="existing-uuid",
            symbol="AAPL",
            event_date=datetime.date(2025, 4, 1),
            event_type="dividend",
            source="claude_auto",
            detection_run_id="r",
        )
        n = client.insert_events([event])
        assert n == 1
        assert bq.insert_rows_json.called

    def test_inserts_new_events(self, mock_bq_client):
        client, bq = mock_bq_client
        bq.query.return_value.result.return_value = []
        bq.insert_rows_json.return_value = []

        event = EventRow(
            symbol="TSLA",
            event_date=datetime.date(2025, 4, 1),
            event_type="split",
            source="claude_auto",
            detection_run_id="r",
        )
        n = client.insert_events([event])
        assert n == 1

    def test_empty_list_returns_zero(self, mock_bq_client):
        client, _ = mock_bq_client
        assert client.insert_events([]) == 0

    def test_raises_on_bq_error(self, mock_bq_client):
        client, bq = mock_bq_client
        bq.query.return_value.result.return_value = []
        bq.insert_rows_json.return_value = [{"error": "some BQ error"}]

        event = EventRow(
            symbol="AAPL",
            event_date=datetime.date(2025, 4, 1),
            event_type="dividend",
            source="manual",
            detection_run_id="r",
        )
        with pytest.raises(RuntimeError, match="BQ insert errors"):
            client.insert_events_manual([event])

    def test_add_note_same_id_inserts_twice(self, mock_bq_client):
        """insert_events appends — calling twice with the same event produces two rows."""
        client, bq = mock_bq_client
        bq.query.return_value.result.return_value = []
        bq.insert_rows_json.return_value = []
        event = EventRow(
            event_id="fixed-id",
            symbol="AAPL",
            event_date=datetime.date(2025, 4, 1),
            event_type="manager_note",
            source="manual",
            detection_run_id="manual",
        )
        n1 = client.insert_events([event])
        assert n1 == 1

        n2 = client.insert_events([event])
        assert n2 == 1


class TestGetPreviousProfile:
    def test_native_date_types_accepted(self, mock_bq_client):
        """BQ returns datetime.date natively; ProfileRow must be constructed without error."""
        client, bq = mock_bq_client

        bq.query.return_value.result.return_value = [
            {
                "symbol": "AAPL",
                "snapshot_date": datetime.date(2025, 1, 1),
                "ingested_at": datetime.datetime(2025, 1, 1, 0, 0, 0),
                "name": None,
                "exchange": None,
                "gics_sector": "Technology",
                "gics_industry_group": None,
                "gics_industry": None,
                "gics_sub_industry": None,
                "market_cap": None,
                "enterprise_value": None,
                "pe_ratio": None,
                "forward_pe": None,
                "pb_ratio": None,
                "ps_ratio": None,
                "dividend_yield": None,
                "payout_ratio": None,
                "fifty_two_week_high": None,
                "fifty_two_week_low": None,
                "beta": None,
                "shares_outstanding": None,
                "shares_short": None,
                "short_ratio": None,
                "analyst_target_price": None,
                "analyst_recommendation": None,
                "data_source": "yfinance",
            }
        ]

        result = client.get_previous_profile(before_date=datetime.date(2025, 6, 1))
        assert "AAPL" in result
        profile = result["AAPL"]
        assert isinstance(profile.snapshot_date, datetime.date)
        assert profile.gics_sector == "Technology"
