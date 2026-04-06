"""
Unit tests for the station availability ingestion pipeline.

All external API calls are mocked to ensure tests are fast, deterministic,
and do not depend on network access.
"""

import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from bixi_availability_prediction.data.ingest_station_status import (
    fetch_station_status,
    fetch_station_info,
    build_availability_snapshot,
    save_snapshot,
)


# --- Fixtures ---

MOCK_STATION_STATUS_PAYLOAD = {
    "last_updated": 1710000000,
    "ttl": 5,
    "data": {
        "stations": [
            {
                "station_id": "1",
                "num_bikes_available": 5,
                "num_docks_available": 10,
                "is_installed": 1,
                "is_renting": 1,
                "is_returning": 1,
                "last_reported": 1709999900,
            },
            {
                "station_id": "2",
                "num_bikes_available": 0,
                "num_docks_available": 20,
                "is_installed": 1,
                "is_renting": 0,
                "is_returning": 0,
                "last_reported": 1709999800,
            },
        ]
    },
}

MOCK_STATION_INFO_PAYLOAD = {
    "last_updated": 1710000000,
    "ttl": 5,
    "data": {
        "stations": [
            {
                "station_id": "1",
                "name": "Station A",
                "lat": 45.51,
                "lon": -73.56,
                "capacity": 15,
            },
            {
                "station_id": "2",
                "name": "Station B",
                "lat": 45.52,
                "lon": -73.57,
                "capacity": 20,
            },
        ]
    },
}


def _mock_response(payload):
    """Create a mock requests.Response with given JSON payload."""
    mock = MagicMock()
    mock.json.return_value = payload
    mock.raise_for_status.return_value = None
    return mock


# --- Tests for fetch_station_status ---


class TestFetchStationStatus:

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_returns_dataframe_with_expected_columns(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_STATUS_PAYLOAD)
        df = fetch_station_status("http://fake-url")

        expected_columns = [
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "is_installed",
            "is_renting",
            "is_returning",
            "last_reported_utc",
            "last_updated_utc",
        ]
        assert list(df.columns) == expected_columns

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_returns_correct_number_of_rows(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_STATUS_PAYLOAD)
        df = fetch_station_status("http://fake-url")
        assert len(df) == 2

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_last_updated_utc_is_timezone_aware(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_STATUS_PAYLOAD)
        df = fetch_station_status("http://fake-url")
        assert df["last_updated_utc"].iloc[0].tzinfo is not None

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_last_reported_utc_is_timezone_aware(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_STATUS_PAYLOAD)
        df = fetch_station_status("http://fake-url")
        assert df["last_reported_utc"].dt.tz is not None

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_station_ids_match_input(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_STATUS_PAYLOAD)
        df = fetch_station_status("http://fake-url")
        assert set(df["station_id"]) == {"1", "2"}

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_raises_on_http_error(self, mock_get):
        mock_get.return_value = MagicMock()
        mock_get.return_value.raise_for_status.side_effect = Exception("404")
        with pytest.raises(Exception, match="404"):
            fetch_station_status("http://fake-url")


# --- Tests for fetch_station_info ---


class TestFetchStationInfo:

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_returns_dataframe_with_expected_columns(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_INFO_PAYLOAD)
        df = fetch_station_info("http://fake-url")
        assert list(df.columns) == ["station_id", "name", "lat", "lon", "capacity"]

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_returns_correct_number_of_rows(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_INFO_PAYLOAD)
        df = fetch_station_info("http://fake-url")
        assert len(df) == 2

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_station_names_are_strings(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_INFO_PAYLOAD)
        df = fetch_station_info("http://fake-url")
        assert df["name"].dtype == "object"

    @patch("bixi_availability_prediction.data.ingest_station_status.requests.get")
    def test_capacity_is_numeric(self, mock_get):
        mock_get.return_value = _mock_response(MOCK_STATION_INFO_PAYLOAD)
        df = fetch_station_info("http://fake-url")
        assert pd.api.types.is_numeric_dtype(df["capacity"])


# --- Tests for build_availability_snapshot ---


class TestBuildAvailabilitySnapshot:

    @patch("bixi_availability_prediction.data.ingest_station_status.fetch_station_info")
    @patch("bixi_availability_prediction.data.ingest_station_status.fetch_station_status")
    def test_merged_dataframe_has_all_columns(self, mock_status, mock_info):
        mock_status.return_value = pd.DataFrame(
            {
                "station_id": ["1", "2"],
                "num_bikes_available": [5, 0],
                "num_docks_available": [10, 20],
                "is_installed": [1, 1],
                "is_renting": [1, 0],
                "is_returning": [1, 0],
                "last_reported_utc": pd.to_datetime(
                    [1709999900, 1709999800], unit="s", utc=True
                ),
                "last_updated_utc": datetime(2024, 3, 9, 10, 0, tzinfo=timezone.utc),
            }
        )
        mock_info.return_value = pd.DataFrame(
            {
                "station_id": ["1", "2"],
                "name": ["Station A", "Station B"],
                "lat": [45.51, 45.52],
                "lon": [-73.56, -73.57],
                "capacity": [15, 20],
            }
        )

        df = build_availability_snapshot()

        expected_columns = {
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "is_installed",
            "is_renting",
            "is_returning",
            "last_reported_utc",
            "last_updated_utc",
            "name",
            "lat",
            "lon",
            "capacity",
            "ingested_at_utc",
        }
        assert set(df.columns) == expected_columns

    @patch("bixi_availability_prediction.data.ingest_station_status.fetch_station_info")
    @patch("bixi_availability_prediction.data.ingest_station_status.fetch_station_status")
    def test_ingested_at_utc_is_populated(self, mock_status, mock_info):
        mock_status.return_value = pd.DataFrame(
            {
                "station_id": ["1"],
                "num_bikes_available": [5],
                "num_docks_available": [10],
                "is_installed": [1],
                "is_renting": [1],
                "is_returning": [1],
                "last_reported_utc": pd.to_datetime([1709999900], unit="s", utc=True),
                "last_updated_utc": datetime(2024, 3, 9, 10, 0, tzinfo=timezone.utc),
            }
        )
        mock_info.return_value = pd.DataFrame(
            {
                "station_id": ["1"],
                "name": ["Station A"],
                "lat": [45.51],
                "lon": [-73.56],
                "capacity": [15],
            }
        )

        df = build_availability_snapshot()
        assert df["ingested_at_utc"].notna().all()

    @patch("bixi_availability_prediction.data.ingest_station_status.fetch_station_info")
    @patch("bixi_availability_prediction.data.ingest_station_status.fetch_station_status")
    def test_unmatched_station_gets_null_info(self, mock_status, mock_info):
        mock_status.return_value = pd.DataFrame(
            {
                "station_id": ["999"],
                "num_bikes_available": [3],
                "num_docks_available": [7],
                "is_installed": [1],
                "is_renting": [1],
                "is_returning": [1],
                "last_reported_utc": pd.to_datetime([1709999900], unit="s", utc=True),
                "last_updated_utc": datetime(2024, 3, 9, 10, 0, tzinfo=timezone.utc),
            }
        )
        mock_info.return_value = pd.DataFrame(
            {
                "station_id": ["1"],
                "name": ["Station A"],
                "lat": [45.51],
                "lon": [-73.56],
                "capacity": [15],
            }
        )

        df = build_availability_snapshot()
        assert df["name"].isna().iloc[0]
        assert df["capacity"].isna().iloc[0]


# --- Tests for save_snapshot ---


class TestSaveSnapshot:

    def test_creates_parquet_file(self, tmp_path):
        df = pd.DataFrame({"station_id": ["1"], "num_bikes_available": [5]})

        output_path = save_snapshot(df, str(tmp_path))

        assert output_path.endswith(".parquet")
        assert os.path.exists(output_path)
        result = pd.read_parquet(output_path)
        assert len(result) == 1
        assert list(result.columns) == ["station_id", "num_bikes_available"]

    def test_multiple_saves_create_separate_files(self, tmp_path):
        df = pd.DataFrame({"station_id": ["1"], "num_bikes_available": [5]})

        path1 = save_snapshot(df, str(tmp_path))
        # Ensure different timestamp
        import time
        time.sleep(1)
        path2 = save_snapshot(df, str(tmp_path))

        assert path1 != path2
        parquet_files = list(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 2

    def test_filename_contains_timestamp(self, tmp_path):
        df = pd.DataFrame({"station_id": ["1"]})

        output_path = save_snapshot(df, str(tmp_path))

        filename = os.path.basename(output_path)
        assert filename.startswith("station_availability_")
        # Timestamp format: YYYYMMDD_HHMMSS
        assert len(filename) == len("station_availability_YYYYMMDD_HHMMSS.parquet")

    def test_creates_output_directory(self, tmp_path):
        nested_dir = str(tmp_path / "nested" / "dir")
        df = pd.DataFrame({"station_id": ["1"]})

        output_path = save_snapshot(df, nested_dir)
        assert os.path.exists(output_path)
