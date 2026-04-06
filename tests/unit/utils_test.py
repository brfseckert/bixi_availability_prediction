"""
Unit tests for standarize_station_name and archive_to_ingestion_schema utility functions.
"""

import pandas as pd
import pytest

from bixi_availability_prediction.utils import (
    archive_to_ingestion_schema,
    standarize_station_name,
)


EXPECTED_COLUMNS = [
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
]


@pytest.fixture
def archive_df():
    return pd.DataFrame({
        "station": ["Station A", "Station B"],
        "longitude": [-73.56, -73.57],
        "latitude": [45.51, 45.52],
        "commit_at": [
            "2026-01-15T10:30:00+00:00",
            "2026-01-15T10:30:00+00:00",
        ],
        "skipped_updates": [0, 3],
        "bikes": [5, 0],
        "stands": [10, 20],
    })


class TestArchiveToIngestionSchema:

    def test_output_columns_match_ingestion_schema(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert list(result.columns) == EXPECTED_COLUMNS

    def test_row_count_preserved(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert len(result) == len(archive_df)

    def test_bikes_mapped_to_num_bikes_available(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert list(result["num_bikes_available"]) == [5, 0]

    def test_stands_mapped_to_num_docks_available(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert list(result["num_docks_available"]) == [10, 20]

    def test_capacity_is_bikes_plus_stands(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert list(result["capacity"]) == [15, 20]

    def test_station_renamed_to_name(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert list(result["name"]) == ["Station A", "Station B"]

    def test_coordinates_renamed(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert list(result["lat"]) == [45.51, 45.52]
        assert list(result["lon"]) == [-73.56, -73.57]

    def test_commit_at_parsed_as_utc(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert result["last_reported_utc"].dt.tz is not None
        assert result["last_updated_utc"].dt.tz is not None

    def test_last_reported_equals_last_updated(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert (result["last_reported_utc"] == result["last_updated_utc"]).all()

    def test_operational_flags_default_to_one(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert (result["is_installed"] == 1).all()
        assert (result["is_renting"] == 1).all()
        assert (result["is_returning"] == 1).all()

    def test_station_id_is_na(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert result["station_id"].isna().all()

    def test_ingested_at_utc_is_nat(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert result["ingested_at_utc"].isna().all()

    def test_skipped_updates_not_in_output(self, archive_df):
        result = archive_to_ingestion_schema(archive_df)
        assert "skipped_updates" not in result.columns


class TestStandarizeStationName:

    def test_lowercases_names(self):
        series = pd.Series(["StationA", "STATIONB"])
        result = standarize_station_name(series)
        assert list(result) == ["stationa", "stationb"]

    def test_removes_spaces(self):
        series = pd.Series(["Station A", "Station B"])
        result = standarize_station_name(series)
        assert list(result) == ["stationa", "stationb"]

    def test_replaces_slash_with_underscore(self):
        series = pd.Series(["Rivard / Mont-Royal"])
        result = standarize_station_name(series)
        assert result.iloc[0] == "rivard_mont-royal"

    def test_removes_non_ascii_characters(self):
        series = pd.Series(["Métro Mont-Royal", "Café René"])
        result = standarize_station_name(series)
        assert result.iloc[0] == "mtromont-royal"
        assert result.iloc[1] == "cafren"

    def test_strips_leading_trailing_whitespace(self):
        series = pd.Series(["  Station  "])
        result = standarize_station_name(series)
        assert result.iloc[0] == "station"

    def test_duplicate_values_mapped_consistently(self):
        series = pd.Series(["Station A", "Station A", "Station B"])
        result = standarize_station_name(series)
        assert result.iloc[0] == result.iloc[1]
        assert result.iloc[0] != result.iloc[2]

    def test_returns_series(self):
        series = pd.Series(["Station A"])
        result = standarize_station_name(series)
        assert isinstance(result, pd.Series)

    def test_preserves_index(self):
        series = pd.Series(["Station A", "Station B"], index=[10, 20])
        result = standarize_station_name(series)
        assert list(result.index) == [10, 20]

    def test_empty_series(self):
        series = pd.Series([], dtype=str)
        result = standarize_station_name(series)
        assert len(result) == 0


class TestStandarizeWithArchiveSchema:
    """Tests demonstrating how standarize_station_name works with
    archive_to_ingestion_schema to enable joins across data sources."""

    def test_normalizes_archive_names_for_joining(self):
        archive_df = pd.DataFrame({
            "station": ["Métro Mont-Royal (Rivard / du Mont-Royal)"],
            "longitude": [-73.56],
            "latitude": [45.51],
            "commit_at": ["2026-01-15T10:30:00+00:00"],
            "skipped_updates": [0],
            "bikes": [5],
            "stands": [10],
        })
        capacity_df = pd.DataFrame({
            "station_name": ["Métro Mont-Royal (Rivard / du Mont-Royal)"],
            "capacity": [15],
        })

        ingested = archive_to_ingestion_schema(archive_df)
        ingested["station_key"] = standarize_station_name(ingested["name"])
        capacity_df["station_key"] = standarize_station_name(
            capacity_df["station_name"]
        )

        merged = ingested.merge(capacity_df, on="station_key")
        assert len(merged) == 1

    def test_handles_casing_differences_across_sources(self):
        archive_df = pd.DataFrame({
            "station": ["station abc"],
            "longitude": [-73.56],
            "latitude": [45.51],
            "commit_at": ["2026-01-15T10:30:00+00:00"],
            "skipped_updates": [0],
            "bikes": [5],
            "stands": [10],
        })
        capacity_df = pd.DataFrame({
            "station_name": ["Station ABC"],
            "capacity": [15],
        })

        ingested = archive_to_ingestion_schema(archive_df)
        ingested["station_key"] = standarize_station_name(ingested["name"])
        capacity_df["station_key"] = standarize_station_name(
            capacity_df["station_name"]
        )

        merged = ingested.merge(capacity_df, on="station_key")
        assert len(merged) == 1
