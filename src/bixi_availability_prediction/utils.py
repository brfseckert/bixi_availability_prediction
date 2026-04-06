"""
Module dedicated to utility functions used throughout the project
"""

import pandas as pd
from pandas import Series, DataFrame


def standarize_station_name(series: Series) -> Series:
    """
    Remove non-ASCII charcaters, lowercase and strip station name
    """

    unique_stations = series.unique()
    cleaned_station_names = {
        station: (
            station.encode('ascii', 'ignore')
            .decode('ascii')
            .lower()
            .replace(' ', '')
            .replace('/', '_')
            .strip()
        )
        for station in unique_stations
    }

    return  series.map(cleaned_station_names)


def archive_to_ingestion_schema(df: DataFrame) -> DataFrame:
    """
    Convert an archive CSV DataFrame (columns: station, longitude, latitude,
    commit_at, skipped_updates, bikes, stands) to match the schema produced
    by ingest_station_status.build_availability_snapshot.
    """
    commit_at = pd.to_datetime(df["commit_at"], utc=True)

    return DataFrame({
        "station_id": pd.NA,
        "num_bikes_available": df["bikes"],
        "num_docks_available": df["stands"],
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported_utc": commit_at,
        "last_updated_utc": commit_at,
        "name": df["station"],
        "lat": df["latitude"],
        "lon": df["longitude"],
        "capacity": df["bikes"] + df["stands"],
        "ingested_at_utc": pd.NaT,
    })
