"""
Module dedicated to utility functions used throughout the project
"""

from pandas import Series


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

    return Series(
        cleaned_station_names[station] 
        for station in series
    )
