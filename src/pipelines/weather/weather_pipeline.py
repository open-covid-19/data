import re
import sys
import math
from random import shuffle
from functools import partial
from typing import Any, Dict, List, Tuple
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool as Pool

import numpy
from tqdm.contrib import concurrent
from pandas import DataFrame, Series, Int64Dtype, merge, read_csv, concat, isna

from lib.cast import safe_int_cast
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from lib.time import datetime_isoformat
from lib.utils import ROOT


class WeatherPipeline(DefaultPipeline):

    # A bit of a circular dependency but we need the latitude and longitude to compute weather
    def fetch(self, cache: Dict[str, str], **fetch_opts) -> List[str]:
        return [ROOT / "output" / "tables" / "geography.csv"]

    @staticmethod
    def haversine_distance(
        stations: DataFrame, lat: float, lon: float, radius: float = 6373.0
    ) -> Series:
        """ Compute the distance between two <latitude, longitude> pairs in kilometers """

        # Compute the pairwise deltas
        lat_diff = stations.lat - lat
        lon_diff = stations.lon - lon

        # Apply Haversine formula
        a = numpy.sin(lat_diff / 2) ** 2
        a += math.cos(lat) * numpy.cos(stations.lat) * numpy.sin(lon_diff / 2) ** 2
        c = numpy.arctan2(numpy.sqrt(a), numpy.sqrt(1 - a)) * 2

        return radius * c

    @staticmethod
    def nearest_station(stations, lat: float, lon: float):
        # Compute the distance with each station
        distances = WeatherPipeline.haversine_distance(stations, lat, lon)

        # Return the closest station and its distance
        idxmin = distances.idxmin()
        return distances.loc[idxmin], stations.loc[idxmin]

    @staticmethod
    def fix_temp(value: int):
        value = safe_int_cast(value)
        return None if value is None else "%.1f" % (value / 10.0)

    @staticmethod
    def station_records(station_cache: Dict[str, DataFrame], stations: DataFrame, location: Series):

        # Get the nearest station from our list of stations given lat and lon
        distance, nearest = WeatherPipeline.nearest_station(stations, location.lat, location.lon)

        # Query the cache and pull data only if not already cached
        if nearest.id not in station_cache:

            # Read the records from the nearest station
            station_url = (
                "https://www.ncei.noaa.gov/data"
                "/global-historical-climatology-network-daily/access/{}.csv"
            ).format(nearest.id)
            column_mapping = {
                "DATE": "date",
                "STATION": "noaa_station",
                "TMIN": "minimum_temperature",
                "TMAX": "maximum_temperature",
                "PRCP": "rainfall",
                "SNOW": "snowfall",
            }
            data = read_csv(station_url, usecols=lambda column: column in column_mapping.keys())
            data = data.rename(columns=column_mapping)

            # Convert temperature to correct values
            data["minimum_temperature"] = data["minimum_temperature"].apply(
                WeatherPipeline.fix_temp
            )
            data["maximum_temperature"] = data["maximum_temperature"].apply(
                WeatherPipeline.fix_temp
            )

            # Get only data for 2020 and add location values
            data = data[data.date > "2019-12-31"]

            # Save into the cache
            station_cache[nearest.id] = data

        # Get station records from the cache
        data = station_cache[nearest.id].copy()

        # Return all the available data from the records
        output_columns = [
            "date",
            "key",
            "noaa_station",
            "noaa_distance",
            "minimum_temperature",
            "maximum_temperature",
            "rainfall",
            "snowfall",
        ]
        data["key"] = location.key
        data["noaa_distance"] = "%.03f" % distance
        return data[[col for col in output_columns if col in data.columns]]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ):

        # Get all the weather stations with data up until 2020
        stations_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
        stations = read_csv(
            stations_url,
            sep=r"\s+",
            names=("id", "lat", "lon", "measurement", "year_start", "year_end"),
        )
        stations = stations[stations.year_end == 2020][["id", "lat", "lon", "measurement"]]

        # Filter stations that at least provide max and min temps
        measurements = ["TMIN", "TMAX"]
        stations = stations.groupby(["id", "lat", "lon"]).agg(lambda x: "|".join(x))
        stations = stations[stations.measurement.apply(lambda x: all(m in x for m in measurements))]
        stations = stations.reset_index()

        # Get all the POI from metadata and go through each key
        metadata = dataframes[0][["key", "latitude", "longitude"]].dropna()

        # Convert all coordinates to radians
        stations["lat"] = stations.lat.apply(math.radians)
        stations["lon"] = stations.lon.apply(math.radians)
        metadata["lat"] = metadata.latitude.apply(math.radians)
        metadata["lon"] = metadata.longitude.apply(math.radians)

        # Use a cache to avoid having to query the same station multiple times
        station_cache: Dict[str, DataFrame] = {}

        # Make sure the stations and the cache are sent to each function call
        map_func = partial(WeatherPipeline.station_records, station_cache, stations)

        # We don't care about the index while iterating over each metadata item
        map_iter = [record for _, record in metadata.iterrows()]

        # Shuffle the iterables to try to make better use of the caching
        shuffle(map_iter)

        # Bottleneck is network so we can use lots of threads in parallel
        records = concurrent.thread_map(map_func, map_iter, total=len(metadata))

        return concat(records)


class WeatherPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "date": str,
        "key": str,
        "noaa_station": str,
        "noaa_distance": float,
        "minimum_temperature": float,
        "maximum_temperature": float,
        "rainfall": float,
        "snowfall": float,
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [(WeatherPipeline(), {})]
