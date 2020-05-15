import re
import sys
import math
from random import shuffle
from functools import partial
from typing import Any, Dict, List, Tuple
from multiprocessing import cpu_count

from tqdm import tqdm
from multiprocess import Pool
from pandas import DataFrame, Series, Int64Dtype, merge, read_csv, concat, isna

from lib.cast import safe_int_cast
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from lib.time import datetime_isoformat
from lib.utils import ROOT


class WeatherPipeline(DefaultPipeline):
    @staticmethod
    def haversine_distance(
        latlon1: Tuple[float, ...], latlon2: Tuple[float, ...], radius: float = 6373.0
    ):
        """ Compute the distance between two <latitude, longitude> pairs in kilometers """

        # Convert to radians
        latlon1 = tuple(map(lambda x: math.radians(float(x)), latlon1))
        latlon2 = tuple(map(lambda x: math.radians(float(x)), latlon2))

        # Compute the pairwise deltas
        deltaLon = latlon2[1] - latlon1[1]
        deltaLat = latlon2[0] - latlon1[0]

        # Apply Haversine formula
        a = (
            math.sin(deltaLat / 2) ** 2
            + math.cos(latlon1[0]) * math.cos(latlon2[0]) * math.sin(deltaLon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return radius * c

    @staticmethod
    def nearest_station(stations, latlon: Tuple[int, int]):
        distances = stations.apply(
            lambda row: WeatherPipeline.haversine_distance(latlon, (row.lat, row.lon)), axis=1
        )
        idxmin = distances.idxmin()
        nearest = stations.iloc[idxmin].copy()
        nearest["distance"] = distances.iloc[idxmin]
        return nearest

    @staticmethod
    def fix_temp(value: int):
        value = safe_int_cast(value)
        return None if value is None else "%.1f" % (value / 10.0)

    @staticmethod
    def station_records(station_cache: Dict[str, DataFrame], stations: DataFrame, location: Series):

        # Get the nearest station from our list of stations given lat and lon
        nearest = WeatherPipeline.nearest_station(stations, (location.latitude, location.longitude))

        # Query the cache and pull data only if not already there
        if nearest.id not in station_cache:

            # Read the records from the nearest station
            station_url = (
                "https://www.ncei.noaa.gov/data"
                "/global-historical-climatology-network-daily/access/{}.csv"
            ).format(nearest.id)
            column_mapping = {
                "DATE": "date",
                "STATION": "station",
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
            data["distance"] = "%.03f" % nearest.distance

            # Save into the cache
            output_columns = [
                "date",
                "station",
                "distance",
                "minimum_temperature",
                "maximum_temperature",
                "rainfall",
                "snowfall",
            ]
            station_cache[nearest.id] = data[[col for col in output_columns if col in data.columns]]

        # Return all the available data from the records
        data = station_cache[nearest.id].copy()
        data["key"] = location.key
        return data

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts):

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
        stations = stations.groupby(["id", "lat", "lon"]).sum()
        stations = stations[stations.measurement.apply(lambda x: all(m in x for m in measurements))]
        stations = stations.reset_index()

        # Get all the POI from metadata and go through each key
        metadata = aux["metadata"][["key"]]
        metadata = metadata.merge(aux["wikidata"][["key", "latitude", "longitude"]]).dropna()

        # Use a cache to avoid having to query the same station multiple times
        station_cache: Dict[str, DataFrame] = {}

        # Make sure the stations and the cache is sent to each function call
        map_func = partial(WeatherPipeline.station_records, station_cache, stations)

        # We don't care about the index while iterating over each metadata item
        iter_values = [record for _, record in metadata.iterrows()]

        # Shuffle the iterables to try to make better use of the caching
        shuffle(iter_values)

        # Bottleneck is network so we can use lots of threads in parallel
        records = list(
            tqdm(Pool(cpu_count()).imap_unordered(map_func, iter_values), total=len(metadata),)
        )

        return concat(records).sort_values(["key", "date"])


class WeatherPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "date": str,
        "key": str,
        "station": str,
        "distance": float,
        "minimum_temperature": float,
        "maximum_temperature": float,
        "rainfall": float,
        "snowfall": float,
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [(WeatherPipeline(), {})]
