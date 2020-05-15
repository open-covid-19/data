import os
import sys
import math
from typing import Tuple
from random import shuffle
from functools import partial
from multiprocess import Pool

from tqdm import tqdm
from pandas import DataFrame, read_csv
from multiprocessing import cpu_count

# This script must be run from /src
sys.path.append(os.getcwd())
from lib.io import read_file
from lib.utils import ROOT


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


def nearest_station(stations, record):
    latlon = record.latitude, record.longitude
    distances = stations.apply(lambda row: haversine_distance(latlon, (row.lat, row.lon)), axis=1)
    idxmin = distances.idxmin()
    nearest = stations.iloc[idxmin].copy()
    nearest["key"] = record.key
    nearest["distance"] = distances.iloc[idxmin]
    return nearest.to_dict()


# Get all the weather stations with data up until 2020
columns = ["id", "lat", "lon", "measurement", "year_start", "year_end"]
stations_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
stations = read_csv(stations_url, sep=r"\s+", names=columns)
stations = stations[stations.year_end == 2020][columns]

# Filter stations that at least provide max and min temps
measurements = ["TMIN", "TMAX"]
stations = stations.groupby([col for col in columns if col != "measurement"]).agg(
    lambda x: "|".join(x)
)
stations = stations[stations.measurement.apply(lambda x: all(m in x for m in measurements))]
stations = stations.reset_index()

# Get all the POI from metadata and go through each key
data_root = ROOT / "src" / "data"
wikidata = read_file(data_root / "wikidata.csv")
metadata = read_file(data_root / "metadata.csv")[["key"]]
metadata = metadata.merge(wikidata[["key", "latitude", "longitude"]]).dropna()

# Make sure the stations and the cache is sent to each function call
map_func = partial(nearest_station, stations)

# We don't care about the index while iterating over each metadata item
iter_values = [record for _, record in metadata.iterrows()]

# Shuffle the iterables to try to make better use of the caching
shuffle(iter_values)

# Bottleneck is network so we can use lots of threads in parallel
records = list(tqdm(Pool(cpu_count()).imap_unordered(map_func, iter_values), total=len(metadata),))

# Put into a dataframe and output
data = (
    DataFrame.from_records(records)
    .sort_values("key")
    .rename(columns={"id": "station", "lat": "latitude", "lon": "longitude"})
)
data[["key", "station", "latitude", "longitude", "distance", "measurement"]].to_csv(
    sys.stdout, index=False
)
