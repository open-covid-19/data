#!/usr/bin/env python

'''
Parse weather from NOAA database to get daily max/min temperatures, rainfall
and snowfall for each location in the metadata file.
'''

import re
import sys
import math
from multiprocessing.pool import ThreadPool
from typing import Tuple

from tqdm import tqdm
from pandas import read_csv, concat, isna

from utils import read_metadata, safe_int_cast

def haversine_distance(latlon1: Tuple[float, float], latlon2: Tuple[float, float], radius: float = 6373.0):
    ''' Compute the distance between two <latitude, longitude> pairs in kilometers '''

    # Convert to radians
    latlon1 = tuple(map(lambda x: math.radians(float(x)), latlon1))
    latlon2 = tuple(map(lambda x: math.radians(float(x)), latlon2))

    # Compute the pairwise deltas
    deltaLon = latlon2[1] - latlon1[1]
    deltaLat = latlon2[0] - latlon1[0]

    # Apply Haversine formula
    a = math.sin(deltaLat / 2) ** 2 + math.cos(latlon1[0]) * math.cos(latlon2[0]) * math.sin(deltaLon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius * c


def nearest_station(stations, latlon: Tuple[int, int]):
    distances = stations.apply(lambda row: haversine_distance(latlon, (row.lat, row.lon)), axis=1)
    idxmin = distances.idxmin()
    nearest = stations.iloc[idxmin].copy()
    nearest['distance'] = distances.iloc[idxmin]
    return nearest


def fix_temp(value: int):
    value = safe_int_cast(value)
    return None if value is None else '%.1f' % (value / 10.)


def station_records(location):

    # DataFrame.iterrows() outputs index and record, we only want the record
    location = location[1]

    # Get the nearest station from our list of stations given lat and lon
    nearest = nearest_station(stations, (location.Latitude, location.Longitude))

    # Read the records from the nearest station
    station_url = ('https://www.ncei.noaa.gov/data'
                   '/global-historical-climatology-network-daily/access/{}.csv').format(nearest.id)
    column_mapping = {
        'DATE': 'Date',
        'STATION': 'Station',
        'TMIN': 'MinimumTemperature',
        'TMAX': 'MaximumTemperature',
        'PRCP': 'Rainfall',
        'SNOW': 'Snowfall',
    }
    data = read_csv(station_url, usecols=lambda column: column in column_mapping.keys())
    data = data.rename(columns=column_mapping)

    # Convert temperature to correct values
    data['MinimumTemperature'] = data['MinimumTemperature'].apply(fix_temp)
    data['MaximumTemperature'] = data['MaximumTemperature'].apply(fix_temp)

    # Get only data for 2020 and add location values
    data = data[data.Date > '2019-12-31']
    data['Key'] = location.Key
    data['Distance'] = '%.03f' % nearest.distance

    # Return all the available data from the records
    output_columns = [
        'Key',
        'Date',
        'Station',
        'Distance',
        'MinimumTemperature',
        'MaximumTemperature',
        'Rainfall',
        'Snowfall']
    return data[[col for col in output_columns if col in data.columns]]


# Get all the weather stations with data up until 2020
stations_url = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt'
stations = read_csv(
    stations_url, sep=r'\s+', names=('id', 'lat', 'lon', 'measurement', 'year_start', 'year_end'))
stations = stations[stations.year_end == 2020][['id', 'lat', 'lon', 'measurement']]

# Filter stations that at least provide max and min temps
measurements = ['TMIN', 'TMAX']
stations = stations.groupby(['id', 'lat', 'lon']).sum()
stations = stations[stations.measurement.apply(lambda x: all(m in x for m in measurements))]
stations = stations.reset_index()

# Get all the POI from metadata and go through each key
metadata = read_metadata()[['Key', 'Latitude', 'Longitude']]
# Bottleneck is network so we can use lots of threads in parallel
records = list(tqdm(ThreadPool(8).imap_unordered(
    station_records, metadata.iterrows()), total=len(metadata)))

concat(records).sort_values(['Key', 'Date']).to_csv(sys.stdout, index=False)
