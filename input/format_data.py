#!/usr/bin/env python

'''
This script performs some operations on data to ensure backwards-compatibility.
'''

import os
import shutil
from pathlib import Path
import pandas
from pandas import DataFrame
from utils import read_csv


def dataframe_to_json(data: DataFrame, path: Path, **kwargs):
    ''' Saves a pandas DataFrame into a UTF-8 encoded JSON file '''
    with open(path, 'w', encoding='UTF-8') as file:
        data.to_json(file, force_ascii=False, **kwargs)


def dataframe_split(data: DataFrame, pivot_columns: list, root: Path, name: str):
    '''
    This function performs the following steps:
    1. Outputs dataset as CSV and JSON to output/<name>.csv and output/<name>.json
    2. Outputs dataset as CSV and JSON to output/<name>_latest.csv and output/<name>_latest.json
    '''

    # Output time-series dataset as-is
    data.to_csv(root / 'output' / ('%s.csv' % name), index=False)
    dataframe_to_json(data, root / 'output' / ('%s.json' % name), orient='records')

    # Output a day's subset to the _latest version of the dataset
    latest = pandas.DataFrame(columns=list(data.columns))
    pivot_series = pandas.Series([''.join([str(row[col]) for col in pivot_columns])
                                  for _, row in data.iterrows()], index=data.index, dtype='O')
    for column in sorted(pivot_series.unique()):
        latest = pandas.concat([latest, data[pivot_series == column].iloc[-1:]])
    latest = latest.sort_values(['Date', *pivot_columns])

    latest.to_csv(root / 'output' / ('%s_latest.csv' % name), index=False)
    dataframe_to_json(latest, root / 'output' / ('%s_latest.json' % name), orient='records')


# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read the minimal data file and write to output
minimal = read_csv(ROOT / 'output' / 'data.csv').sort_values(['Date', 'Key'])
minimal.to_csv(ROOT / 'output' / 'data_minimal.csv', index=False)
dataframe_to_json(minimal, ROOT / 'output' / 'data_minimal.json', orient='records')

# Read the metadata file and write to output (including _latest)
metadata = read_csv(ROOT / 'input' / 'metadata.csv')
metadata = metadata[[col for col in metadata.columns if not col.startswith('_')]]
metadata.to_csv(ROOT / 'output' / 'metadata.csv', index=False)
dataframe_to_json(metadata, ROOT / 'output' / 'metadata.json', orient='records')

# Merge minimal with the metadata file to create full file and write to output
full = minimal.merge(metadata)
dataframe_split(full, ('CountryCode', 'RegionCode'), ROOT, 'data')

# Backwards compatibility: China dataset and Region -> RegionName
china = full[full['CountryCode'] == 'CN']
china = china[~china['RegionCode'].isna()]
dataframe_split(china, ('RegionCode',), ROOT, 'cn')
china['Region'] = china['RegionName']
dataframe_split(china, ('RegionCode',), ROOT, 'china')

# Backwards compatibility: Usa dataset and RegionName -> RegionCode
usa = full[full['CountryCode'] == 'US']
usa = usa[~usa['RegionCode'].isna()]
dataframe_split(usa, ('RegionCode',), ROOT, 'us')
usa['Region'] = usa['RegionCode']
dataframe_split(usa, ('RegionCode',), ROOT, 'usa')

# Backwards compatibility: Data used to be called "world" and had only country-level data
world = full[full['RegionCode'].isna()]
world = world[[col for col in world.columns if 'Region' not in col]]
dataframe_split(world, ('CountryCode',), ROOT, 'world')

# Read forecast file and output JSON format
forecast = read_csv(ROOT / 'output' / 'data_forecast.csv')
dataframe_to_json(forecast, ROOT / 'output' / 'data_forecast.json', orient='records')

# Read categories file and output JSON format
forecast = read_csv(ROOT / 'output' / 'data_forecast.csv')
dataframe_to_json(forecast, ROOT / 'output' / 'data_forecast.json', orient='records')