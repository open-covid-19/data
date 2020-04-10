from covid_io import fuzzy_text, read_file

import os
import re
import sys
from pathlib import Path
from argparse import ArgumentParser
from datetime import date, datetime, timedelta
from typing import List, Dict

import numpy
import pandas
import requests
from bs4.element import Tag
from bs4 import BeautifulSoup
from pandas import DataFrame, Series
from scipy import optimize

import matplotlib
import matplotlib.pyplot
from matplotlib.ticker import MaxNLocator
import seaborn
seaborn.set()


# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Used for deterministic SVG files, see https://stackoverflow.com/a/48110626
matplotlib.rcParams['svg.hashsalt'] = 0


def series_converter(series: pandas.Series):
    if series.name in ('Latitude', 'Longitude'):
        return series.astype(float).apply(lambda x: '' if pandas.isna(x) else '%.6f' % x)
    elif series.name in ('Confirmed', 'Estimated', 'Deaths', 'Population'):
        return series.apply(safe_int_cast).astype('Int64')
    elif series.name.startswith('New') or series.name.startswith('Current'):
        return series.astype(float).round().astype('Int64')
    else:
        return series.fillna('').astype(str)


def timezone_adjust(timestamp: str, offset: int):
    ''' Adjust hour difference between a timezone and GMT+1 '''
    timestamp = datetime.fromisoformat(timestamp)
    if timestamp.hour <= 24 - offset:
        return timestamp.date().isoformat()
    else:
        return (timestamp + timedelta(days=1)).date().isoformat()


def merge_previous(data: DataFrame, prev: DataFrame, index_columns: list):
    ''' Merges a DataFrame with the latest Open COVID-19 data, overwrites rows if necessary '''

    # Only look at columns present in the snapshot data
    prev = prev[set(data.columns) & set(prev.columns)]

    # Remove all repeated records from the previous dataset
    data = data.set_index(index_columns)
    prev = prev.set_index(index_columns)
    prev = prev.drop(list(set(data.index) & set(prev.index)))

    # Create new dataset of previous + current
    return pandas.concat([prev, data], sort=False).reset_index()


def read_metadata():
    ''' Reads the country and region metadata file '''

    column_dtypes = {
        'Date': str,
        'CountryCode': str,
        'CountryName': str,
        'RegionCode': str,
        'RegionName': str,
        '_RegionLabel': str,
        'Confirmed': 'Int64',
        'Deaths': 'Int64',
        'Latitude': str,
        'Longitude': str,
        'Population': 'Int64'
    }

    metadata = read_file(
        ROOT / 'input' / 'metadata.csv', dtype=column_dtypes, keep_default_na=False, na_values=[''])

    # Make sure that all entries have a valid region label column
    metadata['_RegionLabel'] = metadata.apply(_infer_region_label, axis=1)

    return metadata


def _infer_region_label(row: Series):
    if '_RegionLabel' in row and not pandas.isna(row['_RegionLabel']):
        return fuzzy_text(row['_RegionLabel'])
    elif 'RegionName' in row and not pandas.isna(row['RegionName']):
        return fuzzy_text(row['RegionName'])
    else:
        return None


def dataframe_output(data: DataFrame, code: str = None, metadata_merge: str = 'inner'):
    '''
    This function performs the following steps:
    1. Sorts the dataset by date and country / region
    2. Merges the data with country / region metadata
    '''
    # If no country code is given, then this is region-level metadata
    if code is None:
        data['RegionCode'] = None
    else:
        data['CountryCode'] = code

    # We should always have country code column
    assert 'CountryCode' in data.columns

    # Core columns are those that appear in all datasets and can be used for merging with metadata
    core_columns = open(ROOT / 'input' / 'output_columns.csv').readline().strip().split(',')

    # Data from https://developers.google.com/public-data/docs/canonical/countries_csv and Wikipedia
    metadata = read_metadata()

    # Make sure _RegionLabel column exists for all region-label data
    if code is not None:
        data['_RegionLabel'] = data.apply(_infer_region_label, axis=1)

    # Try to merge with metadata using keys in decreasing order of concreteness
    merge_keys = ['Date', 'CountryCode', 'Confirmed', 'Deaths']
    for key in ('Key', 'RegionCode', '_RegionLabel'):
        if key in data.columns:
            data_ = data[[key] + merge_keys].merge(metadata, how=metadata_merge)
            if len(data_) > 0:
                data = data_
                break

    # Preserve the order of the core columns and ensure records are sorted
    data = data[core_columns].sort_values(core_columns)

    # Make sure the core columns have the right data type
    for column in data.columns:
        data[column] = series_converter(data[column])

    # Output time-series dataset to sys.out
    data.to_csv(sys.stdout, header=None, index=False)


def get_outbreak_mask(data: DataFrame, threshold: int = 10):
    ''' Returns a mask for > N confirmed cases. Used to filter out uninteresting dates '''
    return data['Confirmed'] > threshold


def _logistic_function(X: float, a: float, b: float, c: float):
    '''
    Used for prediction model. Uses the function:
    `f(x) = a * e^(-b * e^(-cx))`
    '''
    return a * numpy.exp(-b * numpy.exp(-c * X))


def _forward_indices(indices: list, window: int):
    ''' Adds `window` indices to a list of dates '''
    date_indices = [date.fromisoformat(idx) for idx in indices]
    for _ in range(window):
        date_indices.append(date_indices[-1] + timedelta(days=1))
    return [idx.isoformat() for idx in date_indices]


def compute_forecast(data: pandas.Series, window: int):
    '''
    Perform a forecast of `window` days past the last day of `data`, including a model estimate of
    all days already existing in `data`.
    '''

    # Some of the parameter fittings result in overflow
    numpy.seterr(all='ignore')

    # Perform a simple fit of all available data up to this date
    X, y = list(range(len(data))), data.tolist()
    # Providing a reasonable initial guess is crucial for this model
    params, _ = optimize.curve_fit(
        _logistic_function, X, y, maxfev=int(1E6), p0=[max(y), numpy.median(X), .1])

    # Append N new days to our indices
    date_indices = _forward_indices(data.index, window)

    # Perform projection with the previously estimated parameters
    projected = [_logistic_function(x, *params) for x in range(len(X) + window)]
    return pandas.Series(projected, index=date_indices, name='Estimated')


def _plot_options():
    return {'figsize': (16, 8), 'fontsize': 'x-large', 'grid': True}


def _plot_save(fname: str, ax):
    # Add legend
    ax.legend(loc='upper left', fontsize='x-large')
    # Remove X label
    ax.xaxis.set_label_text('')
    # Make Y axis int only
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    # Save the figure
    ax.get_figure().tight_layout()
    ax.get_figure().savefig(fname)
    # Close the figure
    matplotlib.pyplot.close(ax.get_figure())


def plot_column(fname: str, data: pandas.Series):
    df = DataFrame({data.name: data.iloc[-14:]})
    ax = df.plot(kind='bar', **_plot_options())
    _plot_save(fname, ax)


def plot_forecast(fname: str, confirmed: pandas.Series, estimated: pandas.Series):

    # Replace all the indices from data with zeroes in our projected data
    projected = estimated.copy().iloc[-14:]
    projected[confirmed.dropna().index] = 0

    # Add new date indices to the original data and fill them with zeroes
    confirmed = confirmed.copy()
    for index in sorted(set(projected.index) - set(confirmed.index)):
        confirmed.loc[index] = 0
    confirmed = confirmed[projected.index]

    df = DataFrame({'Confirmed': confirmed, 'Projected': projected})
    ax = df.plot(kind='bar', **_plot_options())
    ax.plot(estimated.index, estimated, color='red', label='Estimate')
    _plot_save(fname, ax)


def compute_record_key(record: dict):
    ''' Outputs the primary key for a dataframe row '''
    region_code = record.get('RegionCode')
    country_code = record['CountryCode']
    key_suffix = '' if not region_code or pandas.isna(region_code) else '_%s' % region_code
    return country_code + key_suffix


def pivot_table(data: DataFrame, pivot_name: str = 'Pivot'):
    ''' Put a table in our preferred format when the regions are columns and date is index '''
    dates = data.index.tolist() * len(data.columns)
    pivots = sum([[name] * len(column) for name, column in data.iteritems()], [])
    values = sum([column.tolist() for name, column in data.iteritems()], [])
    records = zip(dates, pivots, values)
    return DataFrame.from_records(records, columns=['Date', pivot_name, 'Value'])


def cumsum(data: DataFrame, keys: List[str] = None):
    ''' Performs the cumsum operation per group '''

    # Make sure data is indexed
    if keys is not None:
        data = data.set_index(keys)

    # Make sure data is sorted
    data = data.sort_index()

    # Perform the cumsum per group
    data = data.groupby(level=0).cumsum()

    # If there are multiple entries for a key, add them up
    data = data.loc[~data.index.duplicated(keep='last')]

    return data


def safe_int_cast(value):
    if value is None:
        return None
    if pandas.isna(value):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if value == '':
        return None
    try:
        value = str(value)
        value = re.sub(r',', '', value)
        value = re.sub(r'−', '-', value)
        return round(float(value))
    except:
        return None


def safe_datetime_parse(value: str, date_format: str):
    try:
        return datetime.strptime(value, date_format)
    except:
        return None
