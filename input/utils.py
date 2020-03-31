import re
import sys
from pathlib import Path
from argparse import ArgumentParser
from datetime import date, datetime, timedelta

import numpy
import pandas
from pandas import DataFrame
from scipy import optimize
from unidecode import unidecode

import matplotlib
import matplotlib.pyplot
from matplotlib.ticker import MaxNLocator
import seaborn
seaborn.set()


# Used for deterministic SVG files, see https://stackoverflow.com/a/48110626
matplotlib.rcParams['svg.hashsalt'] = 0

# Define constants
URL_GITHUB_RAW = 'https://raw.githubusercontent.com'
URL_OPEN_COVID_19 = 'https://open-covid-19.github.io/data/data.csv'
COLUMN_DTYPES = {
    'Date': str,
    'CountryCode': str,
    'CountryName': str,
    'RegionCode': str,
    'RegionName': str,
    'Confirmed': 'Int64',
    'Deaths': 'Int64',
    'Latitude': str,
    'Longitude': str
}


def parse_level_args(args: list = sys.argv[1:]):
    ''' Parses the command line arguments to determine if this is country- or region-level data '''
    parser = ArgumentParser()
    parser.add_argument('level', choices=['country', 'region'])
    return parser.parse_args(args)



def read_csv(path: str, **kwargs):
    return pandas.read_csv(
        path, dtype=COLUMN_DTYPES, keep_default_na=False, na_values=[''], **kwargs)

def github_raw_url(project: str, path: str, branch: str = 'master') -> str:
    ''' Get the absolute URL of a file hosted on GitHub using the GitHub Raw URL format '''
    return '{base_url}/{project}/{branch}/{path}'.format(
        **{'base_url': URL_GITHUB_RAW, 'project': project, 'branch': branch, 'path': path})


def github_raw_dataframe(project: str, path: str, branch: str = 'master', **kwargs) -> pandas.DataFrame:
    ''' Read a dataframe from a file hosted using GitHub Raw '''
    url = github_raw_url(project, path, branch=branch)
    if url.endswith('csv'):
        return pandas.read_csv(url, **kwargs)
    elif url.endswith('json'):
        return pandas.read_json(url, **kwargs)
    else:
        raise ValueError('Unknown data type: %s' % url)


def series_converter(series: pandas.Series):
    if series.name in ('Latitude', 'Longitude'):
        return series.astype(float).apply(lambda x: '' if pandas.isna(x) else '%.6f' % x)
    elif series.name in ('Confirmed', 'Estimated', 'Deaths', 'Population'):
        return series.astype(float).round().astype('Int64')
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


def merge_previous(data: pandas.DataFrame, index_columns: list, filter_function):
    ''' Merges a DataFrame with the latest Open COVID-19 data, overwrites rows if necessary '''

    # Read live data and filter it as requested by argument
    prev_data = read_csv(URL_OPEN_COVID_19)
    prev_data = prev_data.loc[prev_data.apply(filter_function, axis=1)]

    # Only look at columns present in the snapshot data
    prev_data = prev_data[set(data.columns) & set(prev_data.columns)]

    # Remove all repeated records from the previous dataset
    data = data.set_index(index_columns)
    prev_data = prev_data.set_index(index_columns)
    for idx in (set(data.index) & set(prev_data.index)):
        prev_data = prev_data.drop(idx)

    # Create new dataset of previous + current
    return pandas.concat([prev_data, data], sort=False).reset_index()


def dataframe_output(data: DataFrame, root: Path, code: str = None, metadata_merge: str = 'inner'):
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

    # Add the key to each record
    data['Key'] = data.apply(compute_record_key, axis=1)

    # Core columns are those that appear in all datasets and can be used for merging with metadata
    core_columns = read_csv(root / 'input' / 'output_columns.csv').columns.tolist()

    # Data from https://developers.google.com/public-data/docs/canonical/countries_csv and Wikipedia
    metadata = read_csv(root / 'input' / 'metadata.csv')
    # Fuzzy matching of the region label, to avoid character encoding issues or small changes
    if '_RegionLabel' in data.columns:
        fuzzy_text = lambda txt: re.sub(r'[^a-z]', '', unidecode(str(txt)).lower())
        data['_RegionLabel'] = data['_RegionLabel'].apply(fuzzy_text)
        metadata['_RegionLabel'] = metadata['_RegionLabel'].apply(fuzzy_text)
    # Merge with metadata from appropriate helper dataset
    data = data.merge(metadata, how=metadata_merge)

    # If a column does not exist in the dataset, output empty values
    for column in core_columns:
        if column not in data.columns: data[column] = ''

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
    for _ in range(window): date_indices.append(date_indices[-1] + timedelta(days=1))
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
