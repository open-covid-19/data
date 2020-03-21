import pandas
from pandas import DataFrame
from pathlib import Path

def _pivot_columns(data: DataFrame):
    ''' Gets the pivot columns in the right order from this dataframe '''
    pivot_columns = []
    if 'RegionCode' in data.columns: pivot_columns += ['RegionCode']
    if 'RegionName' in data.columns: pivot_columns += ['RegionName']
    if 'CountryCode' in data.columns: pivot_columns += ['CountryCode']
    if 'CountryName' in data.columns: pivot_columns += ['CountryName']
    return pivot_columns

def _core_columns(data: DataFrame, pivot_columns: list):
    ''' Gets the core columns in the right order from this dataframe '''
    return ['Date'] + pivot_columns + ['Confirmed', 'Deaths']

def dataframe_output(data: DataFrame, root: Path, name: str, metadata_merge: str = 'inner'):
    '''
    This function performs the following steps:
    1. Sorts the dataset by date and country / region
    2. Merges the data with country / region metadata
    3. Outputs dataset as CSV and JSON to output/<name>.csv and output/<name>.json
    4. Outputs dataset as CSV and JSON to output/<name>_latest.csv and output/<name>_latest.json
    '''
    # Core columns are those that appear in all datasets and can be used for merging with metadata
    pivot_columns = _pivot_columns(data)
    core_columns = _core_columns(data, pivot_columns)

    # Merge with metadata from appropriate helper dataset
    # Data from https://developers.google.com/public-data/docs/canonical/countries_csv and Wikipedia
    metadata = pandas.read_csv(root / 'input' / ('metadata_%s.csv' % name), dtype=str)
    meta_columns = [col for col in metadata.columns
                    if col not in core_columns and not col.startswith('_')]
    data = data.merge(metadata, how=metadata_merge)[core_columns + meta_columns]

    # Make sure the dataset is properly sorted
    data = data.sort_values(['Date', *pivot_columns])

    # Make sure the core columns have the right data type
    data['Date'] = data['Date'].astype(str)
    data['Confirmed'] = data['Confirmed'].astype(float).astype('Int64')
    data['Deaths'] = data['Deaths'].astype(float).astype('Int64')
    for pivot_column in pivot_columns:
        data[pivot_column] = data[pivot_column].astype(str)

    # Preserve the order of the core columns, which must be recomputed after merging with metadata
    core_columns = _core_columns(data, _pivot_columns(data))
    extra_columns = [col for col in data.columns if col not in core_columns]
    data = data[core_columns + extra_columns]

    # Output time-series dataset as-is
    data.to_csv(root / 'output' / ('%s.csv' % name), index=False)
    dataframe_to_json(data, root / 'output' / ('%s.json' % name), orient='records')

    # Output a subset to the _latest version of the dataset
    latest = pandas.DataFrame(columns=list(data.columns))
    for pivot_column in sorted(data[pivot_columns[0]].unique()):
        latest = pandas.concat([latest, data[data[pivot_columns[0]] == pivot_column].iloc[-1:]])

    latest.to_csv(root / 'output' / ('%s_latest.csv' % name), index=False)
    dataframe_to_json(latest, root / 'output' / ('%s_latest.json' % name), orient='records')

def dataframe_to_json(data: DataFrame, path: Path, **kwargs):
    ''' Saves a pandas DataFrame into a UTF-8 encoded JSON file '''
    with open(path, 'w', encoding='UTF-8') as file:
        data.to_json(file, force_ascii=False, **kwargs)
