import sys
import pandas
from pandas import DataFrame
from pathlib import Path

def _series_converter(series: pandas.Series):
    if series.name == 'Estimated':
        return series.astype(float)
    if series.name == 'Confirmed' or series.name == 'Deaths':
        return series.astype(float).astype('Int64')
    else:
        return series.astype(str)

def dataframe_output(data: DataFrame, root: Path, name: str, metadata_merge: str = 'inner'):
    '''
    This function performs the following steps:
    1. Sorts the dataset by date and country / region
    2. Merges the data with country / region metadata
    '''
    # Core columns are those that appear in all datasets and can be used for merging with metadata
    core_columns = pandas.read_csv(root / 'input' / 'output_columns.csv').columns.tolist()
    pivot_columns = core_columns[:-2]

    # Merge with metadata from appropriate helper dataset
    # Data from https://developers.google.com/public-data/docs/canonical/countries_csv and Wikipedia
    metadata = pandas.read_csv(root / 'input' / ('metadata_%s.csv' % name), dtype=str)
    meta_columns = [col for col in metadata.columns
                    if col not in pivot_columns and not col.startswith('_')]
    data = data.merge(metadata, how=metadata_merge)

    # If a column does not exist in the dataset, output empty values
    for column in core_columns:
        if column not in data.columns: data[column] = ''

    # Make sure the core columns have the right data type
    for column in data.columns: data[column] = _series_converter(data[column])

    # Preserve the order of the core columns, which must be recomputed after merging with metadata
    all_columns = pivot_columns + meta_columns
    extra_columns = [col for col in all_columns if col not in core_columns]
    data = data[core_columns + extra_columns]

    # Make sure the dataset is properly sorted
    data = data.sort_values(core_columns)

    # Output time-series dataset to sys.out
    data.to_csv(sys.stdout, header=None, index=False)
