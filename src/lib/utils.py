import os
from pathlib import Path
from typing import Any, List, Dict
from pandas import DataFrame, Series, concat, isna, isnull
from .cast import column_convert

ROOT = Path(os.path.dirname(__file__)) / '..' / '..'


def get_or_default(dict_like: Dict, key: Any, default: Any):
    return dict_like[key] if key in dict_like and not isnull(dict_like[key]) else default


def pivot_table(data: DataFrame, pivot_name: str = 'pivot') -> DataFrame:
    ''' Put a table in our preferred format when the regions are columns and date is index '''
    dates = data.index.tolist() * len(data.columns)
    pivots = sum([[name] * len(column) for name, column in data.iteritems()], [])
    values = sum([column.tolist() for name, column in data.iteritems()], [])
    records = zip(dates, pivots, values)
    return DataFrame.from_records(records, columns=['date', pivot_name, 'value'])


def agg_last_not_null(rows: DataFrame) -> Series:
    ''' Aggregator function used to keep the last non-null value in a list of rows '''
    for row in rows:
        if not isna(row):
            return row
    return rows.iloc[-1]


def combine_tables(tables: List[DataFrame], keys: List[str]) -> DataFrame:
    ''' Combine a list of tables, keeping the right-most non-null value for every column '''
    data = concat(tables)
    grouped = data.groupby([col for col in keys if col in data.columns])
    return grouped.aggregate(agg_last_not_null).reset_index()


def grouped_diff(data: DataFrame, keys: List[str]) -> DataFrame:
    ''' Computes the difference for each item within the group determined by `keys` '''
    assert keys[-1] == 'date', '"date" key should be last'
    data = data.sort_values(keys)
    group = data.groupby(keys[:-1])
    value_columns = [column for column in data.columns if column not in keys]
    for column in value_columns:
        if sum(~data[column].isna()) == 0:
            continue
        data[column] = group[column].transform(lambda x: x.ffill().diff())
    return data.dropna(subset=value_columns)
