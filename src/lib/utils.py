import os
from pathlib import Path
from typing import Any, List, Dict
from pandas import DataFrame, Series, concat, isna
from .cast import column_convert

ROOT = Path(os.path.dirname(__file__)) / '..' / '..'


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
    grouped = data.groupby(keys)
    return grouped.aggregate(agg_last_not_null).reset_index()


def output_table(schema: Dict[str, Any], data: DataFrame, *output_opts) -> DataFrame:
    '''
    This function performs the following operations:
    1. Filters out columns not in the output schema
    2. Converts each column to the appropriate type
    3. Sorts the values based on the column order
    4. Outputs the resulting data
    '''
    output_columns = list(schema.keys())

    # Filter only the output columns
    data = data[output_columns]

    # Make sure all columns are present and have the appropriate type
    for column, dtype in schema.items():
        if column not in data:
            data[column] = None
        data[column] = column_convert(data[column], dtype)

    # Output the sorted data
    return data.sort_values(output_columns)
