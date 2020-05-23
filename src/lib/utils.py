import os
from pathlib import Path
from functools import partial, reduce
from typing import Any, Callable, List, Dict, Tuple, Optional
from tqdm import tqdm
from pandas import DataFrame, Series, concat, isna, isnull
from .cast import column_convert

ROOT = Path(os.path.dirname(__file__)) / ".." / ".."
CACHE_URL = "https://raw.githubusercontent.com/open-covid-19/data/cache"


def get_or_default(dict_like: Dict, key: Any, default: Any):
    return dict_like[key] if key in dict_like and not isnull(dict_like[key]) else default


def pivot_table(data: DataFrame, pivot_name: str = "pivot") -> DataFrame:
    """ Put a table in our preferred format when the regions are columns and date is index """
    dates = data.index.tolist() * len(data.columns)
    pivots: List[str] = sum([[name] * len(column) for name, column in data.iteritems()], [])
    values: List[Any] = sum([column.tolist() for name, column in data.iteritems()], [])
    records = zip(dates, pivots, values)
    return DataFrame.from_records(records, columns=["date", pivot_name, "value"])


def agg_last_not_null(series: Series, progress_bar: Optional[tqdm] = None) -> Series:
    """ Aggregator function used to keep the last non-null value in a list of rows """
    if progress_bar:
        progress_bar.update()
    return reduce(lambda x, y: y if not isnull(y) else x, series)


def combine_tables(
    tables: List[DataFrame], keys: List[str], progress_label: str = None
) -> DataFrame:
    """ Combine a list of tables, keeping the right-most non-null value for every column """
    data = concat(tables)
    grouped = data.groupby([col for col in keys if col in data.columns])
    if not progress_label:
        return grouped.aggregate(agg_last_not_null).reset_index()
    else:
        progress_bar = tqdm(
            total=len(grouped) * len(data.columns), desc=f"Combine {progress_label} outputs"
        )
        agg_func = partial(agg_last_not_null, progress_bar=progress_bar)
        combined = grouped.aggregate(agg_func).reset_index()
        progress_bar.n = len(grouped) * len(data.columns)
        progress_bar.refresh()
        return combined


def grouped_transform(
    data: DataFrame,
    keys: List[str],
    transform: Callable,
    skip: List[str] = None,
    prefix: Tuple[str, str] = None,
) -> DataFrame:
    """ Computes the transform for each item within the group determined by `keys` """
    assert keys[-1] == "date", '"date" key should be last'
    data = data.sort_values(keys)
    group = data.groupby(keys[:-1])
    skip = [] if skip is None else skip
    prefix = ("", "") if prefix is None else prefix
    value_columns = [column for column in data.columns if column not in keys + skip]
    data = data.dropna(subset=value_columns, how="all").copy()
    for column in value_columns:
        if column in skip:
            continue
        if sum(~data[column].isna()) == 0:
            continue
        data[prefix[0] + column] = group[column].transform(transform)
    return data.rename(columns={col: prefix[1] + col for col in value_columns})


def grouped_diff(data: DataFrame, keys: List[str], skip: List[str] = None) -> DataFrame:
    return grouped_transform(
        data, keys, lambda x: x.ffill().diff(), skip=skip, prefix=("new_", "total_")
    )


def grouped_cumsum(data: DataFrame, keys: List[str], skip: List[str] = None) -> DataFrame:
    return grouped_transform(
        data, keys, lambda x: x.fillna(0).cumsum(), skip=skip, prefix=("total_", "new_")
    )
