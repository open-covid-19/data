import warnings
from typing import Any, Dict, List, Tuple
from numpy import issubdtype, number
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype
from .cast import safe_float_cast


def _detect_perform_action(msg: str, action: str):
    if action == "warn":
        warnings.warn(msg)
    elif action == "raise":
        raise ValueError(msg)
    else:
        raise TypeError("Unknown action {}".format(action))


def detect_correct_schema(schema: Dict[str, type], data: DataFrame, action: str = "warn") -> None:
    missing_columns = set(schema.keys()).difference(data.columns)
    if len(missing_columns) > 0:
        _detect_perform_action("Missing columns from schema: {}".format(missing_columns), action)


def detect_null_columns(schema: Dict[str, type], data: DataFrame, action: str = "warn") -> None:
    for column in data.columns:
        if sum(~data[column].isnull()) == 0:
            _detect_perform_action("Null column detected: " + column, action)


def detect_zero_columns(schema: Dict[str, type], data: DataFrame, action: str = "warn") -> None:
    for column in data.columns:
        if len(data[column]) == 0:
            continue
        if not is_numeric_dtype(data[column]):
            continue
        if sum(data[column].apply(safe_float_cast).fillna(0).apply(abs)) < 1:
            _detect_perform_action("All-zeroes column detected: " + column, action)
