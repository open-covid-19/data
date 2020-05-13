import re
import datetime
import warnings
import pandas
from typing import Any, Callable, Optional


def safe_float_cast(value: Any) -> Optional[float]:
    if value is None:
        return None
    if pandas.isna(value):
        return None
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        return value
    if value == "":
        return None
    try:
        value = str(value)
        value = re.sub(r",", "", value)
        value = re.sub(r"−", "-", value)
        return float(value)
    except:
        return None


def safe_int_cast(value: Any, round_function: Callable[[float], int] = round) -> Optional[int]:
    value = safe_float_cast(value)
    if value is None:
        return None
    try:
        value = round_function(value)
        return value
    except:
        return None


def safe_datetime_parse(
    value: str, date_format: str, warn: bool = False
) -> Optional[datetime.datetime]:
    try:
        return datetime.datetime.strptime(str(value), date_format)
    except ValueError as exc:
        if warn:
            warnings.warn("Could not parse date {} using format {}".format(value, date_format))
        return None


def column_convert(series: pandas.Series, dtype: type) -> pandas.Series:
    if dtype == pandas.Int64Dtype():
        return series.apply(safe_int_cast).astype(dtype)
    if dtype == float:
        return series.apply(safe_float_cast).astype(dtype)
    if dtype == str:
        return series.fillna("").astype(str)

    raise ValueError("Unsupported dtype %r" % dtype)
