import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.cast import safe_int_cast
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff, pivot_table


def _parse_date(date: str):
    return datetime_isoformat(
        "%s-%d" % (date, datetime.datetime.now().year), "%d-%b-%Y"
    )


class CatchmeupPipeline(DefaultPipeline):
    data_urls: List[str] = [
        "https://docs.google.com/spreadsheets/d/1sgiz8x71QyIVJZQguYtG9n6xBEKdM4fXuDs_d8zKOmY/gviz/tq?tqx=out:csv&sheet=Data%20Provinsi"
    ]
    fetch_opts: List[Dict[str, Any]] = [{"ext": "csv"}]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: List[DataFrame], **parse_opts
    ) -> DataFrame:

        df = dataframes[0]
        df.columns = df.iloc[0]
        df = df.rename(columns={"Provinsi": "date"})
        df = df.iloc[1:].set_index("date")

        df = df[df.columns.dropna()]
        df = pivot_table(df.transpose(), pivot_name="match_string")
        df["date"] = df["date"].apply(_parse_date)
        df = df.dropna(subset=["date"])
        df = df.rename(columns={"value": "confirmed"})
        df["confirmed"] = df["confirmed"].apply(safe_int_cast).astype("Int64")

        keep_columns = ["date", "match_string", "confirmed"]
        df = df[df["match_string"] != "Total"]
        df = df[df["match_string"] != "Dalam proses investigasi"]
        df = grouped_diff(df[keep_columns], ["match_string", "date"])
        df["country_code"] = "ID"
        return df
