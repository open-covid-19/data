from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.utils import grouped_diff
from lib.time import datetime_isoformat


class DatadistaPipeline(DefaultPipeline):
    url_base = "https://raw.github.com/datadista/datasets/master/COVID%2019"
    data_urls: List[str] = [
        "{}/ccaa_covid19_casos_long.csv".format(url_base),
        "{}/ccaa_covid19_fallecidos_long.csv".format(url_base),
        "{}/ccaa_covid19_hospitalizados_long.csv".format(url_base),
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        join_keys = ["fecha", "CCAA"]
        join_opts = {"on": join_keys, "how": "outer"}
        data = dataframes[0]
        data = merge(data, dataframes[1], suffixes=("confirmed", "deceased"), **join_opts)
        data = merge(data, dataframes[2], suffixes=("", ""), **join_opts)

        data["country_code"] = "ES"
        data = data.rename(
            columns={
                "fecha": "date",
                "CCAA": "match_string",
                "totalconfirmed": "confirmed",
                "totaldeceased": "deceased",
                "total": "hospitalized",
            }
        ).sort_values(["match_string", "date"])

        # Keep only the columns we can process
        data = data[["date", "match_string", "confirmed", "deceased", "hospitalized"]]

        # Compute the diff for each day
        data = grouped_diff(data, keys=["match_string", "date"])

        # Add a country code column to all records
        data["country_code"] = "ES"
        return data
