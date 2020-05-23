from datetime import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.utils import grouped_diff

_gh_base_url = "https://raw.github.com/covid19-eu-zh/covid19-eu-data/master/dataset"


class Covid19EuDataPipeline(DefaultPipeline):
    def __init__(self, country_code: str):
        super().__init__()
        self.country_code = country_code
        self.data_urls = ["{}/covid-19-{}.csv".format(_gh_base_url, country_code.lower())]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0].rename(
            columns={
                "datetime": "date",
                "country": "country_code",
                "nuts_2": "match_string",
                "nuts_3": "match_string",
                "cases": "confirmed",
                "deaths": "deceased",
                "tests": "tested",
                "recovered": "recovered",
                "hospitalized": "hospitalized",
                "intensive_care": "icu",
            }
        )
        data["date"] = data["date"].apply(lambda x: datetime.fromisoformat(x))
        data["date"] = data["date"].apply(lambda x: x.date().isoformat())

        # Remove bogus data
        blacklist = ("unknown", "unknown county", "nezjištěno", "outside mainland norway")
        data = data.dropna(subset=["match_string"])
        data.match_string = data.match_string.str.lower()
        data = data[~data["match_string"].isin(blacklist)]
        data = data[~data["match_string"].apply(lambda x: len(x) == 0 or x.startswith("http"))]

        # Remove unnecessary columns
        data = data[[col for col in data.columns if not "/" in col]]

        # Some tables have repeated data
        data = data.groupby(["country_code", "match_string", "date"]).last().reset_index()

        return grouped_diff(
            data,
            ["country_code", "match_string", "date"],
            skip=["tests", "recovered", "hospitalized", "icu"],
        )
