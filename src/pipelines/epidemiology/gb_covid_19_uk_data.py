from typing import Any, Dict, List
from pandas import DataFrame, concat, merge, NA
from lib.cast import safe_int_cast
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff


_gh_base_url = "https://raw.githubusercontent.com/tomwhite/covid-19-uk-data/master/data"


class Covid19UkDataL2Pipeline(DefaultPipeline):
    data_urls: List[str] = [
        "{}/covid-19-indicators-uk.csv".format(_gh_base_url),
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Aggregate indicator time series data into relational format
        records = []
        for idx, rows in dataframes[0].groupby(["Date", "Country"]):
            records.append(
                {
                    "date": idx[0],
                    "subregion1_name": idx[1],
                    **{
                        record.loc["Indicator"]: record.loc["Value"]
                        for _, record in rows.iterrows()
                    },
                }
            )

        data = DataFrame.from_records(records).rename(
            columns={"ConfirmedCases": "confirmed", "Deaths": "deceased", "Tests": "tested",}
        )

        for col in ("confirmed", "deceased", "tested"):
            data[col] = data[col].apply(safe_int_cast).astype("Int64")

        data = grouped_diff(data, ["subregion1_name", "date"])
        data.loc[data["subregion1_name"] == "UK", "subregion1_name"] = None
        data["subregion2_code"] = None
        data["country_code"] = "GB"
        return data


class Covid19UkDataL3Pipeline(DefaultPipeline):
    data_urls: List[str] = [
        "{}/covid-19-cases-uk.csv".format(_gh_base_url),
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # County data is already in the format we want
        data = (
            dataframes[0]
            .rename(
                columns={
                    "Date": "date",
                    "Country": "subregion1_name",
                    "AreaCode": "subregion2_code",
                    "TotalCases": "confirmed",
                }
            )
            .drop(columns=["Area"])
            .dropna(subset=["subregion2_code"])
        )

        data["confirmed"] = data["confirmed"].apply(safe_int_cast).astype("Int64")
        data = grouped_diff(data, ["subregion1_name", "subregion2_code", "date"])
        data["country_code"] = "GB"
        return data
