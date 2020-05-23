from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff


class OpenZHPipeline(DefaultPipeline):
    data_urls: List[str] = [
        "https://raw.github.com/openZH/covid_19/master/COVID19_Fallzahlen_CH_total.csv"
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = (
            dataframes[0]
            .rename(
                columns={
                    "ncumul_tested": "tested",
                    "ncumul_conf": "confirmed",
                    "ncumul_deceased": "deceased",
                    "ncumul_hosp": "hospitalized",
                    "ncumul_ICU": "intensive_care",
                    "ncumul_vent": "ventilator",
                    "ncumul_released": "recovered",
                    "abbreviation_canton_and_fl": "subregion1_code",
                }
            )
            .drop(columns=["time", "source"])
        )

        # TODO: Match FL subdivision (not a canton?)
        data = data[data.subregion1_code != "FL"]
        
        data = grouped_diff(data, ["subregion1_code", "date"])
        data["country_code"] = "CH"
        return data
