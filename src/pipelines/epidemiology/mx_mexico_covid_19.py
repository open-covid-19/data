import datetime
from typing import Any, Dict, List
from numpy import unique
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff, pivot_table


class MexicoCovid19Pipeline(DefaultPipeline):
    data_urls: List[str] = [
        "https://raw.githubusercontent.com/carranco-sga/Mexico-COVID-19/master/Mexico_COVID19_CTD.csv"
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(columns={"Fecha": "date"}).set_index("date")

        deceased_columns = [col for col in data.columns if col.endswith("_D")]
        confirmed_columns = [col[:-2] for col in deceased_columns]

        deceased = data[deceased_columns]
        confirmed = data[confirmed_columns]
        deceased.columns = confirmed.columns

        deceased = pivot_table(deceased, pivot_name="subregion1_code").rename(
            columns={"value": "deceased"}
        )
        confirmed = pivot_table(confirmed, pivot_name="subregion1_code").rename(
            columns={"value": "confirmed"}
        )

        data = confirmed.merge(deceased).sort_values(["date", "subregion1_code"])

        # Output the results
        data = grouped_diff(data, ["subregion1_code", "date"])
        data["country_code"] = "MX"
        return data
