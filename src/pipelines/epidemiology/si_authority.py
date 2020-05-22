from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_cumsum


class SloveniaPipeline(DefaultPipeline):
    data_urls: List[str] = [
        "https://www.gov.si/assets/vlada/Koronavirus-podatki/en/EN_Covid-19-all-data.xlsx"
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "Date": "date",
                "Tested (all)": "total_tested",
                "Tested (daily)": "new_tested",
                "Positive (all)": "total_confirmed",
                "Positive (daily)": "new_confirmed",
                "All hospitalized on certain day": "current_hospitalized",
                "All persons in intensive care on certain day": "active_intensive_care",
                "Discharged": "recovered",
                "Deaths (all)": "total_deceased",
                "Deaths (daily)": "new_deceased",
            }
        )

        # Make sure all records have the country code
        data["country_code"] = "SI"

        # Make sure that the date column is a string
        data.date = data.date.astype(str)

        # Compute the cumsum counts
        data = grouped_cumsum(
            data,
            ["country_code", "date"],
            skip=[
                col
                for col in data.columns
                if any(kword in col for kword in ("new", "total", "active"))
            ],
        )

        # Output the results
        return data
