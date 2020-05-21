from typing import Any, Dict, List
from pandas import DataFrame, isnull
from lib.cast import safe_int_cast
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat, date_offset
from lib.utils import get_or_default


class OurWorldInDataPipeline(DefaultPipeline):
    data_urls: List[str] = ["https://covid.ourworldindata.org/data/owid-covid-data.csv"]

    @staticmethod
    def _adjust_date(data: DataFrame, metadata: DataFrame) -> DataFrame:
        """ Adjust the date of the data based on the report offset """

        # Save the current columns to filter others out at the end
        data_columns = data.columns

        # Filter auxiliary dataset to only get the relevant keys
        data = data.merge(metadata, suffixes=("", "aux_"), how="left")

        # Perform date adjustment for all records so date is consistent across datasets
        data.aggregate_report_offset = data.aggregate_report_offset.apply(safe_int_cast)
        data["date"] = data.apply(
            lambda x: date_offset(x["date"], get_or_default(x, "aggregate_report_offset", 0)),
            axis=1,
        )

        return data[data_columns]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = (
            dataframes[0]
            .rename(
                columns={
                    "iso_code": "3166-1-alpha-3",
                    "new_cases": "new_confirmed",
                    "new_deaths": "new_deceased",
                    "new_tests": "new_tested",
                    "total_cases": "total_confirmed",
                    "total_deaths": "total_deceased",
                    "total_tests": "total_tested",
                }
            )
            .merge(aux["country_codes"])
        )

        # Adjust the date of the records to match local reporting
        data = self._adjust_date(data, aux["metadata"])

        return data
