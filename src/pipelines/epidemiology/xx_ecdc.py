from typing import Any, Dict, List
from pandas import DataFrame, isnull
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat, date_offset
from lib.utils import get_or_default, grouped_cumsum


class ECDCPipeline(DefaultPipeline):
    data_urls: List[str] = ["https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/"]
    fetch_opts: List[Dict[str, Any]] = [{"ext": "csv"}]

    @staticmethod
    def _adjust_date(data: DataFrame, aux: DataFrame) -> DataFrame:
        """ Adjust the date of the data based on the report offset """

        # Save the current columns to filter others out at the end
        data_columns = data.columns

        # Filter auxiliary dataset to only get the relevant keys
        data = data.merge(aux, suffixes=("", "aux_"), how="left")

        # Perform date adjustment for all records so date is consistent across datasets
        data["date"] = data.apply(
            lambda x: date_offset(x["date"], get_or_default(x, "epi_report_offset", 0)), axis=1,
        )

        return data[data_columns]

    def parse_dataframes(
        self, dataframes: List[DataFrame], metadata: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]
        metadata = metadata["metadata"]

        # Ensure date field is used as a string
        data["dateRep"] = data["dateRep"].astype(str)

        # Convert date to ISO format
        data["date"] = data["dateRep"].apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))

        # Workaround for https://github.com/open-covid-19/data/issues/8
        # ECDC mistakenly labels Greece country code as EL instead of GR
        data["geoId"] = data["geoId"].apply(lambda code: "GR" if code == "EL" else code)

        # Workaround for https://github.com/open-covid-19/data/issues/13
        # ECDC mistakenly labels Greece country code as UK instead of GB
        data["geoId"] = data["geoId"].apply(lambda code: "GB" if code == "UK" else code)

        # Remove bogus entries (cruiseships, etc.)
        data = data[~data["geoId"].apply(lambda code: len(code) > 2)]

        data = data.rename(columns={"geoId": "key", "cases": "confirmed", "deaths": "deceased",})

        # Adjust the date of the records to match local reporting
        data = self._adjust_date(data, metadata)

        # Keep only the columns we can process
        data = data[["date", "key", "confirmed", "deceased"]]

        return grouped_cumsum(data, ["key", "date"])
