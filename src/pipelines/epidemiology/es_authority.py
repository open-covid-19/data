from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.io import read_file
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff


class ISCIIIPipeline(DefaultPipeline):
    data_urls: List[str] = ["https://covid19.isciii.es/resources/serie_historica_acumulados.csv"]

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # Retrieve the CSV files from https://covid19.isciii.es
        data = (
            read_file(sources[0], error_bad_lines=False, encoding="ISO-8859-1")
            .rename(
                columns={
                    "FECHA": "date",
                    "CCAA": "subregion1_code",
                    "Fallecidos": "deceased",
                    "Hospitalizados": "hospitalized",
                    "UCI": "intensive_care",
                }
            )
            .dropna(subset=["date"])
        )

        # Confirmed cases are split across 2 columns
        confirmed_columns = ["CASOS", "PCR+"]
        for col in confirmed_columns:
            data[col] = data[col].fillna(0)
        data["confirmed"] = data.apply(lambda x: sum([x[col] for col in confirmed_columns]), axis=1)

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda date: datetime_isoformat(date, "%d/%m/%Y"))

        # Keep only the columns we can process
        data = data[
            ["date", "subregion1_code", "confirmed", "deceased", "hospitalized", "intensive_care"]
        ]

        # Reported cases are cumulative, compute the diff
        data = grouped_diff(data, ["subregion1_code", "date"])

        # Add the country code to all records
        data["country_code"] = "ES"

        # Output the results
        return data
