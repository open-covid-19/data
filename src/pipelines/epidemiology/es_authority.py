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
        df = (
            read_file(sources[0], error_bad_lines=False, encoding="ISO-8859-1")
            .rename(
                columns={
                    "FECHA": "date",
                    "CCAA": "subregion1_code",
                    "Fallecidos": "deceased",
                    "Hospitalizados": "hospitalised",
                    "UCI": "ICU",
                    "Recuperados": "recovered",
                }
            )
            .dropna(subset=["date"])
        )

        # Confirmed cases are split across 2 columns
        confirmed_columns = ["CASOS", "PCR+"]
        for col in confirmed_columns:
            df[col] = df[col].fillna(0)
        df["confirmed"] = df.apply(lambda x: sum([x[col] for col in confirmed_columns]), axis=1)

        # Convert dates to ISO format
        df["date"] = df["date"].apply(lambda date: datetime_isoformat(date, "%d/%m/%Y"))

        # Reported cases are cumulative, compute the diff
        df = grouped_diff(df, ["subregion1_code", "date"])

        # Add the country code to all records
        df["country_code"] = "ES"

        # Country-wide is the sum of all regions
        country_level = (
            df.drop(columns=["subregion1_code"])
            .groupby(["date", "country_code"])
            .sum()
            .reset_index()
        )
        country_level["subregion1_code"] = None
        df = concat([country_level, df])

        # Output the results
        return df
