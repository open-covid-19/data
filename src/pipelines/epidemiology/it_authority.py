from datetime import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.utils import grouped_diff


_gh_url_base = "https://raw.github.com/pcm-dpc/COVID-19/master/dati-json"


class PcmDpcL1Pipeline(DefaultPipeline):
    data_urls: List[str] = ["{}/dpc-covid19-ita-andamento-nazionale.json".format(_gh_url_base)]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "data": "date",
                "ricoverati_con_sintomi": "symptomatic_hospitalised",
                "terapia_intensiva": "icu",
                "totale_ospedalizzati": "hospitalised",
                "isolamento_domiciliare": "quarantined",
                "totale_positivi": "active_confirmed",
                "variazione_totale_positivi": "diff_active_confirmed",
                "nuovi_positivi": "confirmed",
                "dimessi_guariti": "recovered",
                "deceduti": "deceased",
                "totale_casi": "total_cases",
                "tamponi": "tested",
                "casi_testati": "cases_tested",
            }
        )

        # Parse date into a datetime object
        data["date"] = data["date"].apply(lambda date: datetime.fromisoformat(date).date())

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda date: date.isoformat())

        # Compute the daily counts
        data["country_code"] = "IT"
        key_columns = ["country_code", "date"]
        cumsum_columns = ["deceased", "tested", "recovered"]
        data[cumsum_columns] = grouped_diff(data[cumsum_columns + key_columns], key_columns)[
            cumsum_columns
        ]

        # Make sure all records have the country code and null region code
        data["subregion1_code"] = None

        # Output the results
        return data


class PcmDpcL2Pipeline(DefaultPipeline):
    data_urls: List[str] = ["{}/dpc-covid19-ita-regioni.json".format(_gh_url_base)]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "data": "date",
                "denominazione_regione": "match_string",
                "ricoverati_con_sintomi": "symptomatic_hospitalised",
                "terapia_intensiva": "intensive_care",
                "totale_ospedalizzati": "hospitalised",
                "isolamento_domiciliare": "quarantined",
                "totale_positivi": "active_confirmed",
                "variazione_totale_positivi": "diff_active_confirmed",
                "nuovi_positivi": "new_confirmed",
                "dimessi_guariti": "recovered",
                "deceduti": "deceased",
                "totale_casi": "total_confirmed",
                "tamponi": "tested",
                "casi_testati": "cases_tested?",
            }
        )

        # Parse date into a datetime object
        data["date"] = data["date"].apply(lambda date: datetime.fromisoformat(date).date())

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda date: date.isoformat())

        # Keep only data we can process
        data = data[
            [
                "date",
                "match_string",
                "intensive_care",
                "hospitalised",
                "quarantined",
                "new_confirmed",
                "recovered",
                "deceased",
                "total_confirmed",
                "tested",
            ]
        ]

        # Compute the daily counts
        key_columns = ["match_string", "date"]
        skip_columns = ['new_confirmed', 'total_confirmed']
        data = grouped_diff(data, key_columns, skip=skip_columns)

        # Make sure all records have the country code
        data["country_code"] = "IT"

        # Output the results
        return data
