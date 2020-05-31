# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataPipeline
from lib.utils import grouped_diff


_column_map = {
    "data": "date",
    "denominazione_regione": "match_string",
    "ricoverati_con_sintomi": "symptomatic_hospitalized",
    "terapia_intensiva": "current_intensive_care",
    "totale_ospedalizzati": "current_hospitalized",
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


class PcmDpcL1Pipeline(DataPipeline):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(columns=_column_map)

        # Parse date into a datetime object
        data["date"] = data["date"].apply(lambda date: datetime.fromisoformat(date).date())

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda date: date.isoformat())

        # Keep only data we can process
        data = data[[col for col in data.columns if col in _column_map.values()]]

        # Compute the daily counts
        data["country_code"] = "IT"
        key_columns = ["country_code", "date"]
        skip_columns = [
            "new_confirmed",
            "total_confirmed",
            "current_intensive_care",
            "current_hospitalized",
        ]
        data = grouped_diff(data, key_columns, skip=skip_columns)

        # Make sure all records have the country code and null region code
        data["subregion1_code"] = None

        # Output the results
        return data


class PcmDpcL2Pipeline(DataPipeline):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(columns=_column_map)

        # Parse date into a datetime object
        data["date"] = data["date"].apply(lambda date: datetime.fromisoformat(date).date())

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda date: date.isoformat())

        # Keep only data we can process
        data = data[[col for col in data.columns if col in _column_map.values()]]

        # Compute the daily counts
        key_columns = ["match_string", "date"]
        skip_columns = [
            "new_confirmed",
            "total_confirmed",
            "current_intensive_care",
            "current_hospitalized",
        ]
        data = grouped_diff(data, key_columns, skip=skip_columns)

        # Make sure all records have the country code
        data["country_code"] = "IT"

        # Output the results
        return data
