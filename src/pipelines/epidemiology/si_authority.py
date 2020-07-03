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

from typing import Dict, List
from pandas import DataFrame
from lib.data_source import DataSource
from lib.cast import safe_int_cast


class SloveniaDataSource(DataSource):
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
                "All persons in intensive care on certain day": "current_intensive_care",
                "Discharged": "new_recovered",
                "Deaths (all)": "total_deceased",
                "Deaths (daily)": "new_deceased",
            }
        )

        # It's only country-level data so we can compute the key directly
        data["key"] = "SI"

        # Make sure that the date column is a string
        data.date = data.date.astype(str)

        # Remove markers from data fields
        value_columns = [
            col
            for col in data.columns
            if any(col.startswith(token) for token in ("new", "total", "current"))
        ]
        for col in value_columns:
            data[col] = data[col].apply(lambda x: safe_int_cast(str(x).replace("*", "")))

        # Output the results
        return data
