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

from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff


class CovidTrackingPipeline(DataPipeline):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        column_map = {
            "date": "date",
            "state": "subregion1_code",
            "positive": "confirmed",
            "death": "deceased",
            "total": "tested",
            "recovered": "recovered",
            "hospitalizedCurrently": "currently_hospitalized",
            "hospitalizedCumulative": "hospitalized",
            "inIcuCurrently": "currently_intensive_care",
            "inIcuCumulative": "intensive_care",
            "onVentilatorCurrently": "currently_ventilator",
            "onVentilatorCumulative": "ventilator",
        }

        # Rename the appropriate columns
        data = dataframes[0].drop(columns=["hospitalized"]).rename(columns=column_map)

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y%m%d"))

        # Keep only columns we can process
        data["key"] = "US_" + data["subregion1_code"]
        data = data[["key"] + list(column_map.values())].drop(columns=["subregion1_code"])

        # Compute the daily counts
        curr_columns = [col for col in data.columns if "current" in col]
        data = grouped_diff(data, ["key", "date"], skip=curr_columns)

        # Output the results
        return data
