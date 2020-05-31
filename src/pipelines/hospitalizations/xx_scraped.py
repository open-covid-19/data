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
from lib.io import read_file
from lib.pipeline import DataPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff, grouped_cumsum, pivot_table


class ScrapedPipeline(DataPipeline):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ):

        data = dataframes[0].rename(
            columns={
                "discharged_cumulative": "total_discharged",
                "hospitalized_current": "current_hospitalized",
                "hospitalized_cumulative": "total_hospitalized",
                "icu_current": "current_intensive_care",
                "icu_cumulative": "cumulative_intensive_care",
                "ventilator_current": "current_ventilator",
                "ventilator_cumulative": "cumulative_ventilator",
            }
        )

        # Add key and parse date in ISO format
        data["key"] = parse_opts["key"]
        data.date = data.date.astype(str)

        # Determine if we need to compute daily counts
        cum_cols = [col for col in data.columns if "cumulative" in col]
        if cum_cols:
            skip_cols = [col for col in data.columns if col not in cum_cols]
            data = grouped_diff(data, ["key", "date"], skip=skip_cols)

        return data
