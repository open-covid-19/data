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


class SwitzerlandPipeline(DataPipeline):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = (
            dataframes[0]
            .rename(
                columns={
                    "ncumul_tested": "tested",
                    "ncumul_conf": "confirmed",
                    "ncumul_deceased": "deceased",
                    "ncumul_hosp": "hospitalized",
                    "ncumul_ICU": "intensive_care",
                    "ncumul_vent": "ventilator",
                    "ncumul_released": "recovered",
                    "abbreviation_canton_and_fl": "subregion1_code",
                }
            )
            .drop(columns=["time", "source"])
        )

        # TODO: Match FL subdivision (not a canton?)
        data = data[data.subregion1_code != "FL"]

        # Data provided is cumulative, compute the diffs
        data = grouped_diff(data, ["subregion1_code", "date"])
        data["country_code"] = "CH"

        # Country-level data is reported as "ZH"
        country_mask = data.subregion1_code == "ZH"
        data.loc[country_mask, "subregion1_code"] = None

        return data
