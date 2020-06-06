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
from lib.utils import grouped_diff, grouped_cumsum


class LuxembourgPipeline(DataPipeline):
    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        data = read_file(sources[0], error_bad_lines=False, encoding="ISO-8859-1").rename(
            columns={
                "Date": "date",
                "Nombre de personnes en soins normaux": "current_hospitalized",
                "Nombre de personnes en soins intensifs (sans patients du Grand Est)": "current_intensive_care",
                "Nombre de décès - cumulé (sans patients du Grand Est)": "deceased",
                "Total patients COVID ayant quitté l'hôpital (hospitalisations stationnaires, données brutes)": "recovered",
                "Nombre de nouvelles personnes testées COVID+ par jour ": "tested",
            }
        )

        # Get date in ISO format
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))

        # Keep only columns we can provess
        data = data[
            [
                "date",
                "current_hospitalized",
                "current_intensive_care",
                "deceased",
                "recovered",
                "tested",
            ]
        ]

        # Convert recovered into a number
        data.recovered = data.recovered.apply(lambda x: int(x.replace("-", "0")))

        # Compute the daily counts
        data["key"] = "LU"
        data_new = grouped_diff(data[["key", "date", "deceased"]], ["key", "date"])
        data_cum = grouped_cumsum(data[["key", "date", "tested", "recovered"]], ["key", "date"])
        data_cur = data[["key", "date", "current_hospitalized", "current_intensive_care"]]
        data = data_new.merge(data_cum, how="outer").merge(data_cur, how="outer")

        # Output the results
        return data
