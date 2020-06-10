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

import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_cumsum


class FrancePipeline(DataPipeline):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "jour": "date",
                "dep": "subregion2_code",
                "p": "confirmed",
                "t": "tested",
                "incid_hosp": "hospitalized",
                "incid_dc": "deceased",
                "incid_rad": "recovered",
            }
        )

        # Add subregion1_code field to all records
        data["subregion1_code"] = ""

        # Adjust for special regions
        region_adjust_map = {"971": "GUA", "972": "MQ", "973": "GF", "974": "LRE", "976": "MAY"}
        for subregion2_code, subregion1_code in region_adjust_map.items():
            mask = data.subregion2_code == subregion2_code
            data.loc[mask, "subregion2_code"] = None
            data.loc[mask, "subregion1_code"] = subregion1_code

        # Get date in ISO format
        data.date = data.date.astype(str)

        # Get keys from metadata auxiliary table
        data["country_code"] = "FR"
        subregion1_mask = data.subregion2_code.isna()
        data1 = data[subregion1_mask].merge(
            aux["metadata"], on=("subregion1_code", "subregion2_code")
        )
        data2 = data[~subregion1_mask].merge(aux["metadata"], on="subregion2_code")
        data = concat([data1, data2])

        # We only need to keep key-date pair for identification
        keep_columns = ["date", "key", "confirmed", "tested", "deceased", "hospitalized"]
        data = data[[col for col in data.columns if col in keep_columns]]

        # Compute the daily counts
        data = grouped_cumsum(data, ["key", "date"])

        # Group by level 2 region, and add the parts
        l2 = data.copy()
        l2["key"] = l2.key.apply(lambda x: "_".join(x.split("_")[:2]))
        l2 = l2.groupby(["key", "date"]).sum().reset_index()

        # Group by country level, and add the parts
        l1 = l2.copy().drop(columns=["key"])
        l1 = l1.groupby("date").sum().reset_index()
        l1["key"] = "FR"

        # Output the results
        return concat([l2, data])
