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
from lib.cast import safe_datetime_parse
from lib.pipeline import DataPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_cumsum


class ColombiaPipeline(DataPipeline):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        data = dataframes[0].rename(
            columns={
                "Codigo DIVIPOLA": "subregion2_code",
                "Fecha de muerte": "date_deceased",
                "Fecha diagnostico": "date_confirmed",
                "Fecha recuperado": "date_recovered",
            }
        )

        # Clean up the subregion code
        data.subregion2_code = data.subregion2_code.apply(lambda x: "{0:05d}".format(int(x)))

        # Compute the key from the DIVIPOLA code
        data["key"] = (
            "CO_" + data.subregion2_code.apply(lambda x: x[:2]) + "_" + data.subregion2_code
        )

        # A few cases are at the l2 level
        data.key = data.key.apply(lambda x: "CO_" + x[-2:] if x.startswith("CO_00_") else x)

        # Go from individual case records to key-grouped records in a flat table
        merged: DataFrame = None
        for value_column in ("confirmed", "deceased", "recovered"):
            subset = data.rename(columns={"date_{}".format(value_column): "date"})[["key", "date"]]
            subset = subset[~subset.date.isna() & (subset.date != "-   -")].dropna()
            subset[value_column] = 1
            subset = subset.groupby(["key", "date"]).sum().reset_index()
            if merged is None:
                merged = subset
            else:
                merged = merged.merge(subset, how="outer")

        # Convert date to ISO format
        merged.date = merged.date.apply(safe_datetime_parse)
        merged = merged[~merged.date.isna()]
        merged.date = merged.date.apply(lambda x: x.date().isoformat())
        merged = merged.fillna(0)

        # Compute the daily counts
        data = grouped_cumsum(merged, ["key", "date"])

        # Group by level 2 region, and add the parts
        l2 = data.copy()
        l2["key"] = l2.key.apply(lambda x: "_".join(x.split("_")[:2]))
        l2 = l2.groupby(["key", "date"]).sum().reset_index()

        # Group by country level, and add the parts
        l1 = l2.copy().drop(columns=["key"])
        l1 = l1.groupby("date").sum().reset_index()
        l1["key"] = "CO"

        return data
