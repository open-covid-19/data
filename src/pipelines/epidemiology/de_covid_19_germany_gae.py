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
from typing import Dict
from numpy import unique
from pandas import DataFrame
from lib.data_source import DataSource


class Covid19GermanyDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(columns={"time_iso8601": "date"})

        # Convert dates to ISO format
        data["date"] = data["date"].apply(
            lambda x: datetime.datetime.fromisoformat(x).date().isoformat()
        )

        # Get a list of all regions
        regions = unique([col[3:5] for col in data.columns if col.startswith("DE-")])

        # Transform the data from non-tabulated format to our record format
        records = []
        for idx, row in data.iterrows():
            record = {"date": row["date"]}
            for region_code in regions:
                records.append(
                    {
                        "subregion1_code": region_code,
                        "total_confirmed": row["DE-%s_cases" % region_code],
                        "total_deceased": row["DE-%s_deaths" % region_code],
                        **record,
                    }
                )
        data = DataFrame.from_records(records)

        # Ensure we only take one record from the table
        data = data.groupby(["date", "subregion1_code"]).last().reset_index()

        # Output the results
        data["country_code"] = "DE"
        return data
