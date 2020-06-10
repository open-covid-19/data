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
import numpy
from pandas import DataFrame, concat, merge
from lib.io import read_file
from lib.pipeline import DataPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_cumsum


class TexasPipeline(DataPipeline):
    @staticmethod
    def _parse_trends(data: DataFrame) -> DataFrame:
        data.columns = data.iloc[1]
        data.columns = [col.replace("\n", " ") for col in data.columns]
        data = data.replace(".", numpy.nan)
        return data.iloc[2:].rename(
            columns={
                "Date": "date",
                "Cumulative Cases": "total_confirmed",
                "Cumulative Fatalities": "total_deceased",
                "Daily New Cases": "new_confirmed",
                "Daily New Fatalities": "new_deceased",
            }
        )

    @staticmethod
    def _parse_tests(data: DataFrame) -> DataFrame:
        data.columns = data.iloc[0]
        data.columns = [col.replace("\n", " ") for col in data.columns]
        data = data.iloc[1:].rename(
            columns={
                "Date": "date",
                "Viral Tests": "total_tested",
                "Antibody Tests": "total_tested_antibody",
                "Total Tests reported": "total_tested_all",
            }
        )
        data = data.replace(".", numpy.nan)
        data.total_tested.fillna(data.total_tested_all, inplace=True)
        return data

    @staticmethod
    def _parse_hospitalized(data: DataFrame) -> DataFrame:
        data.columns = data.iloc[0]
        data.columns = [col.replace("\n", " ") for col in data.columns]
        data = data.iloc[1:].rename(
            columns={"Date": "date", "Hospitalization by Day": "current_hospitalized"}
        )
        data = data.replace(".", numpy.nan)
        return data

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        data = TexasPipeline._parse_trends(read_file(sources[0], sheet_name="Trends"))
        data = data.merge(
            TexasPipeline._parse_tests(read_file(sources[0], sheet_name="Tests by day"))
        )
        for col in data.columns:
            if col != "date":
                data[col] = data[col].astype(float)

        data.date = data.date.apply(lambda x: x.date().isoformat())
        data["key"] = "US_TX"

        return data
