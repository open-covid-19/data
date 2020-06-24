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
from pandas import DataFrame, Series, concat, merge
from lib.cast import safe_int_cast
from lib.pipeline import DataSource


class NytCovidL2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "date": "date",
                "state": "subregion1_name",
                "cases": "total_confirmed",
                "deaths": "total_deceased",
            }
        )

        # Add state code to the data
        us_meta = aux["metadata"]
        us_meta = us_meta[us_meta["country_code"] == "US"]
        us_meta = us_meta[us_meta["subregion2_code"].isna()]
        state_map = {
            idx: code
            for idx, code in us_meta.set_index("subregion1_name")["subregion1_code"].iteritems()
        }
        data["subregion1_code"] = data["subregion1_name"].apply(lambda x: state_map[x])

        # Manually build the key rather than doing automated merge for performance reasons
        data["key"] = "US_" + data["subregion1_code"]

        # Now that we have the key, we don't need any other non-value columns
        data = data[["date", "key", "total_confirmed", "total_deceased"]]
        return data


def _distribute_cases(
    data: DataFrame, populations: DataFrame, county_mask: Series, codes: List[str]
):
    """
    This function is used to distribute cases from data across [codes] proportional to their
    population relative to each other.

    @return the data after confirmed and deceased counts have been distributed across [codes]
    """
    state_code = data[county_mask].iloc[0]["subregion1_code"]
    populations = {fips: populations.loc[f"US_{state_code}_{fips}", "population"] for fips in codes}
    populations = {fips: val / sum(populations.values()) for fips, val in populations.items()}
    county_records = []
    for _, record in data[county_mask].iterrows():
        county_records += [
            {
                "date": record["date"],
                "subregion2_code": fips,
                "subregion1_code": record["subregion1_code"],
                "total_confirmed": record["total_confirmed"] * relative_population,
                "total_deceased": record["total_deceased"] * relative_population,
            }
            for fips, relative_population in populations.items()
        ]
    return concat([data[~county_mask], DataFrame.from_records(county_records)])


class NytCovidL3DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "date": "date",
                "state": "subregion1_name",
                "fips": "subregion2_code",
                "cases": "total_confirmed",
                "deaths": "total_deceased",
            }
        )

        # Add state code to the data
        us_states = aux["metadata"]
        us_states = us_states[us_states["country_code"] == "US"]
        us_states = us_states[us_states.subregion2_code.isna()]
        data = data.merge(us_states[["subregion1_code", "subregion1_name"]], how="left")

        # NYC has 5 boroughs which are all being reported as the same county with a null FIPS value.
        # To ensure that the sum of all counties is closer to the total numbers state-wide, we
        # divide the cases and fatalities across all boroughs in a manner directly proportional to
        # their population to "redistribute" the case counts.
        populations = aux["demographics"].set_index("key")
        nyc_counties = [
            "36005",  # Bronx
            "36047",  # Kings AKA Brooklyn
            "36061",  # New York County AKA Manhattan
            "36081",  # Queens
            "36085",  # Richmond AKA Staten Island
        ]
        nyc_data_mask = (data.subregion1_code == "NY") & (data.county == "New York City")
        data = _distribute_cases(data, populations, nyc_data_mask, nyc_counties)

        # Do the same for Kansas City and its 4 counties
        kc_counties = [
            "29037",  # Cass County
            "29047",  # Clay County
            "29095",  # Jackson County
            "29165",  # Platte County
        ]
        kc_data_mask = (data.subregion1_code == "MO") & (data.county == "Kansas City")
        data = _distribute_cases(data, populations, kc_data_mask, kc_counties)

        # Remove "unknown" counties
        data = data[data.county != "Unknown"]

        # Give records with null FIPS code a bogus code so they are reported as mismatches
        # There should be none of these, but this will let us catch mismatches if the data changes
        data.loc[data.subregion2_code.isna(), "subregion2_code"] = 0

        # Make sure the FIPS code is well-formatted
        data["subregion2_code"] = data["subregion2_code"].apply(lambda x: "{0:05d}".format(int(x)))

        # Manually build the key rather than doing automated merge for performance reasons
        data["key"] = "US_" + data["subregion1_code"] + "_" + data["subregion2_code"]
        data = data[["date", "key", "total_confirmed", "total_deceased"]]
        return data
