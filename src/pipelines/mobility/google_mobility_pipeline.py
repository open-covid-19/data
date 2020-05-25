import re
from typing import Any, Dict, List, Tuple
from pandas import DataFrame, Int64Dtype, merge, isna
from lib.cast import safe_int_cast
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from lib.time import datetime_isoformat
from lib.utils import ROOT


class GoogleMobilityPipeline(DefaultPipeline):
    data_urls: List[str] = ["https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"]

    @staticmethod
    def process_record(record: Dict):
        subregion1 = record["subregion1_name"]
        subregion2 = record["subregion2_name"]

        # Early exit: country-level data
        if isna(subregion1):
            return None

        if isna(subregion2):
            match_string = subregion1
        else:
            match_string = subregion2

        match_string = re.sub(r"\(.+\)", "", match_string).split("/")[0]
        for token in (
            "Province",
            "Prefecture",
            "State of",
            "Canton of",
            "Autonomous",
            "Voivodeship",
            "District",
        ):
            match_string = match_string.replace(token, "")

        # Workaround for "Blekinge County"
        if record["country_code"] != "US":
            match_string = match_string.replace("County", "")

        return match_string.strip()

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = data = dataframes[0].rename(
            columns={
                "country_region_code": "country_code",
                "sub_region_1": "subregion1_name",
                "sub_region_2": "subregion2_name",
            }
        )

        data.columns = [
            f"mobility_{re.sub('_percent.+', '', col)}" if "percent" in col else col
            for col in data.columns
        ]

        # We can derive the key directly from country code for country-level data
        data["key"] = None
        country_level_mask = data.subregion1_name.isna() & data.subregion2_name.isna()
        data.loc[country_level_mask, "key"] = data.loc[country_level_mask, "country_code"]

        # Drop intra-country records for which we don't have regional data
        meta = aux["metadata"]
        regional_data_countries = meta.loc[~meta.subregion1_code.isna(), "country_code"].unique()
        data = data[~data.key.isna() | data.country_code.isin(regional_data_countries)]

        # Clean up known issues with subregion names
        data["match_string"] = data.apply(GoogleMobilityPipeline.process_record, axis=1)
        usa_mask = data.country_code == "US"
        data.loc[~usa_mask & ~data.subregion1_name.isna(), "subregion1_name"] = ""
        data.loc[~data.subregion2_name.isna(), "subregion2_name"] = ""

        # GB data is actually county-level even if reported as subregion1
        data.loc[data.country_code == "GB", "subregion2_name"] = ""

        return data
