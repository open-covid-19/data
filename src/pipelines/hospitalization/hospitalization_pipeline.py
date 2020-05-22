import re
from typing import Any, Dict, List, Tuple
from pandas import concat, DataFrame, merge, Int64Dtype
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from pipelines.epidemiology.ch_openzh import OpenZHPipeline
from pipelines.epidemiology.it_authority import PcmDpcL1Pipeline, PcmDpcL2Pipeline
from pipelines.epidemiology.jp_2019_ncov_japan import Jp2019NcovJapanByDate
from pipelines.epidemiology.nl_corona_watch_nl import CoronaWatchNlPipeline
from pipelines.epidemiology.si_authority import SloveniaPipeline
from lib.utils import ROOT


class HospitalizationPipeline(DefaultPipeline):
    def fetch(self, cache: Dict[str, str], **fetch_opts) -> List[str]:
        return [
            "https://raw.github.com/google-research/open-covid-19-data/master/data/exported/hospitalizations.csv",
        ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        rename_columns = {"region_code": "key", "region_name": "match_string"}
        for metric in ("hospitalized", "icu", "ventilator"):
            rename_columns[f"{metric}_new"] = f"new_{metric}"
            rename_columns[f"{metric}_cumulative"] = f"total_{metric}"
            rename_columns[f"{metric}_current"] = f"current_{metric}"
        data = dataframes[0].rename(columns=rename_columns)
        data.columns = [col.replace("icu", "intensive_care") for col in data.columns]

        # Add country metadata
        country_codes = aux["country_codes"]
        country_mask = data.key.isin(country_codes["3166-1-alpha-3"])

        # Separate by country and region level, since they use different keys
        country_level = data[country_mask].copy()
        country_level = country_level.rename(columns={"key": "3166-1-alpha-3"}).merge(country_codes)

        # Put the region-level keys in our format
        region_level = data[~country_mask].copy()
        region_level.key = region_level.key.apply(lambda x: x.replace("-", "_"))

        # Make sure we only keep keys that have an exact match, use fuzzy match for the rest
        data = concat([country_level, region_level])
        data.key = data.key.apply(lambda x: x if x in aux["metadata"].key.values else None)

        return data


class HospitalizationPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "date": str,
        "key": str,
        "new_hospitalized": Int64Dtype(),
        "total_hospitalized": Int64Dtype(),
        "current_hospitalized": Int64Dtype(),
        "new_intensive_care": Int64Dtype(),
        "total_intensive_care": Int64Dtype(),
        "current_intensive_care": Int64Dtype(),
        "new_ventilator": Int64Dtype(),
        "total_ventilator": Int64Dtype(),
        "current_ventilator": Int64Dtype(),
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [
        (HospitalizationPipeline(), {}),
        # Data sources for CH level 2
        (OpenZHPipeline(), {}),
        # Data sources for IT level 2
        (PcmDpcL1Pipeline(), {}),
        (PcmDpcL2Pipeline(), {}),
        # Data sources for JP level 2
        (Jp2019NcovJapanByDate(), {}),
        # Data sources for NL levels 2 + 3
        (CoronaWatchNlPipeline(), {}),
        # Data sources for SI level 1
        (SloveniaPipeline(), {}),
    ]
