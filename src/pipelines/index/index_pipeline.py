from typing import Any, Dict, List, Tuple
from pandas import DataFrame, Int64Dtype, merge
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from lib.utils import ROOT


class IndexPipeline(DefaultPipeline):
    def fetch(self, **fetch_opts) -> List[str]:
        return [
            ROOT / "src" / "data" / "metadata.csv",
            # ROOT / "src" / "data" / "country_codes.csv",
            ROOT / "src" / "data" / "knowledge_graph.csv",
        ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Outer join auxiliary metadata all provided tables
        data = dataframes[0]
        for df in dataframes[1:]:
            data = merge(data, df, how="left")

        # Country codes are joined by country_code rather than the usual key
        data = merge(
            data,
            aux["country_codes"].rename(columns={"key": "country_code"}),
            on="country_code",
            how="left",
        )

        # Determine the level of aggregation for each datapoint
        data["aggregation_level"] = None
        subregion1_null = data.subregion1_code.isna()
        subregion2_null = data.subregion2_code.isna()
        data.loc[subregion1_null & subregion2_null, "aggregation_level"] = 0
        data.loc[~subregion1_null & subregion2_null, "aggregation_level"] = 1
        data.loc[~subregion1_null & ~subregion2_null, "aggregation_level"] = 2
        return data


class IndexPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "key": str,
        "wikidata": str,
        "datacommons": str,
        "country_code": str,
        "country_name": str,
        "subregion1_code": str,
        "subregion1_name": str,
        "subregion2_code": str,
        "subregion2_name": str,
        "3166-1-alpha-2": str,
        "3166-1-alpha-3": str,
        "aggregation_level": Int64Dtype(),
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [(IndexPipeline(), {})]
