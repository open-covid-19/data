from typing import Any, Dict, List, Tuple
from pandas import DataFrame, merge
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from lib.utils import ROOT


class MetadataPipeline(DefaultPipeline):
    def fetch(self, **fetch_opts) -> List[str]:
        return [
            ROOT / "src" / "data" / "auxiliary.csv",
            ROOT / "src" / "data" / "wikidata.csv",
        ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: List[DataFrame], **parse_opts
    ) -> DataFrame:
        # Outer join auxiliary metadata with wikidata identifiers
        for df in dataframes[1:]:
            dataframes[0] = merge(dataframes[0], df, how='left')
        return dataframes[0]


class MetadataPipelineChain(PipelineChain):

    schema: Dict[str, Any] = {
        "key": str,
        "wikidata": str,
        "country_name": str,
        "country_code": str,
        "subregion1_name": str,
        "subregion1_code": str,
        "subregion2_name": str,
        "subregion2_code": str,
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [(MetadataPipeline(), {})]
