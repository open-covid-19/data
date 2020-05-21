from typing import Any, Dict, List, Tuple
from pandas import DataFrame, merge, Int64Dtype
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from lib.utils import ROOT


class GeographyPipeline(DefaultPipeline):
    def fetch(self, cache: Dict[str, str], **fetch_opts) -> List[str]:
        return [
            ROOT / "src" / "data" / "metadata.csv",
            ROOT / "src" / "data" / "wikidata.csv",
        ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Outer join auxiliary metadata with wikidata identifiers
        for df in dataframes[1:]:
            dataframes[0] = merge(dataframes[0], df, how="left")
        return dataframes[0]


class GeographyPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "key": str,
        "latitude": float,
        "longitude": float,
        "elevation": Int64Dtype(),
        "area": Int64Dtype(),
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [(GeographyPipeline(), {})]
