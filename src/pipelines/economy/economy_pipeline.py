from typing import Any, Dict, List, Tuple
from pandas import DataFrame, merge, Int64Dtype
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from pipelines._common.wikidata_pipeline import WikidataPipeline


class EconomyPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "key": str,
        "gdp": Int64Dtype(),
        "gdp_per_capita": Int64Dtype(),
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [
        (
            WikidataPipeline(),
            {"parse_opts": {"properties": {"gdp": "P2131", "gdp_per_capita": "P2132",}}},
        )
    ]
