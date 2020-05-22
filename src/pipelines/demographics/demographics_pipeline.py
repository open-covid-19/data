from typing import Any, Dict, List, Tuple
from pandas import DataFrame, merge, Int64Dtype
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from pipelines._common.wikidata_pipeline import WikidataPipeline


class DemographicsPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "key": str,
        "population": Int64Dtype(),
        "life_expectancy": float,
        "human_development_index": float,
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [
        (
            WikidataPipeline(),
            {
                "parse_opts": {
                    "properties": {
                        "population": "P1082",
                        "life_expectancy": "P2250",
                        "human_development_index": "P1081",
                    }
                }
            },
        )
    ]
