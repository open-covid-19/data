from typing import Any, Dict, List, Tuple
from pandas import DataFrame, merge, Int64Dtype
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from pipelines._common.wikidata_pipeline import WikidataPipeline


class GeographyPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "key": str,
        "latitude": float,
        "longitude": float,
        "elevation": Int64Dtype(),
        "area": Int64Dtype(),
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [
        (
            WikidataPipeline(),
            {
                "parse_opts": {
                    "properties": {
                        "latitude": "P625",
                        "longitude": "P625",
                        "elevation": "P2044",
                        "area": "P2046",
                    }
                }
            },
        )
    ]
