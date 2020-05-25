from typing import Any, Dict, List, Tuple
from pandas import DataFrame, merge, Int64Dtype
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from pipelines.mobility.apple_mobility_pipeline import AppleMobilityPipeline
from pipelines.mobility.google_mobility_pipeline import GoogleMobilityPipeline


class MobilityPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "date": str,
        "key": str,
        "mobility_driving": float,
        "mobility_transit": float,
        "mobility_walking": float,
        "mobility_retail_and_recreation": float,
        "mobility_grocery_and_pharmacy": float,
        "mobility_parks": float,
        "mobility_transit_stations": float,
        "mobility_workplaces": float,
        "mobility_residential": float,
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [
        (AppleMobilityPipeline(), {}),
        (GoogleMobilityPipeline(), {}),
    ]
