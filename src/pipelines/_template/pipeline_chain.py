from typing import Any, Dict, List, Tuple
from lib.pipeline import DataPipeline, PipelineChain
from .srcname_pipeline import SourceNamePipeline


class TemplatePipelineChain(PipelineChain):
    """
    Pipeline chain for `template` data, which will output a table with the schema described below.
    For very simple pipelines (e.g. single source) this class can be placed in the same file as the
    one defining the pipeline. See [MetadataPipelineChain] for an example of a very simple pipeline.
    """

    schema: Dict[str, Any] = {
        "date": str,
        "key": str,
        "column1": "Int64",
        "column2": float,
    }
    """ Defines the schema of the output table """

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [
        (
            SourceNamePipeline(),
            {"parse_opts": ..., "merge_opts": ..., "filter_func": ...},
        )
    ]
    """ Defines the pipelines to be run in order to produce the combined, full output """
