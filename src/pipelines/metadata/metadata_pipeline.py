from typing import Any, Dict, List, Tuple
from pandas import DataFrame
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from lib.utils import ROOT


class MetadataPipeline(DefaultPipeline):

    def fetch(self, **fetch_opts) -> List[str]:
        return [ROOT / 'src' / 'data' / 'auxiliary.csv']

    def parse_dataframes(
            self, dataframes: List[DataFrame], aux: List[DataFrame], **parse_opts) -> DataFrame:
        # Simply output the auxiliary CSV file as-is
        return dataframes[0]


class MetadataPipelineChain(PipelineChain):

    schema: Dict[str, Any] = {
        'key': str,
        'dcid': str,
        'country_name': str,
        'country_code': str,
        'subregion1_name': str,
        'subregion1_code': str,
        'subregion2_name': str,
        'subregion2_code': str,
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [(MetadataPipeline(), {})]
