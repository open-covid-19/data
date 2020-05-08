from typing import Any, Dict
from pandas import DataFrame, concat

from lib.time import date_offset
from lib.default_pipeline import DefaultPipeline

from pipelines.epidemiology.pipeline import EpidemiologyPipeline
from pipelines.epidemiology.es_datadista import DatadistaPipeline
from pipelines.epidemiology.xx_ecdc import ECDCPipeline
from pipelines.epidemiology.xx_wikipedia import WikipediaPipeline


def run(aux: DataFrame, **pipeline_opts) -> DataFrame:
    wiki_base_url = 'https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data'

    # Define a chain of pipeline-options tuples
    pipeline_chain = [
        (ECDCPipeline(), {}),
        (DatadistaPipeline(), {}),
        (WikipediaPipeline('{}/Argentina_medical_cases'.format(wiki_base_url)), {'parse_opts':
            {'date_format': '%d %b', 'country': 'AR', 'skiprows': 1, 'droprows': 'date'}}),
    ]

    # Get all the pipeline outputs
    # TODO: parallelize this operation
    pipeline_data = [
        pipeline.run(aux, **{**opts, **pipeline_opts}) for pipeline, opts in pipeline_chain
    ]

    # Combine all pipeline outputs into a single DataFrame
    data = concat(pipeline_data)

    # TODO: group keys and combine values
    # data = ...

    # Return data sorted based on column order
    return data.sort_values(list(EpidemiologyPipeline.output_columns.keys()))
