from typing import Any, Dict
from pandas import DataFrame, concat

from lib.cast import column_convert
from lib.time import date_offset
from lib.utils import combine_tables
from lib.default_pipeline import DefaultPipeline

from .pipeline import EpidemiologyPipeline
from .es_datadista import DatadistaPipeline
from .xx_ecdc import ECDCPipeline
from .xx_wikipedia import WikipediaPipeline


def run(aux: DataFrame, **pipeline_opts) -> DataFrame:
    wiki_base_url = 'https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data'

    # Define a chain of pipeline-options tuples
    pipeline_chain = [
        (ECDCPipeline(), {}),
        (DatadistaPipeline(), {}),
        (WikipediaPipeline('{}/Argentina_medical_cases'.format(wiki_base_url)),
         {'parse_opts': {'date_format': '%d %b', 'country': 'AR', 'skiprows': 1, 'cumsum': True}}),
        (WikipediaPipeline('{}/Australia_medical_cases'.format(wiki_base_url)),
         {'parse_opts': {'date_format': '%d %B', 'country': 'AU', 'cumsum': True}}),
    ]

    # Get all the pipeline outputs
    # TODO: parallelize this operation
    pipeline_data = [
        pipeline.run(aux, **{**opts, **pipeline_opts}) for pipeline, opts in pipeline_chain
    ]

    # Combine all pipeline outputs into a single DataFrame
    data = combine_tables(pipeline_data, ['date', 'key'])

    # Re-do casting of columns which sometimes is overriden by the combine step
    for column, dtype in EpidemiologyPipeline.output_columns.items():
        data[column] = column_convert(data[column], dtype)

    # Return data sorted based on column order
    return data.sort_values(list(EpidemiologyPipeline.output_columns.keys()))
