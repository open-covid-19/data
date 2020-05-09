from pandas import DataFrame

from lib.utils import combine_tables, output_table
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

    # Return data using the pipeline's output parameters
    return output_table(EpidemiologyPipeline.schema, data)
