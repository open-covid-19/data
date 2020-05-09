from pandas import DataFrame

from lib.utils import combine_tables, output_table
from lib.default_pipeline import DefaultPipeline

from .pipeline import EpidemiologyPipeline
from .es_datadista import DatadistaPipeline
from .fr_france_covid_19 import FranceCovid19Pipeline
from .xx_ecdc import ECDCPipeline
from .xx_wikipedia import WikipediaPipeline


def run(aux: DataFrame, **pipeline_opts) -> DataFrame:
    wiki_base_url = 'https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data'

    # Define a chain of pipeline-options tuples
    pipeline_chain = [
        # Data sources for all level 1
        (ECDCPipeline(), {}),

        # Data sources for ES levels 1 and 2
        (DatadistaPipeline(), {}),

        # Data sources for AR level 2
        (WikipediaPipeline('{}/Argentina_medical_cases'.format(wiki_base_url)),
         {'parse_opts': {'date_format': '%d %b', 'country': 'AR', 'skiprows': 1, 'cumsum': True}}),

        # Data sources for AU level 2
        (WikipediaPipeline('{}/Australia_medical_cases'.format(wiki_base_url)),
         {'parse_opts': {'date_format': '%d %B', 'country': 'AU', 'cumsum': True}}),

        # Data sources for FR level 2
        (WikipediaPipeline('{}/France_medical_cases'.format(wiki_base_url)),
         {'parse_opts': {'date_format': '%Y-%m-%d', 'country': 'FR', 'skiprows': 1}}),
        (FranceCovid19Pipeline(), {}),
    ]

    # Get all the pipeline outputs
    # TODO: parallelize this operation (but keep ordering)
    pipeline_data = [
        pipeline.run(aux, **{**opts, **pipeline_opts}) for pipeline, opts in pipeline_chain
    ]

    # Combine all pipeline outputs into a single DataFrame
    data = combine_tables(pipeline_data, ['date', 'key'])

    # Return data using the pipeline's output parameters
    return output_table(EpidemiologyPipeline.schema, data)
