from pandas import DataFrame

from lib.utils import combine_tables, output_table
from lib.default_pipeline import DefaultPipeline

from .pipeline import CovariatesPipeline
from .google_mobility import GoogleMobilityPipeline

def run(aux: DataFrame, **pipeline_opts) -> DataFrame:
    # Define a chain of pipeline-options tuples
    pipeline_chain = [
        # Data sources for all level 1
        (GoogleMobilityPipeline(), {}),
    ]

    # Get all the pipeline outputs
    # TODO: parallelize this operation (but keep ordering)
    pipeline_data = [
        pipeline.run(aux, **{**opts, **pipeline_opts}) for pipeline, opts in pipeline_chain
    ]

    # Combine all pipeline outputs into a single DataFrame
    data = combine_tables(pipeline_data, ['date', 'key'])

    # Return data using the pipeline's output parameters
    return output_table(CovariatesPipeline.schema, data)
