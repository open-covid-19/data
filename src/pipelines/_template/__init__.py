from pandas import DataFrame

from lib.utils import combine_tables, output_table
from lib.default_pipeline import DefaultPipeline

from .pipeline import TemplatePipeline
from .srcname import SourceNamePipeline


def run(aux: DataFrame, **pipeline_opts) -> DataFrame:

    # Define a chain of pipeline-options tuples
    pipeline_chain = [
        # We could use a pipeline as-is
        (SourceNamePipeline(), {}),
        # It's OK to use a different pipeline for each element, or we can reuse the same pipeline
        # but with different options passed to it
        (SourceNamePipeline('arg1'), {'kwarg1': 'value'}),
    ]

    # Get all the pipeline outputs
    pipeline_data = [
        pipeline.run(aux, **{**opts, **pipeline_opts}) for pipeline, opts in pipeline_chain
    ]

    # Combine all pipeline outputs into a single DataFrame, it is highly recommended to use the
    # [lib.utils.combine_tables] function to do this. It will concatenate all data and remove
    # duplicates by keeping the last non-null value from the input tables.
    data = combine_tables(pipeline_data, ['date', 'key'])

    # Return data. It is recommended to use the [lib.utils.output_table] function which
    # automatically filters and casts all the appropriate columns based on the schema provided.
    return output_table(SourceNamePipeline.schema, data)
