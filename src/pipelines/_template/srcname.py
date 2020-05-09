from typing import Any, Dict, List
from pandas import DataFrame
from lib.time import datetime_isoformat
from .pipeline import TemplatePipeline


class SourceNamePipeline(TemplatePipeline):
    ''' This is a custom pipeline that downloads a CSV file and outputs it '''

    data_urls: List[str] = ['https://example.com/data.csv']
    ''' Define our data URLs, which can be more than one '''

    fetch_opts: List[Dict[str, Any]] = None
    ''' Leave the fetch options as the default, see [lib.net.download] for more details '''

    def parse_dataframes(self, dataframes: List[DataFrame], **parse_opts):
        '''
        If the data fetched is a supported format, like CSV or JSON, it will be automatically parsed
        and passed as an argument to this function. For data that requires special parsing, override
        the [DefaultPipeline.parse] function instead.
        '''

        # We only fetched one source, so the list will be of length one
        data = dataframes[0]

        # Here we can manipulate the data any way we want...

        # The default merge strategy uses the following strategy to merge:
        # 1. If *all* code / name for country, subregion_1 and subregion_2 match
        # 2. If `string_match` column is the same as code / name for subregion_1 or subregion_2
        # 3. If `string_match` matches the `string_regex` using a case-insensitive regex match
        # data['string_match'] = ...

        # Finally, return the data which is ready to go to the next step of the pipeline
        return data
