from typing import Any, Callable, Dict, List
from pandas import DataFrame

class DataPipeline:
    '''
    Interface for data pipelines. A data pipeline consists of a series of steps performed in the
    following order:
    1. Fetch: download resources into raw data
    2. Parse: convert raw data to structured format
    3. Merge: associate each record with a known `key`
    4. Transform: perform corrections and transformations on the data
    5. Filter: filter out unneeded data and keep only desired output columns
    6. Patch: apply hotfixes to specific data issues
    '''

    output_columns: Dict[str, Any] = None
    ''' Names and corresponding dtypes of output columns '''

    def fetch(self, **fetch_opts) -> List[str]:
        ''' Downloads the required resources and returns a list of local paths. '''
        ...

    def parse(self, sources: List[str], **parse_opts) -> DataFrame:
        ''' Parses a list of raw data records into a DataFrame. '''
        ...

    def merge(self, record: Dict[str, Any], aux: DataFrame, **merge_opts) -> str:
        '''
        Outputs a key used to merge this record with the datasets.
        The key must be present in the `aux` DataFrame index.
        '''
        ...

    def transform(self, data: DataFrame, aux: DataFrame, **transform_opts) -> DataFrame:
        '''
        Performs optional data transformations that make this data more useful, such as adjusting
        the date or applying workarounds to some data columns.
        '''
        ...

    def filter(self, data: DataFrame, filter_func: Callable[[Any], bool], **filter_opts) -> DataFrame:
        ''' Outputs a filtered version of the input dataset respecting `filter_opts`. '''
        ...

    def patch(self, data: DataFrame, patch: DataFrame, **patch_opts) -> DataFrame:
        '''
        Outputs a patched version of the dataframe overwriting records from `data` with `patch`.
        '''
        ...

    def run(
        self,
        aux: DataFrame,
        fetch_opts: Dict[str, Any] = {},
        parse_opts: Dict[str, Any] = {},
        merge_opts: Dict[str, Any] = {},
        transform_opts: Dict[str, Any] = {},
        filter_func: Callable[[Any], bool] = None,
        filter_opts: Dict[str, Any] = {},
        patch: DataFrame = None,
        patch_opts: Dict[str, Any] = {}) -> DataFrame:

        data = self.fetch(**fetch_opts)
        data = self.parse(data, **parse_opts)

        # Merging is done record by record
        data['key'] = data.apply(lambda x: self.merge(x, aux), axis=1)

        # Drop records which have no key merged
        # TODO: log records with missing key
        data = data.dropna(subset=['key'])

        # Transform the data to perform numerical corrections
        data = self.transform(data, aux, **transform_opts)

        # Filter out data, make sure appropriate columns exist and are of right type
        data = self.filter(data, filter_func, **filter_opts)

        # Patch the data only if necessary
        if patch is not None:
            data = self.patch(data, patch, **patch_opts)

        # Return the final dataframe
        return data
