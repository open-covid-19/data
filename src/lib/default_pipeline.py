import re
import warnings
from typing import Any, Dict, List, Callable

import requests
from pandas import DataFrame, isnull, isna

from .cast import column_convert
from .net import download
from .io import read_file, fuzzy_text
from .data_pipeline import DataPipeline

class DefaultPipeline(DataPipeline):
    '''
    Data pipeline which provides a default implementation for:
    * Fetch: downloads raw data from a list of URLs into ../snapshots folder. See [lib.net].
    * Merge: outputs a key from the auxiliary dataset after performing best-effort matching.
    *
    '''
    data_urls: List[str] = None
    fetch_opts: List[Dict[str, Any]] = None

    def fetch(self, **fetch_opts) -> List[str]:
        num_urls = len(self.data_urls)
        fetch_iter = zip(self.data_urls, self.fetch_opts or [{}] * num_urls)
        return [download(url, **{**opts, **fetch_opts}) for url, opts in fetch_iter]

    def merge(self, record: Dict[str, Any], aux: DataFrame, **merge_opts) -> str:

        # Start by filtering the auxiliary dataset as much as possible
        for column in ('country', 'subregion_1', 'subregion_2'):
            if column not in record:
                continue
            elif isna(record[column]):
                aux = aux[aux[column].isna()]
            elif record[column]:
                aux = aux[aux[column] == record[column]]

        # Auxiliary dataset might have a single record left, then we are done
        if len(aux) == 1:
            return aux.iloc[0]['key']

        # Exact key match might be possible and it's the next fastest option
        if 'key' in record and record['key'] in aux:
            return record['key']

        # Iterate over the auxiliary dataset and attempt to do a regex match of the input string
        if 'match_string' in record:
            match_string = fuzzy_text(record['match_string'])
            for idx, row in aux.iterrows():
                key = row['key']
                if 'match_regex' not in row or isnull(row['match_regex']):
                    warnings.warn('Skipping merge because {} has no `match_regex`'.format(key))
                    continue
                if fuzzy_text(row['match_regex']) == match_string:
                    return key
                if re.match(row['match_regex'], match_string, re.IGNORECASE):
                    return key

        warnings.warn('No key match found for:\n{}'.format(record))
        return None

    def _read(self, file_paths: List[str], **read_opts) -> List[DataFrame]:
        ''' Reads a raw file input path into a DataFrame '''
        return [read_file(file_path, **read_opts) for file_path in file_paths]

    def parse(self, sources: List[str], **parse_opts) -> DataFrame:
        return self.parse_dataframes(self._read(sources), **parse_opts)

    def filter(self, data: DataFrame, filter_func: Callable[[Any], bool], **filter_opts) -> DataFrame:
        assert self.output_columns is not None

        if filter_func is not None:
            data = data[data.apply(filter_func, axis=1)]

        # Make sure all columns are present and have the appropriate type
        for column, dtype in self.output_columns.items():
            if column not in data: data[column] = None
            data[column] = column_convert(data[column], dtype)

        # Filter only the output columns
        return data[self.output_columns.keys()]

    def patch(self, data: DataFrame, patch: DataFrame, **patch_opts) -> DataFrame:
        data = data.copy()
        data[patch.index] = patch
        return data

    def parse_dataframes(self, dataframes: List[DataFrame], **parse_opts) -> DataFrame:
        ''' Parse the inputs into a single output dataframe '''
        raise NotImplementedError()
