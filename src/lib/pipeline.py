import re
import sys
import warnings
import traceback
import subprocess
from io import StringIO
from pathlib import Path
from functools import partial
from multiprocessing import cpu_count
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import requests
from pandas import DataFrame, isnull, isna, read_csv
from tqdm import tqdm

from .anomaly import detect_anomaly_all, detect_stale_columns
from .cast import column_convert
from .concurrent import process_map
from .net import download
from .io import read_file, fuzzy_text
from .utils import ROOT, CACHE_URL, column_convert, combine_tables


class DataPipeline:
    """
    Interface for data pipelines. A data pipeline consists of a series of steps performed in the
    following order:
    1. Fetch: download resources into raw data
    1. Parse: convert raw data to structured format
    1. Merge: associate each record with a known `key`
    1. Filter: filter out unneeded data and keep only desired output columns
    """

    def fetch(self, cache: Dict[str, str], **fetch_opts) -> List[str]:
        """ Downloads the required resources and returns a list of local paths. """
        ...

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        """ Parses a list of raw data records into a DataFrame. """
        ...

    def merge(
        self, record: Dict[str, Any], aux: Dict[str, DataFrame], **merge_opts
    ) -> Optional[str]:
        """
        Outputs a key used to merge this record with the datasets.
        The key must be present in the `aux` DataFrame index.
        """
        ...

    def filter(
        self, data: DataFrame, filter_func: Callable[[Any], bool], **filter_opts
    ) -> DataFrame:
        """ Outputs a filtered version of the input dataset respecting `filter_opts`. """
        ...

    def run(
        self,
        cache: Dict[str, str],
        aux: Dict[str, DataFrame],
        fetch_opts: Dict[str, Any] = None,
        parse_opts: Dict[str, Any] = None,
        merge_opts: Dict[str, Any] = None,
        filter_func: Callable[[Any], bool] = None,
        filter_opts: Dict[str, Any] = None,
    ) -> DataFrame:
        data: DataFrame = None

        # Fetch the data, feeding the cached resources to the fetch step
        data = self.fetch(cache, **(fetch_opts or {}))

        # Make yet another copy of the auxiliary table to avoid affecting future steps in `parse`
        data = self.parse(data, {name: df.copy() for name, df in aux.items()}, **(parse_opts or {}))

        # Merging is done record by record
        data["key"] = data.apply(lambda x: self.merge(x, aux), axis=1)

        # Drop records which have no key merged
        # TODO: log records with missing key somewhere on disk
        data = data.dropna(subset=["key"])

        # Filter out data according to the user-provided filter function
        if filter_func is not None:
            data = self.filter(data, filter_func, **(filter_opts or {}))

        # Return the final dataframe
        return data


class DefaultPipeline(DataPipeline):
    """
    Data pipeline which provides a default implementation for:
    * Fetch: downloads raw data from a list of URLs into ../snapshots folder. See [lib.net].
    * Merge: outputs a key from the auxiliary dataset after performing best-effort matching.
    * Filter: applies `filter_func` to each record and keeps the rows for which the result is `true`

    The merge function provided here is crucial for many pipelines that use it. The easiest/fastest
    way to merge records is by providing the exact `key` that will match an existing record in the
    [data/metadata.csv] file.
    """

    data_urls: List[Union[Path, str]] = []
    """ Define our URLs of raw data to be downloaded """

    fetch_opts: List[Dict[str, Any]] = []
    """ Fetch options; see [lib.net.download] for more details """

    def fetch(self, cache: Dict[str, str], **fetch_opts) -> List[str]:
        num_urls = len(self.data_urls)
        fetch_iter = zip(self.data_urls, self.fetch_opts or [{}] * num_urls)
        return [download(url, **{**opts, **fetch_opts}) for url, opts in fetch_iter]

    def merge(
        self, record: Dict[str, Any], aux: Dict[str, DataFrame], **merge_opts
    ) -> Optional[str]:
        # Merge only needs the metadata auxiliary data table
        metadata = aux["metadata"]

        # Exact key match might be possible and it's the fastest option
        if "key" in record and not isnull(record["key"]):
            if record["key"] in metadata["key"].values:
                return record["key"]
            else:
                warnings.warn("Key provided but not found in metadata: {}".format(record))
                return None

        # Start by filtering the auxiliary dataset as much as possible
        for column_prefix in ("country", "subregion1", "subregion2"):
            for column_suffix in ("code", "name"):
                column = "{}_{}".format(column_prefix, column_suffix)
                if column not in record:
                    continue
                elif isnull(record[column]):
                    metadata = metadata[metadata[column].isna()]
                elif record[column]:
                    metadata = metadata[metadata[column] == record[column]]

        # Auxiliary dataset might have a single record left, then we are done
        if len(metadata) == 1:
            return metadata.iloc[0]["key"]

        # Compute a fuzzy version of the record's match string for comparison
        match_string = fuzzy_text(record["match_string"]) if "match_string" in record else None

        # Provided match string could be a subregion code / name
        if match_string is not None:
            for column_prefix in ("subregion1", "subregion2"):
                for column_suffix in ("code", "name"):
                    column = "{}_{}".format(column_prefix, column_suffix)
                    aux_match = metadata[column + "_fuzzy"] == match_string
                    if sum(aux_match) == 1:
                        return metadata[aux_match].iloc[0]["key"]

        # Provided match string could be identical to `match_string` (or with simple fuzzy match)
        if match_string is not None:
            aux_match_1 = metadata["match_regex_fuzzy"] == match_string
            if sum(aux_match_1) == 1:
                return metadata[aux_match_1].iloc[0]["key"]
            aux_match_2 = metadata["match_regex"] == record["match_string"]
            if sum(aux_match_2) == 1:
                return metadata[aux_match_2].iloc[0]["key"]

        # Last resort is to match the `match_string` column with a regex from aux
        if match_string is not None:
            aux_mask = ~metadata["match_regex"].isna()
            aux_regex = metadata["match_regex"][aux_mask].apply(
                lambda x: re.compile(x, re.IGNORECASE)
            )
            aux_match = aux_regex.apply(lambda x: True if x.match(match_string) else False)
            if sum(aux_match) == 1:
                metadata = metadata[aux_mask]
                return metadata[aux_match].iloc[0]["key"]

        warnings.warn("No key match found for:\n{}".format(record))
        return None

    def _read(self, file_paths: List[str], **read_opts) -> List[DataFrame]:
        """ Reads a raw file input path into a DataFrame """
        return [read_file(file_path, **read_opts) for file_path in file_paths]

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        return self.parse_dataframes(self._read(sources), aux, **parse_opts)

    def filter(
        self, data: DataFrame, filter_func: Callable[[Any], bool], **filter_opts
    ) -> DataFrame:
        return data[data.apply(filter_func, axis=1)]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        """ Parse the inputs into a single output dataframe """
        raise NotImplementedError()


class ExternalProcessPipeline(DefaultPipeline):

    command: str = ""
    arguments: List[str] = []

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        process = subprocess.Popen(
            [self.command] + self.arguments + sources,
            cwd=ROOT / "src",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait for process to finish and get out and err streams
        stdout, stderr = process.communicate()

        # Write error to our stderr output
        if stderr:
            print(stderr.decode("UTF-8"), file=sys.stderr)

        # Verify that the stdout output is not empty
        if not stdout:
            raise RuntimeError("Output is empty, did you write the CSV to STDOUT?")

        # Decode stdout as a string
        output = stdout.decode("UTF-8")

        # Verify that stdout isn't just reporting an error (why is this not written to stderr?)
        if output.startswith("Fatal error:"):
            raise RuntimeError(output.strip())

        # Finally, read the output as a CSV
        return read_csv(StringIO(output))


class PipelineChain:
    """
    A pipeline chain is a collection of individual [DataPipeline]s which produce a full table
    ready for output. This is a very thin wrapper that runs the data pipelines and combines their
    outputs.

    One of the reasons for a dedicated class is to allow for discovery of [PipelineChain] objects
    via reflection, users of this class are encouraged to override its methods if custom processing
    is required.

    A pipeline chain is responsible for loading the auxiliary datasets that are passed to the
    individual pipelines. Pipelines can load data themselves, but if the same auxiliary dataset
    is used by many of them it is more efficient to load it here.
    """

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = []
    """ List of pipeline-options tuples executed in order """

    auxiliary_tables: Dict[str, Union[Path, str]] = {
        "metadata": ROOT / "src" / "data" / "metadata.csv",
        "country_codes": ROOT / "src" / "data" / "country_codes.csv",
        "knowledge_graph": ROOT / "src" / "data" / "knowledge_graph.csv",
    }
    """ Auxiliary datasets passed to the pipelines during processing """

    schema: Dict[str, Any] = {}
    """ Names and corresponding dtypes of output columns """

    def output_table(self, data: DataFrame) -> DataFrame:
        """
        This function performs the following operations:
        1. Filters out columns not in the output schema
        2. Converts each column to the appropriate type
        3. Sorts the values based on the column order
        4. Outputs the resulting data
        """
        output_columns = list(self.schema.keys())

        # Make sure all columns are present and have the appropriate type
        for column, dtype in self.schema.items():
            if column not in data:
                data[column] = None
            data[column] = column_convert(data[column], dtype)

        # Filter only output columns and output the sorted data
        return data[output_columns].sort_values(output_columns)

    @staticmethod
    def _run_wrapper(
        pipeline_cache_aux_kwargs: Tuple[
            DataPipeline, Dict[str, str], Dict[str, DataFrame], Dict[str, Any]
        ]
    ) -> Optional[DataFrame]:
        """ Workaround necessary for multiprocess pool, which does not accept lambda functions """
        pipeline, cache, aux, opts = pipeline_cache_aux_kwargs
        try:
            return pipeline.run(cache, aux, **opts)
        except Exception as exc:
            warnings.warn("Error running pipeline {}".format(pipeline.__class__.__name__))
            traceback.print_exc()
        return None

    def run(
        self,
        pipeline_name: str,
        process_count: int = cpu_count(),
        verify: str = "simple",
        progress: bool = True,
        **pipeline_opts,
    ) -> DataFrame:
        """
        Main method which executes all the associated [DataPipeline] objects and combines their
        outputs.
        """
        # Read the cache directory from our cloud storage
        try:
            cache = requests.get("{}/sitemap.json".format(CACHE_URL)).json()
        except:
            cache = {}
            warnings.warn("Cache unavailable")

        # Read the auxiliary input files into memory
        aux = {name: read_file(file_name) for name, file_name in self.auxiliary_tables.items()}

        # Precompute some useful transformations in the auxiliary input files
        aux["metadata"]["match_regex_fuzzy"] = aux["metadata"].match_regex.apply(fuzzy_text)
        for category in ("country", "subregion1", "subregion2"):
            for suffix in ("code", "name"):
                column = "{}_{}".format(category, suffix)
                aux["metadata"]["{}_fuzzy".format(column)] = aux["metadata"][column].apply(
                    fuzzy_text
                )

        # Get all the pipeline outputs
        # This operation is parallelized but output order is preserved
        map_iter = [
            # Make a copy of the auxiliary table to prevent modifying it for everyone, but this way
            # we allow for local modification (which might be wanted for optimization purposes)
            (
                pipeline,
                cache,
                {name: df.copy() for name, df in aux.items()},
                # TODO: Combine nested attributes of dict instead of plain override
                {"parse_opts": {"progress": progress}, **opts, **pipeline_opts},
            )
            for pipeline, opts in self.pipelines
        ]

        # If the process count is less than one, run in series (useful to evaluate performance)
        progress_label = f"Run {pipeline_name} pipeline"
        if process_count <= 1 or len(map_iter) <= 1:
            map_func = tqdm(
                map(PipelineChain._run_wrapper, map_iter),
                total=len(map_iter),
                desc=progress_label,
                disable=not progress,
            )
        else:
            map_func = process_map(
                PipelineChain._run_wrapper, map_iter, desc=progress_label, disable=not progress,
            )

        # Combine all pipeline outputs into a single DataFrame
        pipeline_outputs = [result for result in map_func if result is not None]
        if not pipeline_outputs:
            warnings.warn("Empty result for pipeline chain {}".format(pipeline_name))
            data = DataFrame(columns=self.schema.keys())
        else:
            progress_label = pipeline_name if progress else None
            data = combine_tables(pipeline_outputs, ["date", "key"], progress_label=progress_label)

        # Return data using the pipeline's output parameters
        data = self.output_table(data)

        # Skip anomaly detection unless requested
        if verify == "simple":

            # Validate that the table looks good
            detect_anomaly_all(self.schema, data, pipeline_name)

        if verify == "full":

            # Perform stale column detection for each known key
            map_iter = data.key.unique()
            map_func = lambda key: detect_stale_columns(
                self.schema, data[data.key == key], (pipeline_name, key)
            )
            progress_label = f"Verify {pipeline_name} pipeline"
            if process_count <= 1 or len(map_iter) <= 1:
                map_func = tqdm(
                    map(map_func, map_iter),
                    total=len(map_iter),
                    desc=progress_label,
                    disable=not progress,
                )
            else:
                map_func = process_map(
                    map_func, map_iter, desc=progress_label, disable=not progress,
                )

            # Show progress as the results arrive if requested
            if progress:
                map_func = tqdm(
                    map_func, total=len(map_iter), desc=f"Verify {pipeline_name} pipeline"
                )

            # Consume the results
            _ = list(map_func)

        return data
