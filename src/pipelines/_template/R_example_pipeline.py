from typing import Any, Dict, List
from pandas import DataFrame
from lib.pipeline import ExternalProcessPipeline


class RExamplePipeline(ExternalProcessPipeline):
    """
    Pipeline which runs an R script for the parsing step. If `data_urls` is not empty, the files
    in those URLs will be downloaded and the list of local file names will be passed separated by
    spaces as arguments to your script.
    """

    data_urls: List[str] = ["https://example.com/data.csv"]
    """ Define our data URLs or leave list empty if you want to perform the fetching in R """

    fetch_opts: List[Dict[str, Any]] = []
    """ Leave the fetch options as the default, see [lib.net.download] for more details """

    command: str = "RScript"
    """ Prefer to invoke using the RScript binary to avoid executable permission issues """

    arguments: List[str] = ["./R_example_pipeline.R"]
    """ Optionally, add a list of arguments to be passed to your script """
