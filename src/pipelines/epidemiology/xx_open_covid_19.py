from typing import Any, Dict, List
from pandas import DataFrame, isnull
from lib.cast import safe_int_cast
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat, date_offset
from lib.utils import ROOT


class OpenCovid19Pipeline(DefaultPipeline):
    def fetch(self, cache: Dict[str, str], **fetch_opts):
        return [ROOT / "output" / "tables" / "epidemiology.csv"]

    def parse_dataframes(
        self, dataframes: List[DataFrame], metadata: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        return dataframes[0]
