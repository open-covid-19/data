from typing import Any, Dict
from pandas import DataFrame

from lib.time import date_offset
from lib.default_pipeline import DefaultPipeline

class CovariatesPipeline(DefaultPipeline):
    schema: Dict[str, Any] = {
        'date': str,
        'key': str,
        'value_type': str,
        'value': 'Int64'
    }
