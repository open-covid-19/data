from typing import Any, Dict
from pandas import DataFrame

from lib.time import date_offset
from lib.default_pipeline import DefaultPipeline


class TemplatePipeline(DefaultPipeline):
    '''
    Base pipeline which will be subclassed by all the pipelines within this
    module. It's not strictly enforced, but it's highly recommended to use the
    same base class for all the pipelines.
    '''

    schema: Dict[str, Any] = {
        'date': str,
        'key': str,
        'column1': 'Int64',
        'column2': float,
    }
    ''' Defines the schema of the output table '''

    ##
    # Any functions of DefaultPipeline could be overridden here. We could also
    # define out own functions here if necessary.
    ##
