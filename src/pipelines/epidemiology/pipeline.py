from typing import Any, Dict
from pandas import DataFrame

from lib.time import date_offset
from lib.default_pipeline import DefaultPipeline


class EpidemiologyPipeline(DefaultPipeline):
    output_columns: Dict[str, Any] = {
        'date': str,
        'key': str,
        'confirmed': 'Int64',
        'deceased': 'Int64',
        'hospitalised': 'Int64',
    }

    def transform(self, data: DataFrame, aux: DataFrame, **transform_opts) -> DataFrame:
        ''' Adjust the date of the data based on the report offset '''

        # Save the current columns to filter others out at the end
        data_columns = data.columns

        # Filter auxiliary dataset to only get the relevant keys
        data = data.merge(aux, suffixes=('', 'aux_'))

        # Perform date adjustment for all records so date is consistent across datasets
        data['date'] = data.apply(lambda x: date_offset(x['date'], x['epi_report_offset']), axis=1)

        return data[data_columns]
