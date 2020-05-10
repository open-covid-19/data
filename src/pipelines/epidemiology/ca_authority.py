from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.time import datetime_isoformat
from lib.utils import grouped_diff
from .pipeline import EpidemiologyPipeline


class CanadaPipeline(EpidemiologyPipeline):
    data_urls: List[str] = ['https://health-infobase.canada.ca/src/data/covidLive/covid19.csv']

    def parse_dataframes(self, dataframes: List[DataFrame], **parse_opts):

        # Rename the appropriate columns
        data = dataframes[0].rename(columns={
            'prname': 'subregion_1_name',
            'numconf': 'confirmed',
            'numdeaths': 'deceased',
            'numtested': 'tested',
            'numrecover': 'recovered',
        }).drop(columns=['prnameFR'])

        # Convert date to ISO format
        data['date'] = data['date'].apply(lambda x: datetime_isoformat(x, '%d-%m-%Y'))

        # Compute the daily counts
        data = grouped_diff(data, ['subregion_1_name', 'date'])

        # Make sure all records have the country code
        data['country_code'] = 'CA'

        # Country-level records should have null region name
        country_mask = data['subregion_1_name'] == 'Canada'
        data.loc[country_mask, 'subregion_1_name'] = None

        # Output the results
        return data

    def transform(self, data: DataFrame, aux: DataFrame, **transform_opts):
        # Prevent date adjustment for country-level
        return data
