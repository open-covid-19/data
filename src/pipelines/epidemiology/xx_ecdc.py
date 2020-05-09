from typing import Any, Dict, List
from pandas import DataFrame
from lib.time import datetime_isoformat
from .pipeline import EpidemiologyPipeline

class ECDCPipeline(EpidemiologyPipeline):
    data_urls: List[str] = ['https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/']
    fetch_opts: List[Dict[str, Any]] = [{'ext': 'csv'}]

    def parse_dataframes(self, dataframes: List[DataFrame], **parse_opts):
        data = dataframes[0]

        # Ensure date field is used as a string
        data['dateRep'] = data['dateRep'].astype(str)

        # Convert date to ISO format
        data['date'] = data['dateRep'].apply(lambda x: datetime_isoformat(x, '%d/%m/%Y'))

        # Workaround for https://github.com/open-covid-19/data/issues/8
        # ECDC mistakenly labels Greece country code as EL instead of GR
        data['geoId'] = data['geoId'].apply(lambda code: 'GR' if code == 'EL' else code)

        # Workaround for https://github.com/open-covid-19/data/issues/13
        # ECDC mistakenly labels Greece country code as UK instead of GB
        data['geoId'] = data['geoId'].apply(lambda code: 'GB' if code == 'UK' else code)

        # Workaround for https://github.com/open-covid-19/data/issues/12
        # ECDC data for Italy is simply wrong, so Italy's data will be parsed from a different source
        # ECDC data for Spain is two days delayed because original reporting time mismatch, parse separately
        data = data[data['geoId'] != 'ES']
        data = data[data['geoId'] != 'IT']

        # Add a column to denote null subregion to match only country-level metadata
        data['subregion_1'] = None

        return data.rename(columns={
            'geoId': 'country',
            'cases': 'confirmed',
            'deaths': 'deceased',
        })[['date', 'country', 'subregion_1', 'confirmed', 'deceased']]
