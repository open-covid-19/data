from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.time import datetime_isoformat
from lib.utils import grouped_diff
from .pipeline import EpidemiologyPipeline


class OpenZHPipeline(EpidemiologyPipeline):
    data_urls: List[str] = [
        'https://raw.github.com/openZH/covid_19/master/COVID19_Fallzahlen_CH_total.csv'
    ]

    def parse_dataframes(self, dataframes: List[DataFrame], **parse_opts):
        data = dataframes[0].rename(columns={
            'ncumul_tested': 'tested',
            'ncumul_conf': 'confirmed',
            'ncumul_deceased': 'deceased',
            'abbreviation_canton_and_fl': 'subregion_1_code'
        }).drop(columns=['time', 'source'])

        # TODO: Match FL subdivision (not a canton?)
        data = grouped_diff(data, ['subregion_1_code', 'date'])
        data['country_code'] = 'CH'
        return data
