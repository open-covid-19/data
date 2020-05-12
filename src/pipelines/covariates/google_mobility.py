from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.time import datetime_isoformat
from lib.utils import grouped_diff
from .pipeline import CovariatesPipeline


class GoogleMobilityPipeline(CovariatesPipeline):
    data_urls: List[str] = [
        'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
    ]

    def parse_dataframes(self, dataframes: List[DataFrame], **parse_opts):
        data = dataframes[0].rename(columns = {
            'country_region_code': 'country_code',
            'sub_region_2': 'subregion_2_code',
            'sub_region_1': 'subregion_1_code'
        }).drop(columns = ['country_region']).melt(
            id_vars=['date', 'country_code', 'subregion_1_code',
                     'subregion_2_code'],
            value_vars=['retail_and_recreation_percent_change_from_baseline',
                        'grocery_and_pharmacy_percent_change_from_baseline',
                        'parks_percent_change_from_baseline',
                        'transit_stations_percent_change_from_baseline',
                        'workplaces_percent_change_from_baseline',
                        'residential_percent_change_from_baseline'],
            var_name='value_type', value_name='value')
        return data
