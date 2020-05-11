from datetime import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff


class Covid19BrazilTimeseriesPipeline(DefaultPipeline):
    url_base = 'https://raw.github.com/elhenrico/covid19-Brazil-timeseries/master'
    data_urls: List[str] = [
        '{}/confirmed-new.csv'.format(url_base),
        '{}/deaths-new.csv'.format(url_base),
    ]

    def parse_dataframes(
            self, dataframes: List[DataFrame], aux: List[DataFrame], **parse_opts) -> DataFrame:

        # Read data from GitHub repo
        confirmed, deaths = dataframes
        for df in (confirmed, deaths):
            df.rename(columns={'Unnamed: 1': 'subregion1_code'}, inplace=True)
            df.set_index('subregion1_code', inplace=True)

        # Transform the data from non-tabulated format to record format
        records = []
        for region_code in confirmed.index.unique():
            if '(' in region_code or region_code == 'BR': continue
            for col in confirmed.columns[1:]:
                date = col + '/' + str(datetime.now().year)
                date = datetime.strptime(date, '%d/%m/%Y').date().isoformat()
                records.append({
                    'date': date,
                    'country_code': 'BR',
                    'subregion1_code': region_code,
                    'confirmed': confirmed.loc[region_code, col],
                    'deceased': deaths.loc[region_code, col]})

        return DataFrame.from_records(records)
