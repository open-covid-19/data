import datetime
from typing import Any, Dict, List
from numpy import unique
from pandas import DataFrame, concat, merge
from lib.time import datetime_isoformat
from lib.utils import grouped_diff
from .pipeline import EpidemiologyPipeline


class Covid19GermanyPipeline(EpidemiologyPipeline):
    data_urls: List[str] = [
        'https://raw.github.com/jgehrcke/covid-19-germany-gae/master/data.csv'
    ]

    def parse_dataframes(self, dataframes: List[DataFrame], **parse_opts):

        # Rename the appropriate columns
        data = dataframes[0].rename(columns={'time_iso8601': 'date'})

        # Convert dates to ISO format
        data['date'] = data['date'].apply(
            lambda x: datetime.datetime.fromisoformat(x).date().isoformat())

        # Get a list of all regions
        regions = unique([col[3:5] for col in data.columns if col.startswith('DE-')])

        # Transform the data from non-tabulated format to our record format
        records = []
        for idx, row in data.iterrows():
            record = {'date': row['date']}
            for region_code in regions:
                records.append({
                    'subregion_1_code': region_code,
                    'confirmed': row['DE-%s_cases' % region_code],
                    'deceased': row['DE-%s_deaths' % region_code],
                    **record
                })
        data = DataFrame.from_records(records)

        # Ensure we only take one record from the table
        data = data.groupby(['date', 'subregion_1_code']).last().reset_index()

        # Output the results
        data = grouped_diff(data, ['subregion_1_code', 'date'])
        data['country_code'] = 'DE'
        return data
