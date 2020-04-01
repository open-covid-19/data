import os
from pathlib import Path

from pandas import DataFrame

from utils import github_raw_dataframe, dataframe_output

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read data from GitHub repo
df = github_raw_dataframe('tomwhite/covid-19-uk-data', 'data/covid-19-indicators-uk.csv')

# Aggregate time series data into relational format
records = []
for idx, rows in df.groupby(['Date', 'Country']):
    records.append({
        'Date': idx[0],
        'Country': idx[1],
        **{record.loc['Indicator']: record.loc['Value'] for _, record in rows.iterrows()}
    })
df = DataFrame.from_records(records).rename(
    columns={'Country': '_RegionLabel', 'ConfirmedCases': 'Confirmed'})

# Output the results
dataframe_output(df, ROOT, 'GB')