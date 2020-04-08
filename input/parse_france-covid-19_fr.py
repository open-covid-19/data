#!/usr/bin/env python

from datetime import datetime
from pandas import DataFrame
from utils import dataframe_output, merge_previous
from covid_io import read_argv


# Confirmed and deaths come from different CSV files, parse them separately first
# https://raw.github.com/cedricguadalupe/FRANCE-COVID-19/master/france_coronavirus_time_series-confirmed.csv
# https://raw.github.com/cedricguadalupe/FRANCE-COVID-19/master/france_coronavirus_time_series-deaths.csv
confirmed, deaths = read_argv()
for df in (confirmed, deaths):
    df.set_index('Date', inplace=True)

# Iterate through all date-region combinations
regions = confirmed.columns.tolist()
df = DataFrame(columns=['Date', '_RegionLabel', 'Confirmed', 'Deaths']).set_index(['Date', '_RegionLabel'])
for region in regions:
    if region == 'Total': continue
    for date, value in zip(confirmed.index, confirmed[region]):
        df.loc[(date, region), 'Confirmed'] = value
    for date, value in zip(deaths.index, deaths[region]):
        df.loc[(date, region), 'Deaths'] = value

# Dates need converted to ISO format
df = df.sort_values(['Date', '_RegionLabel']).reset_index()
df['Date'] = df['Date'].apply(lambda date: datetime.strptime(date, '%d/%m/%Y').date().isoformat())

# Output the results
dataframe_output(df, 'FR')