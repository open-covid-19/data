#!/usr/bin/env python

from datetime import datetime
from pandas import DataFrame, NA
from utils import github_raw_dataframe, dataframe_output, pivot_table


# Get CSV file from Github
df = github_raw_dataframe('swsoyee/2019-ncov-japan', 'Data/byDate.csv')
df = df.rename(columns={'date': 'Date'})
df['Date'] = df['Date'].apply(lambda date: datetime.strptime(str(date), '%Y%m%d'))
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())
df = df.set_index('Date').cumsum()
df = pivot_table(df, pivot_name='RegionName').rename(columns={'Value': 'Confirmed'})
df['Deaths'] = None
df = df.dropna(how='all', subset=['Confirmed', 'Deaths'])

# Output the results
dataframe_output(df, 'JP')