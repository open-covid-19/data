#!/usr/bin/env python

from datetime import datetime
from pandas import DataFrame, NA
from covid_io import read_argv
from utils import dataframe_output, pivot_table


# Get CSV file from Github
# https://raw.github.com/swsoyee/2019-ncov-japan/master/Data/byDate.csv
df = read_argv()
df = df.rename(columns={'date': 'Date'})
df['Date'] = df['Date'].apply(lambda date: datetime.strptime(str(date), '%Y%m%d'))
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())
df = df.set_index('Date').cumsum()
df = pivot_table(df, pivot_name='RegionName').rename(columns={'Value': 'Confirmed'})
df['Deaths'] = None
df = df.dropna(how='all', subset=['Confirmed', 'Deaths'])

# Output the results
dataframe_output(df, 'JP')
