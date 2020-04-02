#!/usr/bin/env python

import pandas
import datetime
from utils import github_raw_dataframe, dataframe_output


# Read CSV file from covidtracking's GitHub project
df = pandas.read_csv('https://health-infobase.canada.ca/src/data/covidLive/covid19.csv')

# Rename the appropriate columns
df = df.rename(columns={
    'date': 'Date',
    'prname': '_RegionLabel',
    'numconf': 'Confirmed',
    'numdeaths': 'Deaths',
    'numtested': 'Tested'
})

 # Convert date to datetime object
df['Date'] = df['Date'].apply(
    lambda date: datetime.datetime.strptime(date, '%d-%m-%Y').date().isoformat())

# Get list of all dates
date_list = pandas.date_range(df['Date'].iloc[0], df['Date'].iloc[-1])
date_list = [date.date().isoformat() for date in date_list]

# Make sure all dates have data, fill with nulls if necessary
regions = df['_RegionLabel'].unique()
df = df.set_index(['_RegionLabel', 'Date'])
for date in date_list:
    for region in regions:
        idx = (region, date)
        if idx not in df.index:
            df.loc[idx, 'Confirmed'] = 0 if date == date_list[0] else None
            df.loc[idx, 'Deaths'] = 0 if date == date_list[0] else None
            df.loc[idx, 'Tested'] = 0 if date == date_list[0] else None

# Forward fill those null values
df = df.sort_index()
for col in ('Confirmed', 'Deaths'):
    df[col] = df.groupby(['_RegionLabel', 'Date'])[col].first().ffill()

# Reindex and sort the data
df = df.reset_index().sort_values(['Date', '_RegionLabel'])

# Output the results
dataframe_output(df, 'CA')