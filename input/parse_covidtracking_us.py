#!/usr/bin/env python

'''
This script loads the latest JSON from covidtracking.com website and extracts
the confirmed cases, deaths and total tests for each state.

Credit to the covidtracking.com team for scraping the data from each state.
'''

import sys
import datetime
from utils import github_raw_dataframe, dataframe_output


# Read CSV file from covidtracking's GitHub project
df = github_raw_dataframe(
    'COVID19Tracking/covid-tracking-data', 'data/states_daily_4pm_et.csv')

# Rename the appropriate columns
df = df.rename(columns={
    'date': 'Date',
    'state': 'RegionCode',
    'positive': 'Confirmed',
    'death': 'Deaths',
    'total': 'Tested'
})

# Convert date to ISO format
df['Date'] = df['Date'].apply(
    lambda date: datetime.datetime.strptime(str(date), '%Y%m%d').date().isoformat())

# Output the results
dataframe_output(df, 'US')