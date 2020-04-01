#!/usr/bin/env python

'''
This script loads the latest CSV from
https://github.com/jgehrcke/covid-19-germany-gae and extracts the confirmed
cases, deaths and total tests for each subregion in Germany.

Credit to @jgehrcke + contributors for scraping the data.
'''

import sys
from datetime import datetime
from numpy import unique
from pandas import DataFrame
from utils import github_raw_dataframe, dataframe_output


# Read CSV file from GitHub project
df = github_raw_dataframe('jgehrcke/covid-19-germany-gae', 'data.csv')

# Rename the appropriate columns
df = df.rename(columns={'time_iso8601': 'Date'})

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date).date().isoformat())

# Get a list of all region codes
regions = unique([col[3:5] for col in df.columns if col.startswith('DE-')])

# Transform the data from non-tabulated format to our record format
records = []
for idx, row in df.iterrows():
    record = {'Date': row['Date'], 'CountryCode': 'DE'}
    for region in regions:
        records.append({
            'RegionCode': region,
            'Confirmed': row['DE-%s_cases' % region],
            'Deaths': row['DE-%s_deaths' % region],
            **record
        })
df = DataFrame.from_records(records)

# Output the results
dataframe_output(df, 'DE')