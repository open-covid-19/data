#!/usr/bin/env python

import sys
from datetime import datetime, timedelta
from pandas import isna
from covid_io import read_argv
from utils import dataframe_output, merge_previous


# Retrieve the CSV files from GitHub
df, prev_data = read_argv()
df = df.rename(columns={
    'fecha': 'Date',
    'casos_total': 'Confirmed',
    'fallecimientos': 'Deaths'
})

# Parse date into a datetime object
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date).date())

# Add the country code to all records
df['CountryCode'] = 'ES'

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.isoformat())


def filter_function(row): return row['CountryCode'] == 'ES' and isna(row['RegionCode'])


# Merge with the prior data
prev_data = prev_data.loc[prev_data.apply(filter_function, axis=1)]
df = merge_previous(df, prev_data, ['Date', 'CountryCode'])

# Output the results
dataframe_output(df)
