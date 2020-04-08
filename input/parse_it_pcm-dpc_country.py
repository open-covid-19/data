#!/usr/bin/env python

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas
from covid_io import read_argv
from utils import dataframe_output, merge_previous


df, prev_data = read_argv()
df = df.rename(columns={
    'data': 'Date',
    'totale_casi': 'Confirmed',
    'deceduti': 'Deaths',
    'tamponi': 'Tested'
})

# Parse date into a datetime object
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date).date())

# Offset date by 1 day to match ECDC report
df['RegionCode'] = None
df['Date'] = df['Date'].apply(lambda date: date + timedelta(days=1))

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.isoformat())

# Add the country code to all records
df['CountryCode'] = 'IT'

# Merge the new data with the existing data (prefer new data if duplicates)
filter_function = lambda row: row['CountryCode'] == 'IT' and pandas.isna(row['RegionCode'])
prev_data = prev_data.loc[prev_data.apply(filter_function, axis=1)]
df = merge_previous(df, prev_data, ['Date', 'CountryCode'])

# Output the results
dataframe_output(df)