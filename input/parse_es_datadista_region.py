#!/usr/bin/env python

import sys
from datetime import datetime, timedelta
from pandas import isna
from covid_io import read_argv
from utils import dataframe_output, merge_previous


# Confirmed and deaths come from different CSV files, parse them separately first
confirmed, deaths, prev_data = read_argv()
confirmed = confirmed.rename(columns={
    'fecha': 'Date',
    'CCAA': '_RegionLabel',
    'total': 'Confirmed'
})
deaths = deaths.rename(columns={
    'fecha': 'Date',
    'CCAA': '_RegionLabel',
    'total': 'Deaths'
})

# Now we can simply join them into the same table
df = confirmed.merge(deaths)

# Parse date into a datetime object
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date).date())

# Add the country code to all records
df['CountryCode'] = 'ES'

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.isoformat())


# Output the results
dataframe_output(df, 'ES')
