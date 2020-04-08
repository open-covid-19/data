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

# Country-level data is embedded as "Total" in the CSV files
region_level = df[df['_RegionLabel'] != 'Total']
country_level = df[df['_RegionLabel'] == 'Total'].drop(columns=['_RegionLabel'])

# Offset date of country-level by 1 day to match ECDC report
country_level['Date'] = country_level['Date'].apply(lambda date: date + timedelta(days=1))

# Convert dates to ISO format
for df in (region_level, country_level):
    df['Date'] = df['Date'].apply(lambda date: date.isoformat())


def filter_function(row): return row['CountryCode'] == 'ES' and isna(row['RegionCode'])


# Merge with the prior data
prev_data = prev_data.loc[prev_data.apply(filter_function, axis=1)]
country_level = merge_previous(country_level, prev_data, ['Date', 'CountryCode'])

# Output the results
dataframe_output(country_level)
dataframe_output(region_level, 'ES')
