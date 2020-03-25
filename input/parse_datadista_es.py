#!/usr/bin/env python

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas

from utils import parse_level_args, github_raw_dataframe, dataframe_output, merge_previous

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# This script can parse both region-level and country-level data
is_region = parse_level_args(sys.argv[1:]).level == 'region'

# Confirmed and deaths come from different CSV files, parse them separately first
confirmed = github_raw_dataframe(
    'datadista/datasets', 'COVID%2019/ccaa_covid19_casos_long.csv').rename(columns={
        'fecha': 'Date',
        'CCAA': '_RegionLabel',
        'total': 'Confirmed'
})
deaths = github_raw_dataframe(
    'datadista/datasets', 'COVID%2019/ccaa_covid19_fallecidos_long.csv').rename(columns={
        'fecha': 'Date',
        'CCAA': '_RegionLabel',
        'total': 'Deaths'
})

# Now we can simply join them into the same table
df = confirmed.merge(deaths)

# Parse date into a datetime object
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date).date())

# Offset date by 1 day to match ECDC report
if not is_region:
    df['Date'] = df['Date'].apply(lambda date: date + timedelta(days=1))

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.isoformat())

# Add the country code to all records
df['CountryCode'] = 'ES'

# Country-level data is embedded as "Total" in the CSV files
if is_region:
    df = df[df['_RegionLabel'] != 'Total']
else:
    df['RegionCode'] = None
    df = df[df['_RegionLabel'] == 'Total']
    df = df.drop(columns=['_RegionLabel'])

# Merge the new data with the existing data (prefer new data if duplicates)
if not is_region:
    filter_function = lambda row: row['CountryCode'] == 'ES' and pandas.isna(row['RegionCode'])
    df = merge_previous(df, ['Date', 'CountryCode'], filter_function)

# Output the results
dataframe_output(df, ROOT, 'es' if is_region else 'world')