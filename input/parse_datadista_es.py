#!/usr/bin/env python

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

from utils import dataframe_output, merge_previous

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# This script can parse both region-level and country-level data
is_region = sys.argv[1] == 'region'

# Confirmed and deaths come from different CSV files, parse them separately first
url_base = 'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019'
confirmed = pd.read_csv('%s/ccaa_covid19_casos_long.csv' % url_base).rename(columns={
    'fecha': 'Date',
    'CCAA': '_RegionLabel',
    'total': 'Confirmed'
})
deaths = pd.read_csv('%s/ccaa_covid19_fallecidos_long.csv' % url_base).rename(columns={
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
    filter_function = lambda row: row['CountryCode'] == 'ES' and pd.isna(row['RegionCode'])
    df = merge_previous(df, ['Date', 'CountryCode'], filter_function)

# Output the results
dataframe_output(df, ROOT, 'es' if is_region else 'world')