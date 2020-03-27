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

if is_region:
    df = github_raw_dataframe('pcm-dpc/COVID-19', 'dati-json/dpc-covid19-ita-regioni.json')
else:
    df = github_raw_dataframe('pcm-dpc/COVID-19', 'dati-json/dpc-covid19-ita-andamento-nazionale.json')

df = df.rename(columns={
    'data': 'Date',
    'totale_casi': 'Confirmed',
    'deceduti': 'Deaths',
    'tamponi': 'Tested'
})

if is_region:
    df['_RegionLabel'] = df['denominazione_regione']

# Parse date into a datetime object
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date).date())

# Offset date by 1 day to match ECDC report
if not is_region:
    df['RegionCode'] = None
    df['Date'] = df['Date'].apply(lambda date: date + timedelta(days=1))

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.isoformat())

# Add the country code to all records
df['CountryCode'] = 'IT'

# Merge the new data with the existing data (prefer new data if duplicates)
if not is_region:
    filter_function = lambda row: row['CountryCode'] == 'IT' and pandas.isna(row['RegionCode'])
    df = merge_previous(df, ['Date', 'CountryCode'], filter_function)

# Output the results
dataframe_output(df, ROOT, 'IT' if is_region else None)