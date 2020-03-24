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

if is_region:
    CPC_JSON_URL = 'https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-regioni.json'
else:
    CPC_JSON_URL = 'https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-andamento-nazionale.json'

df = pd.read_json(CPC_JSON_URL).rename(columns={
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
    df['Date'] = df['Date'].apply(lambda date: date + timedelta(days=1))

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.isoformat())

# Add the country code to all records
df['CountryCode'] = 'IT'

# Merge the new data with the existing data (prefer new data if duplicates)
if not is_region:
    filter_function = lambda row: row['CountryCode'] == 'IT' and pd.isna(row['RegionCode'])
    df = merge_previous(df, ['Date', 'CountryCode'], filter_function)

# Output the results
dataframe_output(df, ROOT, 'it' if is_region else 'world')