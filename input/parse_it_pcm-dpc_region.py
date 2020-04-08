#!/usr/bin/env python

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas
from covid_io import read_argv
from utils import dataframe_output, merge_previous


df = read_argv().rename(columns={
    'data': 'Date',
    'totale_casi': 'Confirmed',
    'deceduti': 'Deaths',
    'tamponi': 'Tested'
})

df['_RegionLabel'] = df['denominazione_regione']

# Parse date into a datetime object
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date).date())

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.isoformat())

# Add the country code to all records
df['CountryCode'] = 'IT'

# Output the results
dataframe_output(df, 'IT')