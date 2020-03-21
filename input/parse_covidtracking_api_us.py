#!/usr/bin/env python

'''
This script loads the latest JSON from covidtracking.com website and extracts
the confirmed cases, deaths and total tests for each state.

Credit to the covidtracking.com team for scraping the data from each state.
'''

import os
import sys
import datetime
import pandas as pd
from pathlib import Path
import requests

from utils import dataframe_output

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read JSON file from covidtracking's website
# We must use the requests package directly because covidtracking returns 403 otherwise
# df = pd.read_json(requests.get(
#     'https://covidtracking.com/api/states/daily', headers={'User-agent': 'Mozilla/5.0'}).text)
df = pd.read_json(sys.stdin)

# Rename the appropriate columns
df = df.rename(columns={
    'date': 'Date',
    'state': 'RegionCode',
    'positive': 'Confirmed',
    'death': 'Deaths',
    'total': 'Tested'
})

# Convert date to ISO format
df['Date'] = df['Date'].apply(
    lambda date: datetime.datetime.strptime(str(date), '%Y%m%d').date().isoformat())

# Output the results
dataframe_output(df, ROOT, 'us')