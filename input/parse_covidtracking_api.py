#!/usr/bin/env python

'''
This script loads the latest CSV from covidtracking.com website and extracts
the confirmed cases, deaths and total tests for each state. The output is 
saved both in CSV and JSON format under the `output` folder.

Credit to the covidtracking.com team for scraping the data from each state.
'''

import os
import datetime
import pandas as pd
from pathlib import Path
import requests

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read JSON file from covidtracking's website
# We must use the requests package directly because covidtracking returns 403 otherwise
df = pd.read_json(requests.get(
    'http://covidtracking.com/api/states/daily', headers={'User-agent': 'Mozilla/5.0'}).text)

# Rename the appropriate columns
df = df.rename(columns={
    'date': 'Date',
    'state': 'Region',
    'positive': 'Confirmed', 
    'death': 'Deaths', 
    'total': 'Tested'
})

# Null values are not the same as zero, make sure all numbers are string objects
for col in ('Confirmed', 'Deaths', 'Tested'):
    df[col] = df[col].dropna().astype(int).astype(str)

# Convert date to ISO format
df['Date'] = df['Date'].apply(
    lambda date: datetime.datetime.strptime(str(date), '%Y%m%d').strftime('%Y-%m-%d'))

# Get the coordinates for each region
df = df.merge(pd.read_csv(ROOT / 'input' / 'usa_regions.csv'))
df['CountryName'] = 'United States of America'

# Sort dataset by date + region
df = df.sort_values(['Date', 'Region'])
df = df[[
    'Date', 
    'Region', 
    'CountryCode', 
    'CountryName', 
    'Confirmed', 
    'Deaths', 
    'Tested', 
    'Latitude', 
    'Longitude'
]]

# Extract a subset with only the latest date
df_latest = pd.DataFrame(columns=list(df.columns))
for country in df['Region'].unique():
    df_latest = pd.concat([df_latest, df[df['Region'] == country].iloc[-1:]])

# Save dataset in CSV format into output folder
df.to_csv(ROOT / 'output' / 'usa.csv', index=False)
df_latest.to_csv(ROOT / 'output' / 'usa_latest.csv', index=False)

# Save dataset in JSON format into output folder
df.to_json(ROOT / 'output' / 'usa.json', orient='records')
df_latest.to_json(ROOT / 'output' / 'usa_latest.json', orient='records')