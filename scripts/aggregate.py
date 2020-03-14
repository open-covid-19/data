#!/usr/bin/env python

'''
This script aggregates the time series events into a flat table. It also
extracts the last day of data into its own dataset. The output is saved both
in CSV and JSON format under the `output` folder.
'''

import os
import pandas as pd
from pathlib import Path

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read the time series data from outputs
indices = ['Date', 'Country', 'Region']
ts = pd.read_csv(ROOT / 'time_series.csv').fillna('').set_index(indices)

# Load the region codes into a map for querying
REGION_CODES = pd.read_csv(ROOT / 'input' / 'country_region_codes.csv').set_index('Code')
REGION_MAP = {}
for code, row in REGION_CODES.iterrows():
    REGION_MAP[row['Country'] + '_' + code] = row['Name']
    
# Load the country codes into a nap for querying
COUNTRY_CODES = pd.read_csv(ROOT / 'input' / 'country_iso_codes.csv').set_index('ISO 3166-1-2')
COUNTRY_MAP = {}
for code, row in COUNTRY_CODES.iterrows():
    COUNTRY_MAP[code] = row['ISO 3166']

# Here we convert each type of event into its own column
# There is probably a better way to do this, but data is small and this appears to work
df = ts[[]].copy()
for event in ts['Event'].unique():
    filtered = ts[ts['Event'] == event]
    df.loc[filtered.index, event] = filtered['Value']
df = df.groupby(indices).first().reset_index()

# Expand country and region codes
df['_region_key'] = df.apply(lambda row: row['Country'] + '_' + row['Region'], axis=1)
df['Country'] = df['Country'].apply(lambda code: COUNTRY_MAP[code] if code in COUNTRY_MAP else code)
df['Region'] = df.apply(lambda row: REGION_MAP[row['_region_key']] if row['_region_key'] in REGION_MAP else row['Region'], axis=1) 

# Remove all helper columns from dataset
df = df[[col for col in df.columns if not col.startswith('_')]]

# Extract a subset with only the latest date
df_latest = df[df['Date'] == df['Date'].iloc[-1]]

# Save dataset in CSV format into output folder
df.to_csv(ROOT / 'output' / 'aggregated.csv', index=False)
df_latest.to_csv(ROOT / 'output' / 'latest.csv', index=False)

# Save dataset in JSON format into output folder
df.to_json(ROOT / 'output' / 'aggregated.json', orient='records')
df_latest.to_json(ROOT / 'output' / 'latest.json', orient='records')