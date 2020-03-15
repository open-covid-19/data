# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import os
import re
import sys
import datetime
from pathlib import Path
import pandas as pd

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Get a list of the valid China regions
china_regions = pd.read_csv(ROOT / 'input' / 'china_regions.csv', dtype=str)
region_list = china_regions['Region'].unique()

def parse_record(date: str, tokens: list):
    return [{
        'Date': date,
        'Region': tokens[0],
        'Confirmed': tokens[-2],
        'Deaths': tokens[-1]
    }]

# Get date from the arguments passed to the script
date = sys.argv[1]
# date = datetime.datetime.strptime(fname.split('/')[-1].split('.')[0], '%Y%m%d').strftime('%Y-%m-%d')
date_value = datetime.datetime.strptime(date, '%Y%m%d')
date_formatted = datetime.datetime.strftime(date_value, '%Y-%m-%d')

records = []
# for line in open(fname, 'r'):
for line in sys.stdin:

    # Remove whitespace around lines
    line = line.strip()

    # Filter out empty lines
    if not line: continue

    # Assume columns are separated by at least 4 spaces
    line = re.sub('\s\s\s+', '\t', line)
    tokens = line.split('\t')

    # Filter out lines with less than 7 columns
    if len(tokens) < 7: continue

    # Only process tokens from China region
    if tokens[0] in region_list:
        records += parse_record(date_formatted, tokens)

# Put resulting records into a dataframe
df = pd.DataFrame.from_records(records).merge(china_regions, on='Region')
df['CountryName'] = 'China'

# Merge the new data with the existing data
df = pd.concat([df, pd.read_csv(ROOT / 'output' / 'china.csv', dtype=str)], sort=False).drop_duplicates()

# Sort dataset by date + region
df = df.sort_values(['Date', 'Region'])
df = df[['Date', 'Region', 'CountryCode', 'CountryName', 'Confirmed', 'Deaths', 'Latitude', 'Longitude']]

# Extract a subset with only the latest date
df_latest = pd.DataFrame(columns=list(df.columns))
for country in df['Region'].unique():
    df_latest = pd.concat([df_latest, df[df['Region'] == country].iloc[-1:]])

# Save dataset in CSV format into output folder
df.to_csv(ROOT / 'output' / 'china.csv', index=False)
df_latest.to_csv(ROOT / 'output' / 'china_latest.csv', index=False)

# Save dataset in JSON format into output folder
df.to_json(ROOT / 'output' / 'china.json', orient='records')
df_latest.to_json(ROOT / 'output' / 'china_latest.json', orient='records')
