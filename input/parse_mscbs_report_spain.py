import os
import re
import sys
import datetime
from pathlib import Path
import pandas as pd

from utils import dataframe_to_json

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Get a list of the valid Spain regions
regions = pd.read_csv(ROOT / 'input' / 'spain_regions.csv', dtype=str)
region_list = regions['RegionLabel'].unique()

def parse_record(tokens: list):
    return [{
        'RegionLabel': tokens[0],
        'Confirmed': tokens[1].replace('.', ''),
        'Deaths': tokens[-1].replace('.', '')
    }]

# We will get the date from the report itself
date = None

records = []
table_marker = False
for line in sys.stdin:

    # Remove whitespace around lines
    line = line.strip()

    # Filter out empty lines
    if not line: continue

    # Search for the date of the report
    date_regex = r'(\d\d?\.\d\d?\.\d\d\d\d)'
    if date is None and re.match(date_regex, line):
        date = datetime.datetime.strptime(re.match(date_regex, line).group(1), '%d.%m.%Y')
        date = date.date().isoformat()

    # Assume columns are separated by at least 4 spaces
    line = re.sub('\s\s\s+', '\t', line)
    tokens = line.split('\t')

    # Filter out lines with less than 5 columns
    if len(tokens) < 5: continue

    # Find the marker for the appropriate table
    if tokens[0] == 'CCAA':
        table_marker = True
        continue

    # Exit once the end of the table is reached
    if tokens[0] == 'Total' and table_marker:
        break

    # Only process tokens from known region
    if tokens[0] in region_list:
        records += parse_record(tokens)

# Early exit: no records in the report (2020-03-16 onwards)
if not records:
    print('No records from region found in report')
    sys.exit(1)

# Put resulting records into a dataframe
df = pd.DataFrame.from_records(records).merge(regions, on='RegionLabel')
df['Date'] = date

# Merge the new data with the existing data
df = pd.concat([df, pd.read_csv(ROOT / 'output' / 'spain.csv', dtype=str)], sort=False).drop_duplicates()

# Sort dataset by date + region
df = df.sort_values(['Date', 'Region'])
df = df[['Date', 'Region', 'CountryCode', 'CountryName', 'Confirmed', 'Deaths', 'Latitude', 'Longitude']]

# Extract a subset with only the latest date
df_latest = pd.DataFrame(columns=list(df.columns))
for region in sorted(df['Region'].unique()):
    df_latest = pd.concat([df_latest, df[df['Region'] == region].iloc[-1:]])

# Save dataset in CSV format into output folder
df.to_csv(ROOT / 'output' / 'spain.csv', index=False)
df_latest.to_csv(ROOT / 'output' / 'spain_latest.csv', index=False)

# Save dataset in JSON format into output folder
dataframe_to_json(df, ROOT / 'output' / 'spain.json', orient='records')
dataframe_to_json(df_latest, ROOT / 'output' / 'spain_latest.json', orient='records')
