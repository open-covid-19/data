#!/usr/bin/env python

import os
import re
import sys
import datetime
from pathlib import Path
import pandas as pd

from utils import dataframe_output, merge_previous

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Get a list of the valid Spain regions
regions = pd.read_csv(ROOT / 'input' / 'metadata_es.csv', dtype=str)
region_list = regions['_RegionLabel'].unique()

# Default column index for deaths, but we'll try to infer it from report
deaths_column_index = -3
deaths_column_name = 'Fallecidos'

def parse_record(tokens: list):
    return [{
        '_RegionLabel': tokens[0],
        'Confirmed': re.sub(r'\D', '', tokens[1]),
        'Deaths': re.sub(r'\D', '', tokens[deaths_column_index])
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
    date_regex = r'^(\d\d?\.\d\d?\.\d\d).*$'
    if date is None and re.match(date_regex, line):
        date = datetime.datetime.strptime(re.match(date_regex, line).group(1), '%d.%m.%y')
        date = date.date().isoformat()

    # Assume columns are separated by at least 4 spaces
    line = re.sub('\s\s\s+', '\t', line)
    tokens = line.split('\t')

    # Filter out lines with less than 5 columns
    if len(tokens) < 5: continue

    # Find the marker for the appropriate table
    if tokens[0] == 'CCAA':
        table_marker = True
        if deaths_column_name in tokens:
            deaths_column_index = tokens.index(deaths_column_name)
        continue

    # Exit once the end of the table is reached
    if tokens[0] == 'Total' or tokens[0] == 'ESPAÑA' and table_marker:
        break

    # Only process tokens from known region
    if tokens[0] in region_list:
        records += parse_record(tokens)

    # Exit if we have covered all regions
    if len(records) == len(region_list):
        break

# Early exit: no records in the report (2020-03-16 onwards)
if not records:
    print('No records from region found in report')
    sys.exit(1)

# Put resulting records into a dataframe
df = pd.DataFrame.from_records(records).merge(regions, on='_RegionLabel')
df['Date'] = date

# Merge the new data with the existing data (prefer new data if duplicates)
filter_function = lambda row: row['CountryCode'] == 'ES' and not pd.isna(row['RegionCode'])
df = merge_previous(df, ['Date', 'RegionCode'], filter_function)

# Only keep the necessary columns prior to merging with metadata
df = df[['Date', 'RegionCode', 'Confirmed', 'Deaths']]

# Output the results
dataframe_output(df, ROOT, 'es')