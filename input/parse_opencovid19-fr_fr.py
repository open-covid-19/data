#!/usr/bin/env python

'''
This script loads the latest CSV from
https://github.com/opencovid19-fr/data and extracts the confirmed
cases, deaths and total tests for each subregion in France.

Credit to opencovid19-fr team for aggregating the data.
'''

import os
import sys
from pathlib import Path
from datetime import datetime

from numpy import unique
from pandas import DataFrame, isna

from utils import github_raw_dataframe, dataframe_output

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read CSV file from GitHub project
df = github_raw_dataframe('opencovid19-fr/data', 'dist/chiffres-cles.csv')

# Rename the appropriate columns
df = df.rename(columns={'date': 'Date', 'cas_confirmes': 'Confirmed', 'deces': 'Deaths'})

# Filter rows to those that are region-level
df = df[df['granularite'] == 'region']
df['_RegionLabel'] = df['maille_nom']

# There may be more than one row per region if it has multiple sources
g = df[['Date', '_RegionLabel', 'Confirmed', 'Deaths']].groupby(['Date', '_RegionLabel'])

# Retrieve the first non-null value per group
def first_non_null(rows):
    for row in rows:
        if not isna(row): return row
    return rows.iloc[0]
df = g.agg(first_non_null).reset_index()

# Output the results
dataframe_output(df, ROOT, 'fr')