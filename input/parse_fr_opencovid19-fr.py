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

from covid_io import read_argv
from utils import dataframe_output

# Read CSV file from GitHub project
df = read_argv()
# https://raw.github.com/opencovid19-fr/data/master/dist/chiffres-cles.csv

# Rename the appropriate columns
df = df.rename(columns={'date': 'Date', 'cas_confirmes': 'Confirmed', 'deces': 'Deaths'})

# Filter rows to those that are region-level
df = df[df['granularite'] == 'region']
df['RegionName'] = df['maille_nom']

# There may be more than one row per region if it has multiple sources
g = df[['Date', 'RegionName', 'Confirmed', 'Deaths']].groupby(['Date', 'RegionName'])

# Retrieve the last non-null value per group
def last_non_null(rows):
    for row in rows:
        if not isna(row): return row
    return rows.iloc[-1]
df = g.agg(last_non_null).reset_index()

# Output the results
dataframe_output(df, 'FR')