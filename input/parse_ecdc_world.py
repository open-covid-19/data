#!/usr/bin/env python

'''
This script loads the XLS file from argument and aggregates the data. It also
extracts the last day of data into its own dataset.
'''

import os
import sys
import pandas as pd
from io import BytesIO
from pathlib import Path
import tempfile

from utils import dataframe_output

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read XLS file from stdin
df = pd.read_excel(BytesIO(sys.stdin.buffer.read())).sort_values(['DateRep', 'GeoId'])

# Ensure date field is used as a string
df['DateRep'] = df['DateRep'].astype(str)

# Workaround for https://github.com/open-covid-19/data/issues/8
# ECDC mistakenly labels Greece country code as EL instead of GR
df['GeoId'] = df['GeoId'].apply(lambda code: 'GR' if code == 'EL' else code)

# Workaround for https://github.com/open-covid-19/data/issues/13
# ECDC mistakenly labels Greece country code as UK instead of GB
df['GeoId'] = df['GeoId'].apply(lambda code: 'GB' if code == 'UK' else code)

# Workaround for https://github.com/open-covid-19/data/issues/12
# ECDC data for Italy is simply wrong, so Italy's data will be parsed from a different source
# ECDC data for Spain is two days delayed because original reporting time mismatch, parse separately
df = df[df['GeoId'] != 'ES']
df = df[df['GeoId'] != 'IT']

# Compute the cumsum of values
columns = ['DateRep', 'GeoId', 'Confirmed', 'Deaths']
df_ = pd.DataFrame(columns=columns)
for country in df['GeoId'].unique():
    subset = df[df['GeoId'] == country].copy()
    subset['Confirmed'] = subset['Cases'].cumsum()
    subset['Deaths'] = subset['Deaths'].cumsum()
    df_ = pd.concat([df_, subset[columns]])

df_ = df_[columns]
df_.columns = ['Date', 'CountryCode', 'Confirmed', 'Deaths']
df = df_

# Make sure all data types are appropriately casted
df['Confirmed'] = df['Confirmed'].fillna(0).astype(int)
df['Deaths'] = df['Deaths'].fillna(0).astype(int)

# Output the results
df['RegionCode'] = None
dataframe_output(df, ROOT)