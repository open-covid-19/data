#!/usr/bin/env python

'''
This script loads the XLS file from argument and aggregates the data. It also
extracts the last day of data into its own dataset.
'''

import sys
from io import BytesIO
from datetime import datetime
import pandas as pd
from utils import dataframe_output


# Read CSV file from URL
df = pd.read_csv('https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/', encoding='utf-8')

# Ensure date field is used as a string
df['dateRep'] = df['dateRep'].astype(str)

# Workaround for https://github.com/open-covid-19/data/issues/8
# ECDC mistakenly labels Greece country code as EL instead of GR
df['geoId'] = df['geoId'].apply(lambda code: 'GR' if code == 'EL' else code)

# Workaround for https://github.com/open-covid-19/data/issues/13
# ECDC mistakenly labels Greece country code as UK instead of GB
df['geoId'] = df['geoId'].apply(lambda code: 'GB' if code == 'UK' else code)

# Workaround for https://github.com/open-covid-19/data/issues/12
# ECDC data for Italy is simply wrong, so Italy's data will be parsed from a different source
# ECDC data for Spain is two days delayed because original reporting time mismatch, parse separately
df = df[df['geoId'] != 'ES']
df = df[df['geoId'] != 'IT']

# Compute the cumsum of values
columns = ['Date', 'CountryCode', 'Confirmed', 'Deaths']
df_ = pd.DataFrame(columns=columns)
for country in df['geoId'].unique():
    subset = df[df['geoId'] == country].copy()
    subset['CountryCode'] = subset['geoId']
    subset['Date'] = subset['dateRep'].apply(
        lambda date: datetime.strptime(date, '%d/%m/%Y').date().isoformat())
    subset = subset.sort_values('Date')
    subset['Confirmed'] = subset['cases'].cumsum()
    subset['Deaths'] = subset['deaths'].cumsum()
    df_ = pd.concat([df_, subset[columns]])
df = df_

# Make sure all data types are appropriately casted
df['Confirmed'] = df['Confirmed'].fillna(0).astype(int)
df['Deaths'] = df['Deaths'].fillna(0).astype(int)

# Output the results
df['RegionCode'] = None
dataframe_output(df)