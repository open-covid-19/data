#!/usr/bin/env python

import os
import sys
from pathlib import Path

import pandas as pd

# Establish root of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read data from the open COVID-19 dataset
data = pd.read_csv(ROOT / 'output' / 'data.csv', dtype=str)
data['Confirmed'] = data['Confirmed'].astype(float)
data['Deaths'] = data['Deaths'].astype(float)

# Approximate numbers from early Chinese data
severe_ratio = .15
critical_ratio = .05
mild_ratio = 1 - severe_ratio - critical_ratio

# Estimated numbers from early Chinese data + western reports
mild_recovery_days = 7
severe_recovery_days = 12
critical_recovery_days = 15

# Make sure header is printed only once
print_header_flag = True

# We must do this for every combination of country-region
keys = data.apply(lambda row: (row['CountryCode'], row['RegionCode']), axis=1).unique()
for country_code, region_code in keys:

    # Extract only the data for this key
    df = data[data['CountryCode'] == country_code]
    if pd.isna(region_code):
        df = df[pd.isnull(df['RegionCode'])]
    else:
        df = df[df['RegionCode'] == region_code]
    df = df.set_index(['Date', 'CountryCode', 'CountryName', 'RegionCode', 'RegionName'])

    # Estimate daily counts per category assuming ratio is constant
    df['NewCases'] = df['Confirmed'].diff().astype('Int64')
    df['NewDeaths'] = df['Deaths'].diff().astype('Int64')
    df['NewMild'] = df['NewCases'] * mild_ratio
    df['NewSevere'] = df['NewCases'] * severe_ratio
    df['NewCritical'] = df['NewCases'] * critical_ratio
    df = df[['NewCases', 'NewDeaths', 'NewMild', 'NewSevere', 'NewCritical']]

    # Compute the rolling windows for count of active (current) categories
    df['CurrentlyMild'] = df['NewMild'].rolling(round(mild_recovery_days)).sum()
    df['CurrentlySevere'] = df['NewSevere'].rolling(round(severe_recovery_days)).sum()
    df['CurrentlyCritical'] = df['NewCritical'].rolling(round(critical_recovery_days)).sum()

    # Get rid of the first columns which are useless because of the windowing function
    df = df.iloc[max(mild_recovery_days, severe_recovery_days, critical_recovery_days):]

    # Make sure all columns have the appropriate type
    for col in df.columns[2:]:
        df[col] = df[col].apply(lambda x: '' if pd.isna(x) else '%.2f' % x)

    # Output resulting dataframe
    if print_header_flag:
        df.to_csv(sys.stdout)
        print_header_flag = False
    else:
        df.to_csv(sys.stdout, header=None)