#!/usr/bin/env python

import sys
from covid_io import read_argv
from utils import dataframe_output, datetime_isoformat


# Retrieve the CSV files from https://covid19.isciii.es
df = read_argv(encoding='ISO-8859-1').rename(columns={
    'FECHA': 'Date',
    'CCAA': 'RegionCode',
    'Fallecidos': 'Deaths'
}).dropna(subset=['Date'])

# Add the country code to all records
df['CountryCode'] = 'ES'

# Confirmed cases are split across 3 columns
confirmed_columns = ['CASOS', 'PCR+']
for col in confirmed_columns:
    df[col] = df[col].fillna(0)
df['Confirmed'] = df.apply(lambda x: sum([x[col] for col in confirmed_columns]), axis=1)

# Convert dates to ISO format
df['Date'] = df['Date'].apply(lambda date: datetime_isoformat(date, '%d/%m/%Y'))

# Country-wide is the sum of all regions
region_level = df
country_level = df.groupby(['Date', 'CountryCode']).sum().reset_index()

# Output the results
dataframe_output(country_level)
dataframe_output(region_level, 'ES')
