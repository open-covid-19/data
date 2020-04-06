#!/usr/bin/env python

'''
This script loads the latest CSV from github.com/BlankerL/DXY-COVID-19-Dataand
and extracts the confirmed cases and deaths and for each region.

Credit to the github.com/BlankerL team for scraping the data from DXY.cn.
'''

import os
import sys
import datetime
from pathlib import Path
import pandas as pd
from utils import github_raw_dataframe, dataframe_output, timezone_adjust, ROOT

# This script requires country code as parameter
country_code = sys.argv[1]
assert country_code is not None
FIRST_DATE = '2019-12-31'

# Read DXY CSV file from  website
df = github_raw_dataframe('BlankerL/DXY-COVID-19-Data', 'csv/DXYArea.csv')

# Adjust 7 hour difference between China's GMT+8 and GMT+1 (ECDC report time)
df['Date'] = df['updateTime'].apply(lambda date: timezone_adjust(date, 7))

# Rename the appropriate columns
df = df.rename(columns={
    'countryEnglishName': 'CountryName',
    'provinceEnglishName': 'RegionName',
    'province_confirmedCount': 'Confirmed',
    'province_deadCount': 'Deaths',
    'province_curedCount': 'Recovered'
})

# This is time series data, get only the last snapshot of each day
df = df.sort_values('updateTime').groupby(['Date', 'CountryName']).last().reset_index()

# Get the metadata for each country
countries = pd.read_csv(ROOT / 'input' / 'metadata.csv', dtype=str)

# Get the country name of the country code provided as parameter
country_name = countries.set_index('CountryCode').loc[country_code, 'CountryName'].iloc[0]

# Merge country metadata with the stats from DXY
df = df[df['CountryName'] == country_name].merge(countries, on=['CountryName', 'RegionName'])

# Create a dummy record to be inserted where there is missing data
sample_record = df.iloc[0].copy()
sample_record['Confirmed'] = None
sample_record['Deaths'] = None

# Loop through all the dates, which must be unique in the dataset index and fill data
date_range = pd.date_range(FIRST_DATE, df['Date'].max())
date_range = [date.date().isoformat() for date in date_range]

# Backfill the first date with a zero
if FIRST_DATE not in df['Date'].values:
    df = df.set_index('Date')
    df.loc[FIRST_DATE, 'Confirmed'] = 0
    df.loc[FIRST_DATE, 'Deaths'] = 0
    df = df.reset_index()

# Fill all of country's missing data where numbers did not change
for date in [date for date in date_range if date not in df['Date'].values]:
    inserted_record = sample_record.copy()
    inserted_record['Date'] = date
    df = df.append(inserted_record, ignore_index=True)

df = df.reset_index().sort_values('Date')

# Output the results
dataframe_output(df, country_code)