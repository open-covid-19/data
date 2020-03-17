#!/usr/bin/env python

'''
This script loads the latest CSV from github.com/BlankerL/DXY-COVID-19-Dataand
and extracts the confirmed cases and deaths and for each region. The output is 
saved both in CSV and JSON format under the `output` folder.

Credit to the github.com/BlankerL team for scraping the data from DXY.cn.
'''

import os
import datetime
import pandas as pd
from pathlib import Path
import requests

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read DXY CSV file from  website
df = pd.read_csv('https://raw.githubusercontent.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYArea.csv')

# Since all other reporting is done at 10 AM GMT+1, adjust for timezone difference
def timezone_adjust(time: str):
    ''' Adjust 7 hour difference between China's GMT+8 and GMT+1 '''
    timestamp = datetime.datetime.fromisoformat(time)
    if timestamp.hour <= 24 - 7:
        return timestamp.date().isoformat()
    else:
        return (timestamp + datetime.timedelta(days=1)).date().isoformat()

df['Date'] = df['updateTime'].apply(timezone_adjust)

# Rename the appropriate columns
df = df.rename(columns={
    'countryEnglishName': 'CountryName',
    'provinceEnglishName': 'Region',
    'province_confirmedCount': 'Confirmed', 
    'province_deadCount': 'Deaths', 
    'province_curedCount': 'Recovered'
})

# This is time series data, get only the last snapshot of each day
df = df.sort_values('updateTime').groupby(['Date', 'CountryName', 'Region']).last().reset_index()

# Get the coordinates for each region
df = df[df['CountryName'] == 'China'].merge(
    pd.read_csv(ROOT / 'input' / 'china_regions.csv', dtype=str), on='Region')

# Sort dataset by date + region
df = df.sort_values(['Date', 'Region'])
df = df[[
    'Date', 
    'Region', 
    'CountryCode', 
    'CountryName', 
    'Confirmed', 
    'Deaths', 
    # 'Recovered', # Considered unreliable data
    'Latitude', 
    'Longitude'
]]

# Extract a subset with only the latest date
df_latest = pd.DataFrame(columns=list(df.columns))
for region in sorted(df['Region'].unique()):
    df_latest = pd.concat([df_latest, df[df['Region'] == region].iloc[-1:]])

# Save dataset in CSV format into output folder
df.to_csv(ROOT / 'output' / 'china.csv', index=False)
df_latest.to_csv(ROOT / 'output' / 'china_latest.csv', index=False)

# Save dataset in JSON format into output folder
df.to_json(ROOT / 'output' / 'china.json', orient='records')
df_latest.to_json(ROOT / 'output' / 'china_latest.json', orient='records')