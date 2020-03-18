#!/usr/bin/env python

'''
This script loads the latest CSV from github.com/BlankerL/DXY-COVID-19-Dataand
and extracts the confirmed cases and deaths and for each region. The output is
saved both in CSV and JSON format under the `output` folder.

Credit to the github.com/BlankerL team for scraping the data from DXY.cn.
'''

import os
import sys
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
    'province_confirmedCount': 'Confirmed',
    'province_deadCount': 'Deaths',
    'province_curedCount': 'Recovered'
})

# This is time series data, get only the last snapshot of each day
df = df.sort_values('updateTime').groupby(['Date', 'CountryName']).last().reset_index()

# Get the metadata for each country
countries = pd.read_csv(ROOT / 'input' / 'country_coordinates.csv', dtype=str)

# Get the country name of the country code provided as parameter
country_name = countries.set_index('CountryCode').loc[sys.argv[1], 'CountryName']

# Merge country metadata with the stats from DXY
df = df[df['CountryName'] == country_name].merge(countries, on='CountryName')

# Merge with the rest of the world's data
df = pd.concat([df, pd.read_csv(ROOT / 'output' / 'world.csv', dtype=str)], sort=False)

# Fill all of Italy's missing data where numbers did not change
ffill_columns = ('Confirmed', 'Deaths')
first_date = df['Date'].sort_values().iloc[0]
sample_row = df[df['CountryCode'] == 'IT'].iloc[0]
last_values = {'Confirmed': 0, 'Deaths': 0}
for date in sorted(df['Date'].unique()):
    new_row = sample_row.copy()
    new_row['Date'] = date
    existing_rows = df[(df['Date'] == date) & (df['CountryCode'] == 'IT')]
    if len(existing_rows) > 0:
        for ffill_col in ffill_columns:
            last_values[ffill_col] = existing_rows.iloc[0][ffill_col]
        continue

    for ffill_col in ffill_columns:
        new_row[ffill_col] = last_values[ffill_col]
       
    df = df.append(new_row, ignore_index=True)
df = df.sort_values(['Date', 'CountryCode'])
for ffill_col in ffill_columns: df[ffill_col] = df[ffill_col].ffill()

# Sort dataset by date + country
df = df.sort_values(['Date', 'CountryCode'])
df = df[[
    'Date',
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
for country in sorted(df['CountryCode'].unique()):
    df_latest = pd.concat([df_latest, df[df['CountryCode'] == country].iloc[-1:]])

# Save dataset in CSV format into output folder
df.to_csv(ROOT / 'output' / 'world.csv', index=False)
df_latest.to_csv(ROOT / 'output' / 'world_latest.csv', index=False)

# Save dataset in JSON format into output folder
df.to_json(ROOT / 'output' / 'world.json', orient='records')
df_latest.to_json(ROOT / 'output' / 'world_latest.json', orient='records')