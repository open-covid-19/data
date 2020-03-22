#!/usr/bin/env python

'''
This script loads the latest CSV from github.com/BlankerL/DXY-COVID-19-Dataand
and extracts the confirmed cases and deaths and for each region.

Credit to the github.com/BlankerL team for scraping the data from DXY.cn.
'''

import os
import datetime
import pandas as pd
from pathlib import Path
import requests

from utils import dataframe_output

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
    'provinceEnglishName': 'RegionName',
    'province_confirmedCount': 'Confirmed',
    'province_deadCount': 'Deaths',
    'province_curedCount': 'Recovered'
})

# Filter China data only
df = df[df['CountryName'] == 'China']

# This is time series data, get only the last snapshot of each day
df = df.sort_values('updateTime').groupby(['Date', 'CountryName', 'RegionName']).last().reset_index()

# Output the results
dataframe_output(df, ROOT, 'cn')