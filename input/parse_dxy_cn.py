#!/usr/bin/env python

'''
This script loads the latest CSV from github.com/BlankerL/DXY-COVID-19-Dataand
and extracts the confirmed cases and deaths and for each region.

Credit to the github.com/BlankerL team for scraping the data from DXY.cn.
'''

from utils import github_raw_dataframe, dataframe_output, timezone_adjust


# Read DXY CSV file from  website
df = github_raw_dataframe('BlankerL/DXY-COVID-19-Data', 'csv/DXYArea.csv')

# Adjust 7 hour difference between China's GMT+8 and GMT+1
df['Date'] = df['updateTime'].apply(lambda date: timezone_adjust(date, 7))

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
dataframe_output(df, 'CN')