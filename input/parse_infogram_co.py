#!/usr/bin/env python

import re
import json
from datetime import datetime

import requests
from pandas import DataFrame

from utils import dataframe_output


# Get JSON file from the infogram API
url_data = 'https://e.infogram.com/api/live/flex/bc384047-e71c-47d9-b606-1eb6a29962e3/664bc407-2569-4ab8-b7fb-9deb668ddb7a'
data = json.loads(requests.get(url_data).text)['data'][0]
df = DataFrame.from_records(data)
df.columns = df.iloc[0]
df = df.drop(df.index[0]).rename(columns={
    'Fecha de diagnóstico': 'Date',
    'Departamento o Distrito': 'RegionName',
    'Atención**': 'Event'
})

# Convert date to ISO format
df['Date'] = df['Date'].apply(lambda date: datetime.strptime(date, '%d/%m/%Y'))
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())

# Remove unnecessary columns
df = df[['Date', 'RegionName', 'Event']]

# Correct bogus department names
df['RegionName'] = df['RegionName'].apply(lambda x: re.sub(r'\.', '', x))
df['RegionName'] = df['RegionName'].apply(lambda x: re.sub(r'y C', '', x))
df['RegionName'] = df['RegionName'].apply(lambda x: re.sub(r'(DE)|(DC)|(DT)', '', x))
df['RegionName'] = df['RegionName'].apply(lambda x: x.strip())
df.loc[df['RegionName'] == 'Barranquilla', 'RegionName'] = 'Atlántico'
df.loc[df['RegionName'] == 'Cartagena', 'RegionName'] = 'Bolívar'
df.loc[df['RegionName'] == 'Santa Marta', 'RegionName'] = 'Magdalena'
df.loc[df['RegionName'] == 'Valle del cauca', 'RegionName'] = 'Valle del Cauca'

# Aggregate data by department
keys = ['RegionName', 'Date']
confirmed = df.groupby(keys).count().groupby(level=0).cumsum()
deaths = df[df['Event'] == 'Fallecido'].groupby(keys).count().groupby(level=0).cumsum()

# Make sure that every region-date combination has at least one value
for region in df['RegionName'].unique():
    for date in df['Date'].unique():
        key = (region, date)
        for df_ in (confirmed, deaths):
            if key not in df_.index: df_.loc[key, 'Event'] = 0

# Put all data in the same dataframe
confirmed = confirmed.rename(columns={'Event': 'Confirmed'})
deaths = deaths.rename(columns={'Event': 'Deaths'})
df = confirmed.join(deaths).fillna(0).reset_index().sort_values(['Date', 'RegionName'])

dataframe_output(df, 'CO')