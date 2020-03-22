#!/usr/bin/env python

import os
import sys
import json
import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from utils import plot_forecast, plot_column

# Establish root of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

pivot_columns = ['CountryCode', 'CountryName', 'RegionCode', 'RegionName']

# Read data from the open COVID-19 dataset
df = pd.read_csv(ROOT / 'output' / 'data.csv')
df['Confirmed'] = df['Confirmed'].astype('Int64')
df['Deaths'] = df['Deaths'].astype('Int64')

forecast = pd.read_csv(ROOT / 'output' / 'data_forecast.csv')

# Create the output dataframe ahead, we will fill it one row at a time
make_key = lambda df: pd.Series([''.join([str(row[col] or '') for col in ('CountryCode', 'RegionCode')])
                                for _, row in df.iterrows()], index=df.index, dtype='O')
df['_key'] = make_key(df)
forecast['_key'] = make_key(forecast)

# Build a dataframe used to retrieve back the non-essential columns
df_merge = []
for key in df['_key'].unique():
    df_ = df[df['_key'] == key]
    df_merge.append({'_key': key, **{col: df_[col].iloc[0] or '' for col in pivot_columns}})
df_merge = pd.DataFrame.from_records(df_merge).set_index('_key')

# Loop through each unique combination of country / region
chart_outputs = {}
charts_root = ROOT / 'output' / 'charts'
for key in tqdm(df['_key'].unique()):

    # Filter dataset
    cols = ['_key', 'CountryCode', 'RegionCode', 'Date', 'Deaths', 'Confirmed']
    # Get data only for the selected country / region
    subset = df[df['_key'] == key][cols].set_index('Date')
    # Early exit: no forecast found
    if not len(subset): continue

    # Get rid of unnecessary columns to make merging easier
    region = subset['RegionCode'].fillna('').iloc[0]
    country = subset['CountryCode'].fillna('').iloc[0]
    subset = subset.reset_index()[['_key', 'Date', 'Deaths', 'Confirmed']]

    # Used for naming the output files
    suffix = country
    if region: suffix += '_%s' % region
    prefix = subset.loc[~subset['Confirmed'].isna(), 'Date'].iloc[-1]

    # Sometimes our data appears to have duplicate values for specific cases, work around that
    subset = subset.set_index('Date').query('~index.duplicated()')

    fname_forecast = ('%s_%s_forecast.svg' % (prefix, suffix))
    fname_confirmed = ('%s_%s_confirmed.svg' % (prefix, suffix))
    fname_deaths = ('%s_%s_deaths.svg' % (prefix, suffix))

    # Store the charts in a helper JSON file
    chart_outputs[suffix] = {}

    confirmed = subset['Confirmed'].dropna()
    if len(confirmed) > 0:
        plot_column(charts_root / fname_confirmed, confirmed)
        chart_outputs[suffix]['Confirmed'] = fname_confirmed

    deaths = subset['Deaths'].dropna()
    if len(deaths) > 0:
        plot_column(charts_root / fname_deaths, deaths)
        chart_outputs[suffix]['Deaths'] = fname_deaths

    try:
        # Get the estimated confirmed cases from the forecast
        estimated = forecast[forecast['_key'] == key].set_index(['Date'])['Estimated']
        if len(estimated) == 0: continue

        # Line up the indices for both the estimated and confirmed cases
        estimated = estimated[estimated.index >= confirmed.index[0]].dropna()
        confirmed = confirmed[confirmed.index >= estimated.index[0]].dropna()
        if len(estimated) > len(confirmed):
            plot_forecast(charts_root / fname_forecast, confirmed, estimated)
            chart_outputs[suffix]['Forecast'] = fname_forecast
    except Exception as exc:
        print('Unexpected error:', sys.exc_info()[0])
        print(subset)

with open(charts_root / 'map.json', 'w') as f:
    json.dump(chart_outputs, f)