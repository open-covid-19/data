#!/usr/bin/env python

import os
import sys
import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from utils import get_outbreak_mask, compute_forecast

# Establish root of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Parse parameters
PREDICT_WINDOW = 3
DATAPOINT_COUNT = 14

# Read data from the open COVID-19 dataset
df = pd.read_csv(ROOT / 'output' / 'data.csv', dtype=str)
df['Confirmed'] = df['Confirmed'].astype(float)
df['Deaths'] = df['Deaths'].astype(float)
df = df.set_index('Date')

# Create the output dataframe ahead, we will fill it one row at a time
pivot_columns = ['CountryCode', 'CountryName', 'RegionCode', 'RegionName']
df['_key'] = pd.Series([''.join([str(row[col]) for col in pivot_columns])
                        for _, row in df.iterrows()], index=df.index, dtype='O')
forecast_columns = ['_key', 'ForecastDate', 'Date'] + ['Estimated', 'Confirmed']
df_forecast = pd.DataFrame(columns=forecast_columns).set_index(['_key', 'Date'])

# Build a dataframe used to retrieve back the non-essential columns
df_merge = []
for key in df['_key'].unique():
    df_ = df[df['_key'] == key]
    df_merge.append({'_key': key, **{col: df_[col].iloc[0] or '' for col in pivot_columns}})
df_merge = pd.DataFrame.from_records(df_merge).set_index('_key')

# Loop through each unique combination of country / region
for key in tqdm(df['_key'].unique()):

    # Filter dataset
    cols = ['_key', 'Confirmed']
    # Get data only for the selected country / region
    subset = df[df['_key'] == key][cols]
    # Get data only after the outbreak begun
    subset = subset[get_outbreak_mask(subset)]
    # Early exit: no outbreak found
    if not len(subset): continue
    # Get a list of dates for existing data
    date_range = map(
        lambda datetime: datetime.date().isoformat(),
        pd.date_range(subset.index[0], subset.index[-1])
    )

    # Forecast date is equal to the date of the last known datapoint, unless manually supplied
    forecast_date = subset.index[-1]
    subset = subset[subset.index <= forecast_date].sort_index()

    # Sometimes our data appears to have duplicate values for specific cases, work around that
    subset = subset.query('~index.duplicated()')

    # Early exit: If there are less than DATAPOINT_COUNT output datapoints
    if len(subset) < DATAPOINT_COUNT - PREDICT_WINDOW: continue

    # Perform forecast
    forecast_data = compute_forecast(subset['Confirmed'], PREDICT_WINDOW)

    # Capture only the last DATAPOINT_COUNT days
    forecast_data = forecast_data.sort_index().iloc[-DATAPOINT_COUNT:]

    # Fill out the corresponding index in the output forecast
    for idx in forecast_data.index:
        df_forecast.loc[(key, idx), 'ForecastDate'] = forecast_date
        df_forecast.loc[(key, idx), 'Estimated'] = '%.03f' % forecast_data.loc[idx]
        if idx in subset.index:
            df_forecast.loc[(key, idx), 'Confirmed'] = int(subset.loc[idx, 'Confirmed'])

# Merge back with original data to get the rest of the columns
df_forecast = df_forecast.reset_index().set_index('_key').join(df_merge, how='left')

# Do data cleanup here
data = df_forecast.reset_index()
forecast_columns = ['ForecastDate', 'Date'] + pivot_columns + ['Estimated', 'Confirmed']
data = data.sort_values(['_key', 'Date'])[forecast_columns]

# Make sure the core columns have the right data type
data['ForecastDate'] = data['ForecastDate'].astype(str)
data['Date'] = data['Date'].astype(str)
data['Estimated'] = data['Estimated'].astype(float)
data['Confirmed'] = data['Confirmed'].astype(float).astype('Int64')
for pivot_column in pivot_columns:
    data[pivot_column] = data[pivot_column].fillna('').astype(str)

# Output resulting dataframe
data.to_csv(sys.stdout, index=False)
