#!/usr/bin/env python

import os
import sys
import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from utils import \
    compute_record_key, get_outbreak_mask, compute_forecast, series_converter, read_csv

# Establish root of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Parse parameters
PREDICT_WINDOW = 3
DATAPOINT_COUNT = 14

# Read data from the open COVID-19 dataset
df = read_csv(ROOT / 'output' / 'data_minimal.csv').set_index('Date')

# Create the output dataframe ahead, we will fill it one row at a time
forecast_columns = ['ForecastDate', 'Date', 'Key', 'Estimated', 'Confirmed']
df_forecast = pd.DataFrame(columns=forecast_columns).set_index(['Date', 'Key'])

# Loop through each unique combination of country / region
for key in tqdm(df['Key'].unique()):

    # Filter dataset
    cols = ['Key', 'Confirmed']
    # Get data only for the selected country / region
    subset = df[df['Key'] == key][cols]
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
        df_forecast.loc[(idx, key), 'ForecastDate'] = forecast_date
        df_forecast.loc[(idx, key), 'Estimated'] = '%.03f' % forecast_data.loc[idx]
        if idx in subset.index:
            df_forecast.loc[(idx, key), 'Confirmed'] = int(subset.loc[idx, 'Confirmed'])

# Do data cleanup here
data = df_forecast.reset_index()
forecast_columns = ['ForecastDate', 'Date', 'Key', 'Estimated', 'Confirmed']
data = data.sort_values(['Key', 'Date'])[forecast_columns]

# Make sure the core columns have the right data type
for col in data.columns: data[col] = series_converter(data[col])

# Output resulting dataframe
data.to_csv(sys.stdout, index=False)
