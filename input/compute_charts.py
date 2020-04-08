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

# Read data from the open COVID-19 dataset
df_data = pd.read_csv(ROOT / 'output' / 'data_minimal.csv')
df_forecast = pd.read_csv(ROOT / 'output' / 'data_forecast.csv')[['Date', 'Key', 'Estimated']]

# Loop through each unique combination of country / region
chart_outputs = {}
charts_root = ROOT / 'output' / 'charts'
for key in tqdm(df_data['Key'].unique()):

    # Filter dataset
    cols = ['Key', 'Date', 'Deaths', 'Confirmed']
    # Get data only for the selected country / region
    subset = df_data[df_data['Key'] == key][cols]
    # Early exit: no forecast found
    if not len(subset): continue

    # Used for naming the output files
    prefix = subset.loc[~subset['Confirmed'].isna(), 'Date'].iloc[-1]

    # Sometimes our data appears to have duplicate values for specific cases, work around that
    subset = subset.set_index('Date').query('~index.duplicated()')

    fname_forecast = ('%s_%s_forecast.svg' % (prefix, key))
    fname_confirmed = ('%s_%s_confirmed.svg' % (prefix, key))
    fname_deaths = ('%s_%s_deaths.svg' % (prefix, key))

    # Store the charts in a helper JSON file
    chart_outputs[key] = {}

    confirmed = subset['Confirmed'].dropna()
    if len(confirmed) > 0:
        plot_column(charts_root / fname_confirmed, confirmed)
        chart_outputs[key]['Confirmed'] = fname_confirmed

    deaths = subset['Deaths'].dropna()
    if len(deaths) > 0:
        plot_column(charts_root / fname_deaths, deaths)
        chart_outputs[key]['Deaths'] = fname_deaths

    try:
        # Get the estimated cases from the forecast
        estimated = df_forecast[df_forecast['Key'] == key].set_index(['Date'])['Estimated']
        if len(estimated) == 0: continue

        # Line up the indices for both the estimated and confirmed cases
        estimated = estimated[estimated.index >= confirmed.index[0]].dropna()
        confirmed = confirmed[confirmed.index >= estimated.index[0]].dropna()
        if len(estimated) > len(confirmed):
            plot_forecast(charts_root / fname_forecast, confirmed, estimated)
            chart_outputs[key]['Forecast'] = fname_forecast
    except Exception as exc:
        print('Unexpected error: %r' % sys.exc_info()[0], file=sys.stderr)
        print(subset, file=sys.stderr)

# Output the chart map to stdout
json.dump(chart_outputs, sys.stdout)