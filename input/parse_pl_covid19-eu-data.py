#!/usr/bin/env python

from datetime import datetime
from pandas import DataFrame
from covid_io import read_argv
from utils import dataframe_output


# Get CSV file from Github
df = read_argv()
# https://raw.github.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-pl.csv
df = df.rename(columns={'datetime': 'Date', 'nuts_2': 'RegionName', 'cases': 'Confirmed'})
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date))
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())
df['Deaths'] = None

# Output the results
dataframe_output(df, 'PL')
