#!/usr/bin/env python

from datetime import datetime
from pandas import DataFrame
from utils import github_raw_dataframe, dataframe_output


# Get CSV file from Github
df = github_raw_dataframe('covid19-eu-zh/covid19-eu-data', 'dataset/covid-19-no.csv')
df = df.rename(columns={'datetime': 'Date', 'nuts_3': 'RegionName', 'cases': 'Confirmed'})
df['Date'] = df['Date'].apply(lambda date: datetime.fromisoformat(date))
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())
df['Deaths'] = None

# Output the results
dataframe_output(df, 'NO')