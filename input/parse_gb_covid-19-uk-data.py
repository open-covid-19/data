#!/usr/bin/env python

from pandas import DataFrame
from covid_io import read_argv
from utils import dataframe_output


# Read data from GitHub repo
# https://raw.github.com/tomwhite/covid-19-uk-data/master/data/covid-19-indicators-uk.csv
df = read_argv()

# Aggregate time series data into relational format
records = []
for idx, rows in df.groupby(['Date', 'Country']):
    records.append({
        'Date': idx[0],
        'Country': idx[1],
        **{record.loc['Indicator']: record.loc['Value'] for _, record in rows.iterrows()}
    })
df = DataFrame.from_records(records).rename(
    columns={'Country': '_RegionLabel', 'ConfirmedCases': 'Confirmed'})

# Output the results
dataframe_output(df, 'GB')