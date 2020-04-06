#!/usr/bin/env python

import pandas
import datetime
from utils import github_raw_dataframe, dataframe_output


# Read CSV file from covidtracking's GitHub project
df = pandas.read_csv('https://health-infobase.canada.ca/src/data/covidLive/covid19.csv')

# Rename the appropriate columns
df = df.rename(columns={
    'date': 'Date',
    'prname': '_RegionLabel',
    'numconf': 'Confirmed',
    'numdeaths': 'Deaths',
    'numtested': 'Tested'
})

 # Convert date to datetime object
df['Date'] = df['Date'].apply(
    lambda date: datetime.datetime.strptime(date, '%d-%m-%Y').date().isoformat())

# Output the results
dataframe_output(df, 'CA')