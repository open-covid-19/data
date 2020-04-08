#!/usr/bin/env python

import pandas
import datetime
from covid_io import read_argv
from utils import dataframe_output


# Read CSV file from covidtracking's GitHub project
data = read_argv()

# Rename the appropriate columns
data = data.rename(columns={
    'date': 'Date',
    'prname': '_RegionLabel',
    'numconf': 'Confirmed',
    'numdeaths': 'Deaths',
    'numtested': 'Tested'
})

# Convert date to datetime object
data['Date'] = data['Date'].apply(
    lambda date: datetime.datetime.strptime(date, '%d-%m-%Y').date().isoformat())

# Output the results
dataframe_output(data, 'CA')
