#!/usr/bin/env python

from datetime import datetime
from pandas import DataFrame
from covid_io import read_argv
from utils import dataframe_output


# Get CSV file from Github
data = read_argv()
data = data.rename(columns={
    'date': 'Date',
    'ncumul_tested': 'Tested',
    'ncumul_conf': 'Confirmed',
    'ncumul_deceased': 'Deaths',
    'abbreviation_canton_and_fl': 'RegionCode'})

# Output the results
dataframe_output(data.reset_index(), 'CH')