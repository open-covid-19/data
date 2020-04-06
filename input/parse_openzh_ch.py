#!/usr/bin/env python

from datetime import datetime
from pandas import DataFrame
from utils import github_raw_dataframe, dataframe_output


# Get CSV file from Github
df = github_raw_dataframe('openZH/covid_19', 'COVID19_Fallzahlen_CH_total.csv')
df = df.rename(columns={
    'date': 'Date',
    'ncumul_tested': 'Tested',
    'ncumul_conf': 'Confirmed',
    'ncumul_deceased': 'Deaths',
    'abbreviation_canton_and_fl': 'RegionCode'})

# Output the results
dataframe_output(df.reset_index(), 'CH')