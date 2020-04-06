import os
import sys
import datetime
from pathlib import Path
import pandas as pd
from utils import github_raw_dataframe, dataframe_output, pivot_table, ROOT

df = github_raw_dataframe('carranco-sga/Mexico-COVID-19', 'Mexico_COVID19.csv')
df = df.rename(columns={'Fecha': 'Date'}).set_index('Date')

deaths_columns = [col for col in df.columns if col.endswith('_D')]
confirmed_columns = [col[:-2] for col in deaths_columns]

deaths = df[deaths_columns]
confirmed = df[confirmed_columns]
deaths.columns = confirmed.columns

deaths = pivot_table(deaths, pivot_name='RegionCode').rename(columns={'Value': 'Deaths'})
confirmed = pivot_table(confirmed, pivot_name='RegionCode').rename(columns={'Value': 'Confirmed'})

df = confirmed.merge(deaths).sort_values(['Date', 'RegionCode'])

# Output the results
dataframe_output(df, 'MX')