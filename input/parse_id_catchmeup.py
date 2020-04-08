#!/usr/bin/env python

from datetime import datetime
from datetime import datetime
from covid_io import read_argv
from utils import safe_datetime_parse, pivot_table, dataframe_output


def parse_date(date):
    return safe_datetime_parse('%s-%d' % (date, datetime.now().year), '%d-%b-%Y')


# Read data from Google Sheets
df = read_argv()

df.columns = df.iloc[0]
df = df.rename(columns={'Provinsi': 'Date'})
df = df.iloc[1:].set_index('Date')

df = df[df.columns[5:]]
df = pivot_table(df.transpose(), pivot_name='RegionName')
df['Date'] = df['Date'].apply(parse_date)
df = df.rename(columns={'Value': 'Confirmed'})
df['Deaths'] = None
df = df.dropna(how='all', subset=['Confirmed', 'Deaths'])

# Output the results
dataframe_output(df, 'ID')
