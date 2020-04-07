#!/usr/bin/env python

from datetime import datetime
from datetime import datetime
from utils import safe_datetime_parse, pivot_table, google_sheets_dataframe, dataframe_output


def parse_date(date):
    return safe_datetime_parse('%s-%d' % (date, datetime.now().year), '%d-%b-%Y')

# Read data from Google Sheets
df = google_sheets_dataframe('1sgiz8x71QyIVJZQguYtG9n6xBEKdM4fXuDs_d8zKOmY', sheet='Data%20Provinsi')

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