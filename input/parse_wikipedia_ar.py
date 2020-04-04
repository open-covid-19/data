#!/usr/bin/env python

import re
from datetime import datetime
from utils import read_html, wiki_html_cell_parser, pivot_table, safe_int_cast, dataframe_output


article_path = 'https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data/Argentina_medical_cases'
data = read_html(article_path, parser=wiki_html_cell_parser, table_index=0, skiprows=1, header=True)
data = data.set_index(data.columns[0]).iloc[:-1]
data = data[[col for col in data.columns[:21]]]
data = data.drop(['Date'])

df = pivot_table(data)

df['Date'] = df['Date'] + ' %d' % datetime.now().year
df['Date'] = df['Date'].apply(lambda date: datetime.strptime(date, '%d %b %Y'))
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())

df['Confirmed'] = df['Value'].apply(lambda x: safe_int_cast(x.split('(')[0]))
df['Deaths'] = df['Value'].apply(lambda x: safe_int_cast(x.split('(')[1][:-1] if '(' in x else None))

df = df.sort_values(['Date', 'Pivot']).drop(columns=['Value'])

# Data is already cumsum
ffill_columns = ['Confirmed', 'Deaths']
for region in df['Pivot'].unique():
    mask = df['Pivot'] == region
    df.loc[mask, ffill_columns] = df.loc[mask, ffill_columns].ffill()

# Output the results
dataframe_output(df.rename(columns={'Pivot': 'RegionName'}), 'AR')