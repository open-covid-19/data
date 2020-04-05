#!/usr/bin/env python

import re
from datetime import datetime
from utils import read_html, wiki_html_cell_parser, pivot_table, safe_int_cast, dataframe_output


article_path = 'https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data/Chile_medical_cases'
data = read_html(article_path, parser=wiki_html_cell_parser, table_index=0, skiprows=1, header=True)
data = data.set_index(data.columns[0]).iloc[:-2]
data = data[[col for col in data.columns[:16]]]
data = data.drop(['Date'])

df = pivot_table(data)

df['Date'] = df['Date'].apply(lambda date: datetime.strptime(date, '%Y-%m-%d'))
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())

df['Confirmed'] = df['Value'].apply(lambda x: safe_int_cast(x.split('(')[0]))
df['Deaths'] = df['Value'].apply(lambda x: safe_int_cast(x.split('(')[1][:-1] if '(' in x else None))

df = df.sort_values(['Date', 'Pivot']).drop(columns=['Value'])

cumsum_columns = ['Confirmed', 'Deaths']
for region in df['Pivot'].unique():
    mask = df['Pivot'] == region
    df.loc[mask, cumsum_columns] = df.loc[mask, cumsum_columns].fillna(0).cumsum()

# Output the results
dataframe_output(df.rename(columns={'Pivot': 'RegionName'}), 'CL')