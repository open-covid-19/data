#!/usr/bin/env python

import re
from datetime import datetime
from utils import read_html, pivot_table, safe_int_cast, dataframe_output


article_path = 'https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data/Russia_medical_cases'
data = read_html(article_path, selector='table.wikitable', table_index=1, skiprows=1, header=True)
data = data.set_index(data.columns[0]).iloc[:-1]
data = data[[col for col in data.columns[:-8]]]

df = pivot_table(data)

df['Date'] = df['Date'] + ' %d' % datetime.now().year
df['Date'] = df['Date'].apply(lambda date: datetime.strptime(date, '%d %b %Y'))
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())

df['Value'] = df['Value'].apply(lambda x: re.sub(r'\[.+\]', '', x))
df['Confirmed'] = df['Value'].apply(lambda x: safe_int_cast(x.split('(')[0]))
df['Deaths'] = df['Value'].apply(lambda x: safe_int_cast(x.split('(')[1][:-1] if '(' in x else None))

df = df.sort_values(['Date', '_RegionLabel']).drop(columns=['Value'])

cumsum_columns = ['Confirmed', 'Deaths']
for region in df['_RegionLabel'].unique():
    mask = df['_RegionLabel'] == region
    df.loc[mask, cumsum_columns] = df.loc[mask, cumsum_columns].fillna(0).cumsum()

# Unfortunately we don't have deaths data
df['Deaths'] = None

# Output the results
dataframe_output(df, 'RU')