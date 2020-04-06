import re
import sys
import locale
from datetime import datetime
from argparse import ArgumentParser
from pandas import isna, isnull, DataFrame
from utils import \
    read_csv, read_html, wiki_html_cell_parser, pivot_table,safe_datetime_parse, safe_int_cast, \
    dataframe_output, ROOT

# We need to set locale in order to parse dates properly
locale.setlocale(locale.LC_TIME, 'es_ES')

# Get country name from metadata
metadata = read_csv(ROOT / 'input' / 'metadata.csv')
country_name = metadata.set_index('CountryCode').loc['PE', 'CountryName'].iloc[0]

# Fetch the table from the Wikipedia article
url_article = 'https://es.wikipedia.org/wiki/Pandemia_de_enfermedad_por_coronavirus_de_2020_en_Per%C3%BA'
data = read_html(
    url_article,
    header=True,
    selector='table.wikitable',
    parser=wiki_html_cell_parser,
    table_index=2).rename(columns={'Fecha': 'Date'})
data = data.set_index('Date').iloc[:-1]
data = data.iloc[:, :-3]

# Some poorly maintained tables have duplicate dates, pick the first row in such case
data = data.loc[~data.index.duplicated(keep='first')]

# Pivot the table to fit our preferred format
df = pivot_table(data, pivot_name='RegionCode')
df = df[~df['RegionCode'].isna()]


# Make sure all dates include year
date_format = '%d %B'
if '%Y' not in date_format:
    date_format = date_format + '-%Y'
    df['Date'] = df['Date'] + '-%d' % datetime.now().year

# Parse into datetime object, drop if not possible
df['Date'] = df['Date'].apply(lambda date: safe_datetime_parse(date, date_format))
df = df[~df['Date'].isna()]

# Convert all dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())

# Get the confirmed and deaths data from the table
parenthesis = lambda x: (re.search(r'\((\d+)\)', x) or [None, None])[1]
df['Confirmed'] = df['Value'].apply(lambda x: safe_int_cast(x.split('(')[0]))
df['Deaths'] = df['Value'].apply(lambda x: safe_int_cast(parenthesis(x)))

# Add up all the rows with same Date and RegionName
def aggregate_region_values(group: DataFrame):
    non_null = [value for value in group if not (isna(value) or isnull(value))]
    return None if not non_null else sum(non_null)
df = df.sort_values(['Date', 'RegionCode'])
df = df.drop(columns=['Value']).groupby(['RegionCode', 'Date']).agg(aggregate_region_values)
df = df.reset_index().sort_values(['Date', 'RegionCode'])

# Aggregate the values region by region
value_columns = ['Confirmed', 'Deaths']
for region in df['RegionCode'].unique():
    mask = df['RegionCode'] == region
    df.loc[mask, value_columns] = df.loc[mask, value_columns].fillna(0).cumsum()
    df.loc[mask, value_columns] = df.loc[mask, value_columns].ffill().fillna(0)

# If we don't have deaths data, then make them null rather than zero
df['Deaths'] = None

# Output the results
dataframe_output(df, 'PE')