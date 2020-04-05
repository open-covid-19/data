#!/usr/bin/env python

'''
This script is used to parse a table from Wikipedia for COVID-19 pandemic data.
It is able to handle repeated date rows by ignoring all but the first instance,
and repeated columns by adding all elements with the same column name.
'''

import re
import sys
from datetime import datetime
from argparse import ArgumentParser
from pandas import isna, isnull, DataFrame
from utils import \
    read_csv, read_html, wiki_html_cell_parser, pivot_table, safe_datetime_parse, safe_int_cast, \
    dataframe_output, ROOT


# Number of columns to skip from the end of the table
EXTRA_COLUMN_COUNT = 2

# Parse arguments
parser = ArgumentParser()
parser.add_argument('code', type=str)
parser.add_argument('--cumsum', type=bool, default=False)
parser.add_argument('--skip_head', type=int, default=1)
parser.add_argument('--skip_tail', type=int, default=1)
parser.add_argument('--drop_rows', type=str, default=None)
parser.add_argument('--date_format', type=str, default='%b %d')
parser.add_argument('--table_index', type=int, default=0)
parser.add_argument('--null_deaths', type=bool, default=False)
args = parser.parse_args(sys.argv[1:])

# Get country name from metadata
metadata = read_csv(ROOT / 'input' / 'metadata.csv')
country_name = metadata.set_index('CountryCode').loc[args.code, 'CountryName'].iloc[0]

# Fetch the table from the Wikipedia article
url_base = 'https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data'
url_article = '%s/%s_medical_cases' % (url_base, country_name)
data = read_html(
    url_article,
    header=True,
    selector='table.wikitable',
    parser=wiki_html_cell_parser,
    table_index=args.table_index,
    skiprows=args.skip_head)
data = data.set_index(data.columns[0]).iloc[:-args.skip_tail]
data = data.iloc[:, :-EXTRA_COLUMN_COUNT]
if args.drop_rows is not None:
    data = data.drop(args.drop_rows.split(','))

# Some poorly maintained tables have duplicate dates, pick the first row in such case
data = data.loc[~data.index.duplicated(keep='first')]

# Pivot the table to fit our preferred format
df = pivot_table(data, pivot_name='RegionName')
df = df[~df['RegionName'].isna()]

# Make sure all dates include year
date_format = args.date_format
if '%Y' not in date_format:
    date_format = args.date_format + '-%Y'
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
df = df.sort_values(['Date', 'RegionName'])
df = df.drop(columns=['Value']).groupby(['RegionName', 'Date']).agg(aggregate_region_values)
df = df.reset_index().sort_values(['Date', 'RegionName'])

# Aggregate the values region by region
value_columns = ['Confirmed', 'Deaths']
for region in df['RegionName'].unique():
    mask = df['RegionName'] == region
    if not args.cumsum:
        df.loc[mask, value_columns] = df.loc[mask, value_columns].fillna(0).cumsum()
    df.loc[mask, value_columns] = df.loc[mask, value_columns].ffill().fillna(0)

# If we don't have deaths data, then make them null rather than zero
if args.null_deaths:
    df['Deaths'] = None

# Output the results
dataframe_output(df, args.code)