#!/usr/bin/env python

'''
This script is used to parse a table from Wikipedia for COVID-19 pandemic data.
It is able to handle repeated date rows by ignoring all but the first instance,
and repeated columns by adding all elements with the same column name.
'''

import re
import sys
import locale
from datetime import datetime
from argparse import ArgumentParser
from pandas import isna, isnull, DataFrame
from covid_io import read_file, wiki_html_cell_parser
from utils import \
    pivot_table, safe_datetime_parse, safe_int_cast, dataframe_output


# Parse arguments
parser = ArgumentParser()
parser.add_argument('html-file', type=str)
parser.add_argument('--country-code', type=str, default=None)
parser.add_argument('--article', type=str, default=None)
parser.add_argument('--locale', type=str, default='en_US')
parser.add_argument('--cumsum', type=bool, default=False)
parser.add_argument('--skiprows', type=int, default=1)
parser.add_argument('--skipcols', type=int, default=2)
parser.add_argument('--droprows', type=str, default=None)
parser.add_argument('--date-format', type=str, default='%b %d')
parser.add_argument('--table-index', type=int, default=0)
parser.add_argument('--null-deaths', type=bool, default=False)
args = parser.parse_args(sys.argv[1:])

# We need to set locale in order to parse dates properly
locale.setlocale(locale.LC_TIME, args.locale)

data = read_file(
    getattr(args, 'html-file'),
    header=True,
    selector='table.wikitable',
    parser=wiki_html_cell_parser,
    table_index=args.table_index,
    skiprows=args.skiprows)

# Some of the tables are in Spanish
data = data.rename(columns={'Fecha': 'Date'})

# Set date column as index
columns_lowercase = [(col or '').lower() for col in data.columns]
date_index = columns_lowercase.index('date') if 'date' in columns_lowercase else 0
data = data.set_index(data.columns[date_index])
data = data.iloc[:, :-args.skipcols]
if args.droprows is not None:
    data = data.drop(args.droprows.split(','))

# Pivot the table to fit our preferred format
df = pivot_table(data, pivot_name='RegionName')
df = df[~df['RegionName'].isna()]

# Make sure all dates include year
date_format = args.date_format
if '%Y' not in date_format:
    date_format = date_format + '-%Y'
    df['Date'] = df['Date'] + '-%d' % datetime.now().year

# Parse into datetime object, drop if not possible
df['Date'] = df['Date'].apply(lambda date: safe_datetime_parse(date, date_format))
df = df[~df['Date'].isna()]

# Convert all dates to ISO format
df['Date'] = df['Date'].apply(lambda date: date.date().isoformat())


def parenthesis(x):
    regexp = r'\((\d+)\)'
    return re.sub(regexp, '', x), (re.search(regexp, x) or [None, None])[1]


# Get the confirmed and deaths data from the table
df['Confirmed'] = df['Value'].apply(lambda x: safe_int_cast(parenthesis(x)[0]))
df['Deaths'] = df['Value'].apply(lambda x: safe_int_cast(parenthesis(x)[1]))


def aggregate_region_values(group: DataFrame):
    non_null = [value for value in group if not (isna(value) or isnull(value))]
    return None if not non_null else sum(non_null)


# Add up all the rows with same Date and RegionName
df = df.sort_values(['Date', 'RegionName'])
df = df.drop(columns=['Value']).groupby(['RegionName', 'Date']).agg(aggregate_region_values)
df = df.reset_index().sort_values(['Date', 'RegionName'])

# Compute cumsum of the values region by region
value_columns = ['Confirmed', 'Deaths']
if not args.cumsum:
    for region in df['RegionName'].unique():
        mask = df['RegionName'] == region
        df.loc[mask, value_columns] = df.loc[mask, value_columns].cumsum()

# Get rid of rows which have all null values
df = df.dropna(how='all', subset=value_columns)

# If we don't have deaths data, then make them null rather than zero
if args.null_deaths:
    df['Deaths'] = None

# Output the results
dataframe_output(df, args.country_code)
