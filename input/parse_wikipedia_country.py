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
from covid_io import read_html, count_html_tables, wiki_html_cell_parser
from utils import \
    pivot_table, safe_datetime_parse, safe_int_cast, dataframe_output


# Parse arguments
parser = ArgumentParser()
parser.add_argument('html-file', type=str)
parser.add_argument('--country-code', type=str, default=None)
parser.add_argument('--article', type=str, default=None)
parser.add_argument('--locale', type=str, default='en_US')
parser.add_argument('--cumsum', action='store_true')
parser.add_argument('--skiprows', type=int, default=1)
parser.add_argument('--skipcols', type=int, default=2)
parser.add_argument('--droprows', type=str, default=None)
parser.add_argument('--date-format', type=str, default='%b %d')
parser.add_argument('--null-deaths', action='store_true')
parser.add_argument('--debug', action='store_true')
args = parser.parse_args(sys.argv[1:])

# We need to set locale in order to parse dates properly
locale.setlocale(locale.LC_TIME, args.locale)

# Get the file contents from arguments
html_content = open(getattr(args, 'html-file')).read()

# Tables keep changing order, so iterate through all until one looks good
table_count = count_html_tables(html_content, selector='table.wikitable')

data = None
for table_index in range(table_count):
    data = read_html(
        html_content,
        header=True,
        selector='table.wikitable',
        parser=wiki_html_cell_parser,
        table_index=table_index,
        skiprows=args.skiprows)

    if args.debug:
        print('\n[%d] Data:' % (table_index + 1))
        print(data.head(50))

    # Some of the tables are in Spanish
    data = data.rename(columns={'Fecha': 'Date'})

    # Set date column as index
    columns_lowercase = [(col or '').lower() for col in data.columns]
    date_index = columns_lowercase.index('date') if 'date' in columns_lowercase else 0
    data = data.set_index(data.columns[date_index])
    data = data.iloc[:, :-args.skipcols]
    if args.droprows is not None:
        try:
            data = data.drop(args.droprows.split(','))
        except:
            pass

    # Pivot the table to fit our preferred format
    data = pivot_table(data, pivot_name='RegionName')
    data = data[~data['RegionName'].isna()]

    if args.debug:
        print('\n[%d] Pivoted:' % (table_index + 1))
        print(data.head(50))

    # Make sure all dates include year
    date_format = args.date_format
    if '%Y' not in date_format:
        date_format = date_format + '-%Y'
        data['Date'] = data['Date'].astype(str) + '-%d' % datetime.now().year

    # Parse into datetime object, drop if not possible
    data['Date'] = data['Date'].apply(lambda date: safe_datetime_parse(date, date_format))
    data = data[~data['Date'].isna()]

    # If the dataframe is not empty, then we found a good one
    if len(data) > 10:
        break

# Convert all dates to ISO format
data['Date'] = data['Date'].apply(lambda date: date.date().isoformat())


def parenthesis(x):
    regexp = r'\((\d+)\)'
    return re.sub(regexp, '', x), (re.search(regexp, x) or [None, None])[1]


# Get the confirmed and deaths data from the table
data['Confirmed'] = data['Value'].apply(lambda x: safe_int_cast(parenthesis(x)[0]))
data['Deaths'] = data['Value'].apply(lambda x: safe_int_cast(parenthesis(x)[1]))


def aggregate_region_values(group: DataFrame):
    non_null = [value for value in group if not (isna(value) or isnull(value))]
    return None if not non_null else sum(non_null)


# Add up all the rows with same Date and RegionName
data = data.sort_values(['Date', 'RegionName'])
data = data.drop(columns=['Value']).groupby(['RegionName', 'Date']).agg(aggregate_region_values)
data = data.reset_index().sort_values(['Date', 'RegionName'])

# Compute cumsum of the values region by region
value_columns = ['Confirmed', 'Deaths']
if not args.cumsum:
    for region in data['RegionName'].unique():
        mask = data['RegionName'] == region
        data.loc[mask, value_columns] = data.loc[mask, value_columns].cumsum()

# Get rid of rows which have all null values
data = data.dropna(how='all', subset=value_columns)

# If we don't have deaths data, then make them null rather than zero
if args.null_deaths:
    data['Deaths'] = None

if args.debug:
    print('Output:')
    print(data.head(50))

# Output the results
dataframe_output(data, args.country_code)
