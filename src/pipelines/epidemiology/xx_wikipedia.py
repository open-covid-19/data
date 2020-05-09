import re
import locale
import datetime
from typing import Any, Dict, List
from pandas import DataFrame, isna, isnull

from lib.cast import safe_int_cast, safe_datetime_parse
from lib.io import count_html_tables, read_html, wiki_html_cell_parser
from lib.time import datetime_isoformat
from lib.utils import pivot_table
from pipelines.epidemiology.pipeline import EpidemiologyPipeline

class WikipediaPipeline(EpidemiologyPipeline):
    data_urls: List[str] = ['https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/']
    fetch_opts: List[Dict[str, Any]] = [{'ext': 'html'}]

    def __init__(self, url: str):
        super().__init__()
        self.data_urls = [url]

    @staticmethod
    def parenthesis(x):
        regexp = r'\((\d+)\)'
        return re.sub(regexp, '', x), (re.search(regexp, x) or [None, None])[1]

    @staticmethod
    def aggregate_region_values(group: DataFrame):
        non_null = [value for value in group if not (isna(value) or isnull(value))]
        return None if not non_null else sum(non_null)

    def parse(self, sources, **parse_opts):

        # Get the file contents from source
        html_content = open(sources[0]).read()

        # We need to set locale in order to parse dates properly
        locale.setlocale(locale.LC_TIME, parse_opts.get('locale', 'en_US'))

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
                skiprows=parse_opts.get('skiprows', 0))

            if parse_opts.get('debug'):
                print('\n[%d] Data:' % (table_index + 1))
                print(data.columns)
                print(data.head(50))

            # Some of the tables are in Spanish
            data = data.rename(columns={'Fecha': 'Date'})

            # Set first date column as index, drop other date columns
            columns_lowercase = [(col or '').lower() for col in data.columns]
            date_index = columns_lowercase.index('date') if 'date' in columns_lowercase else 0
            del_index = [i for i, col in enumerate(columns_lowercase) if col == 'date'][1:]
            data = data.iloc[:, [i for i, _ in enumerate(data.columns) if i not in del_index]]
            data = data.set_index(data.columns[date_index])
            # data = data.iloc[:, :-parse_opts.get('skipcols', 0)]
            if parse_opts.get('droprows') is not None:
                try:
                    data = data.drop(parse_opts['droprows'].split(','))
                except:
                    pass

            # Pivot the table to fit our preferred format
            data = pivot_table(data, pivot_name='subregion')
            data = data[~data['subregion'].isna()]

            if parse_opts.get('debug'):
                print('\n[%d] Pivoted:' % (table_index + 1))
                print(data.head(50))

            # Make sure all dates include year
            date_format = parse_opts['date_format']
            if '%Y' not in date_format:
                date_format = date_format + '-%Y'
                data['date'] = data['date'].astype(str) + '-%d' % datetime.datetime.now().year

            # Parse into datetime object, drop if not possible
            data['date'] = data['date'].apply(lambda date: safe_datetime_parse(date, date_format))
            data = data[~data['date'].isna()]

            # If the dataframe is not empty, then we found a good one
            if len(data) > 10 and len(data['subregion'].unique()) > 3:
                break

        # Make sure we have *some* data
        assert len(data) > 0

        # Convert all dates to ISO format
        data['date'] = data['date'].apply(lambda date: date.date().isoformat())

        # Get the confirmed and deaths data from the table
        data['confirmed'] = data['value'].apply(lambda x: safe_int_cast(self.parenthesis(x)[0]))
        data['deceased'] = data['value'].apply(lambda x: safe_int_cast(self.parenthesis(x)[1]))

        # Add up all the rows with same Date and subregion
        data = data.sort_values(['date', 'subregion'])
        data = data.drop(columns=['value']).groupby(['subregion', 'date']).agg(self.aggregate_region_values)
        data = data.reset_index().sort_values(['date', 'subregion'])

        # Compute diff of the values region by region if required
        value_columns = ['confirmed', 'deceased']
        if parse_opts.get('cumsum'):
            for region in data['subregion'].unique():
                mask = data['subregion'] == region
                data.loc[mask, value_columns] = data.loc[mask, value_columns].ffill()
                # Only perform operation if the column is not all NaN
                for column in value_columns:
                    zero_filled = data.loc[mask, column].fillna(0)
                    if sum(zero_filled) > 0:
                        data.loc[mask, column] = zero_filled.diff()

        # Get rid of rows which have all null values
        data = data.dropna(how='all', subset=value_columns)

        # Add the country code to all records
        data['country'] = parse_opts['country']

        # Output the results
        if parse_opts.get('debug'):
            print('\nOutput:')
            print(data.head(50))
        return data.rename(columns={'subregion': 'match_string'})
