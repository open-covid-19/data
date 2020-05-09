import os
import re
import sys
from pathlib import Path
from typing import List
from argparse import ArgumentParser

import pandas
from unidecode import unidecode
from pandas import DataFrame, read_csv
from bs4 import BeautifulSoup, Tag

from .cast import safe_int_cast


def fuzzy_text(text: str):
    return re.sub(r'[^a-z]', '', unidecode(str(text)).lower())


def read_argv(**kwargs):
    ''' Reads the files given as arguments '''
    parser = ArgumentParser()
    parser.add_argument('path', type=str, nargs='+')
    args = parser.parse_args(sys.argv[1:])
    data = [read_file(path, **kwargs) for path in args.path]
    return data[0] if len(data) == 1 else data


def read_file(path: str, **kwargs):
    ext = str(path).split('.')[-1]
    if ext == 'csv':
        return pandas.read_csv(path, **kwargs)
    elif ext == 'json':
        return pandas.read_json(path, **kwargs)
    elif ext == 'html':
        return read_html(open(path).read(), **kwargs)
    elif ext == 'xls' or ext == 'xlsx':
        return pandas.read_excel(path, **kwargs)
    else:
        raise ValueError('Unrecognized extension: %s' % ext)


def _get_html_columns(row: Tag) -> List[Tag]:
    cols = []
    for elem in filter(lambda row: isinstance(row, Tag), row.children):
        cols += [elem] * (safe_int_cast(elem.attrs.get('colspan', 1)) or 1)
    return list(cols)


def _default_html_cell_parser(cell: Tag, row_idx: int, col_idx: int):
    return cell.get_text().strip()


def count_html_tables(html: str, selector: str = 'table'):
    page = BeautifulSoup(html, 'lxml')
    return len(page.select(selector))


def wiki_html_cell_parser(cell: Tag, row_idx: int, col_idx: int):
    return re.sub(r'\[.+\]', '', cell.get_text().strip())


def read_html(
    html: str,
    selector: str = 'table',
    table_index: int = 0,
    skiprows: int = 0,
    header: bool = False,
    parser = None) -> DataFrame:
    ''' Parse an HTML table into a DataFrame '''
    parser = parser if parser is not None else _default_html_cell_parser

    # Fetch table and read its rows
    page = BeautifulSoup(html, 'lxml')
    table = page.select(selector)[table_index]
    rows = [_get_html_columns(row) for row in table.find_all('tr')]

    # Adjust for rowspan > 1
    for idx_row, row in enumerate(rows):
        for idx_cell, cell in enumerate(row):
            rowspan = int(cell.attrs.get('rowspan', 1))
            cell.attrs['rowspan'] = 1  # reset to prevent cascading
            for offset in range(1, rowspan):
                rows[idx_row + offset].insert(idx_cell, cell)

    # Get text within table cells and build dataframe
    records = []
    for row_idx, row in enumerate(rows[skiprows:]):
        records.append([parser(elem, row_idx, col_idx) for col_idx, elem in enumerate(row)])
    data = DataFrame.from_records(records)

    # Parse header if requested
    if header:
        data.columns = data.iloc[0]
        data = data.drop(data.index[0])

    return data
