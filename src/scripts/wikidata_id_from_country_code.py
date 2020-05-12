#!/usr/bin/env python

import os
import re
import sys
import pandas
import requests
from argparse import ArgumentParser
from multiprocessing.pool import ThreadPool

from tqdm import tqdm
from bs4 import BeautifulSoup

# This script must be run from /src
sys.path.append(os.getcwd())
from lib.io import read_html, read_file
from lib.utils import ROOT


def cell_parser(elem, row_idx, col_idx):
    try:
        return elem.select("a").attrs["href"][-1]
    except Exception as exc:
        return elem.get_text().strip()


def recursive_search(root, key):
    if key in root:
        return root[key]
    for val in root.values():
        if isinstance(val, dict):
            res = recursive_search(val, key)
            if res is not None:
                return res
    return None


def wikidata_id_from_title(title: str) -> str:
    api_base = (
        "https://en.wikipedia.org/w"
        "/api.php?action=query&prop=pageprops&redirects=1&format=json"
    )
    res = requests.get("{}&titles={}".format(api_base, title)).json()
    return recursive_search(res, "wikibase_item")


def subdivision_identifiers(country_code: str, table_index: int = 0):
    url = "https://en.wikipedia.org/wiki/ISO_3166-2:" + country_code
    data = read_html(
        requests.get(url).text, table_index=table_index, header=True, parser=cell_parser
    )

    if len(data.columns) < 2 or "Code" not in data.columns:
        return

    data_index = 0
    code_col = list(data.columns).index("Code")
    while data.iloc[data_index][code_col] == "Code":
        data_index += 1

    url_column_list = list(
        # filter(lambda x: x and re.match(r"/wiki/.+_language", x), data.columns)
        map(lambda x: 1 if x and re.match(r"/wiki/.+", x) else 0, data.iloc[data_index])
    )
    url_column_index = 1 if sum(url_column_list) == 0 else url_column_list.index(1)

    for idx, row in list(data.iterrows())[data_index:]:
        title = re.sub(r"/wiki/", "", row.iloc[url_column_index])
        wikidata_id = wikidata_id_from_title(title) or ""
        yield "{},{}".format(row["Code"].replace("-", "_"), wikidata_id)


# Parse arguments from the command line
args = ArgumentParser()
args.add_argument("country_code", type=str)
args.add_argument("--table-index", type=int, default=0)
args.add_argument("--skip-rows", type=int, default=0)
args = args.parse_args()

country_code = args.country_code
aux = read_file(ROOT / "src" / "data" / "auxiliary.csv")
country_name = aux.loc[aux.key == country_code, "country_name"].iloc[0]
print("{},{}".format(country_code, wikidata_id_from_title(country_name)))
for wikidata_info in subdivision_identifiers(args.country_code, args.table_index):
    print(wikidata_info)
