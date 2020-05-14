#!/usr/bin/env python

import os
import re
import sys
import pandas
import requests
from functools import partial
from argparse import ArgumentParser
from multiprocessing import cpu_count
from typing import Any, Dict, List

from tqdm import tqdm
from multiprocess import Pool

# This script must be run from /src
sys.path.append(os.getcwd())
from lib.cast import safe_float_cast
from lib.io import read_file
from lib.utils import ROOT


def recursive_search(root, keys):
    if all(key in root for key in keys):
        return root
    for val in root.values() if isinstance(root, dict) else list(root):
        if isinstance(val, (dict, list)):
            res = recursive_search(val, keys)
            if res is not None:
                return res
    return None


def all_props_keys(props: Dict[str, str]):
    for key in props.keys():
        for part in key.split("|"):
            yield part


def chain_json_get(obj, *keys):
    for key in keys:
        obj = obj[key] if isinstance(obj, list) else obj.get(key, {})
    return obj


def cast_prop_amount(value):
    return safe_float_cast((value or "").replace("+", ""))


def process_property(obj, name: str, prop: str):
    value = chain_json_get(obj, "claims", prop, 0, "mainsnak", "datavalue", "value")
    if "amount" in value:
        value = {name: cast_prop_amount(value.get("amount"))}
    return value


def wikidata_properties(props: Dict[str, str], entity: str) -> Dict[str, Any]:
    api_base = "https://www.wikidata.org/w/api.php?action=wbgetclaims&format=json"
    res = requests.get("{}&entity={}".format(api_base, entity)).json()
    if not res.get("claims"):
        print("Request: {}&entity={}".format(api_base, entity), file=sys.stderr)
        print("Response: {}".format(res), file=sys.stderr)
        return {}

    output: Dict[str, Any] = {}
    for name, prop in props.items():
        output = {**output, **process_property(res, name, prop)}
    return output


def process_entity(props: Dict[str, str], record: pandas.Series):
    key, entity = record
    output = wikidata_properties(props, entity)
    if not output:
        return
    data = {}
    for col in all_props_keys(props):
        v = output.get(col, None)
        if v is None:
            v = ""
        if isinstance(v, (int, float)):
            v = "{0:.06f}".format(v)
            v = v.rstrip("0").rstrip(".")
        data[col] = v
    return key + "," + ",".join("{%s}" % col for col in all_props_keys(props)).format(**data)


# Parse arguments from the command line
argparser = ArgumentParser()
argparser.add_argument("props", type=str)
argparser.add_argument("--output-file", type=str, default=sys.stdout)
args = argparser.parse_args()

# Process the properties provided
props = {}
for prop in args.props.split(","):
    k, v = map(lambda x: x.replace("\n", "").strip(), prop.split(":", 2))
    props[k] = v

# Load metadata and setup parallel processing
wikidata = read_file(ROOT / "src" / "data" / "knowledge_graph.csv")
iter_values = tqdm(wikidata.set_index("key")["wikidata"].iteritems(), total=len(wikidata))
iter_func = partial(process_entity, props)

with open(args.output_file, "w") as fd:
    fd.write("key," + ",".join(all_props_keys(props)) + "\n")
    for idx, res in enumerate(Pool(cpu_count() // 2).imap_unordered(iter_func, iter_values)):
        if res:
            fd.write(res + "\n")
            if idx % 32:
                fd.flush()

"""
Sample usage:
python scripts/wikidata_props.py "`
    latitude|longitude:P625,`
    area:P2046,`
    elevation:P2044,`
    population:P1082,`
    gdp:P2131,`
    gdp_per_capita:P2132,`
    human_development_index:P1081,`
    life_expectancy:P2250" `
    --output-file data/output.csv
"""
