#!/usr/bin/env python

import os
import sys
from argparse import ArgumentParser
import datacommons as dc

# This script must be run from /src
sys.path.append(os.getcwd())
from lib.io import read_file
from lib.utils import ROOT


# Create the legacy data.csv file
data = read_file(ROOT / "output" / "metadata.csv")
data = data.merge(read_file(ROOT / "output" / "geography.csv"))
data = data.merge(read_file(ROOT / "output" / "demographics.csv"))
data = data.merge(read_file(ROOT / "output" / "epidemiology.csv"))
data = data[data.subregion2_code.isna()]
rename_columns = {
    "date": "Date",
    "key": "Key",
    "country_code": "CountryCode",
    "country_name": "CountryName",
    "subregion1_code": "RegionCode",
    "subregion1_name": "RegionName",
    "total_confirmed": "Confirmed",
    "total_deceased": "Deaths",
    "latitude": "Latitude",
    "longitude": "Longitude",
    "population": "Population",
}
data = data[rename_columns.keys()].rename(columns=rename_columns)
data.to_csv(ROOT / "output" / "data.csv", index=False)
data.to_json(ROOT / "output" / "data.json", orient="records")

# Convert all CSV to JSON
for csv_file in (ROOT / "output").glob("*.csv"):
    json_name = csv_file.name.replace("csv", "json")
    data = read_file(csv_file)
    data.to_json(ROOT / "output" / json_name, orient="values")
