#!/usr/bin/env python

import re
import os
import sys
import shutil

# This script must be run from /src
sys.path.append(os.getcwd())
from lib.io import read_file, export_csv
from lib.utils import ROOT

from backcompat_forecast import main as build_forecast


def snake_to_camel_case(txt: str) -> str:
    """ Used to convert V2 column names to V1 column names for backwards compatibility """
    txt = re.sub(r"_(\w)", lambda m: m.group(1).upper(), txt.capitalize())


# Create the folder which will be published
v2_folder = ROOT / "public" / "v2"
v2_folder.mkdir(exist_ok=True, parents=True)

# Copy all output files to the V2 folder
for output_file in (ROOT / "output").glob("*.csv"):
    shutil.copy(output_file, v2_folder / output_file.name)

# Convert all CSV files to JSON using values format
for csv_file in (v2_folder).glob("*.csv"):
    json_name = csv_file.name.replace("csv", "json")
    data = read_file(csv_file)
    data.to_json(v2_folder / json_name, orient="values")

# Create the legacy data.csv file
v1_folder = ROOT / "public"
data = read_file(v2_folder / "index.csv")
data = data.merge(read_file(v2_folder / "geography.csv"))
data = data.merge(read_file(v2_folder / "demographics.csv"))
data = data.merge(read_file(v2_folder / "epidemiology.csv"))
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
data = data.dropna(subset=["Confirmed", "Deaths"], how="all")
data = data.sort_values(["Date", "Key"])
export_csv(data, v1_folder / "data.csv")

# Create the v1 minimal.csv file
export_csv(data[["Date", "Key", "Confirmed", "Deaths"]], v1_folder / "data_minimal.csv")

# Create the v1 CSV files which only require column mapping
v1_v2_name_map = {"response": "oxford-government-response", "weather": "weather"}
for v1_name, v2_name in v1_v2_name_map.items():
    df = read_file(v2_folder / f"{v2_name}.csv")
    df.columns = list(map(snake_to_camel_case, df.columns))
    export_csv(df, v1_folder / f"{v1_name}.csv")

# Create the v1 mobility.csv file
export_csv(
    read_file("https://open-covid-19.github.io/data/mobility.csv"), v1_folder / "mobility.csv",
)

# Create the v1 forecast.csv file
export_csv(build_forecast(), v1_folder / "forecast.csv")

# Convert all v1 CSV files to JSON using record format
for csv_file in (v1_folder).glob("*.csv"):
    json_name = csv_file.name.replace("csv", "json")
    data = read_file(csv_file)
    data.to_json(v1_folder / json_name, orient="records")
