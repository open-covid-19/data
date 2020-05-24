#!/usr/bin/env python

import re
import os
import sys
import shutil
import datetime
from pandas import DataFrame

from lib.forecast import main as build_forecast
from lib.io import read_file, export_csv
from lib.utils import ROOT


def snake_to_camel_case(txt: str) -> str:
    """ Used to convert V2 column names to V1 column names for backwards compatibility """
    return re.sub(r"_(\w)", lambda m: m.group(1).upper(), txt.capitalize())


def camel_to_snake_case(txt: str) -> str:
    """ Used to convert V1 column names to V2 column names for backwards compatibility """
    txt = txt[0].lower() + txt[1:]
    return re.sub(r"([A-Z])", lambda m: "_" + m.group(1).lower(), txt)


def subset_last_days(data: DataFrame, days: int) -> DataFrame:
    """ Used to get the last N days of data """
    # Early exit: this data has no date
    if not "date" in data.columns or len(data.date.dropna()) == 0:
        return data
    else:
        last_date = datetime.date.fromisoformat(max(data.date))
        first_date = last_date - datetime.timedelta(days=days)
        return data[data.date > first_date.isoformat()]


def subset_latest(data: DataFrame) -> DataFrame:
    """ Used to get the latest data for each key """
    # Early exit: this data has no date
    if not "date" in data.columns or len(data.date.dropna()) == 0:
        return data
    else:
        non_null_columns = [col for col in data.columns if not col in ("key", "date")]
        data = data.dropna(subset=non_null_columns, how="all")
        return data.sort_values("date").groupby("key").last().reset_index()


def subset_last_days(data: DataFrame, days: int) -> DataFrame:
    """ Used to get the last N days of data """
    # Early exit: this data has no date
    if not "date" in data.columns or len(data.date.dropna()) == 0:
        return data
    else:
        last_date = datetime.date.fromisoformat(max(data.date))
        first_date = last_date - datetime.timedelta(days=days)
        return data[data.date > first_date.isoformat()]


# Wipe the public folder first
public_folder = ROOT / "output" / "public"
for item in public_folder.glob("*"):
    if item.name.startswith("."):
        continue
    if item.is_file():
        item.unlink()
    else:
        shutil.rmtree(item)

# Create the folder which will be published
v2_folder = public_folder / "v2"
v2_folder.mkdir(exist_ok=True, parents=True)

# Copy all output files to the V2 folder
print("Copying files to public folder...")
for output_file in (ROOT / "output" / "tables").glob("*.csv"):
    shutil.copy(output_file, v2_folder / output_file.name)

# TMP: Get mobility from V1
mobility = read_file("https://open-covid-19.github.io/data/mobility.csv")
rename_columns = {
    col: ("mobility_" if col not in ("Key", "Date") else "") + camel_to_snake_case(col)
    for col in mobility.columns
}
export_csv(mobility.rename(columns=rename_columns), v2_folder / "google-mobility.csv")

# Merge all output files into a single master table
print("Creating master table...")
master = read_file(v2_folder / "index.csv")
for output_file in v2_folder.glob("*.csv"):
    if output_file.name not in ("index.csv", "master.csv"):
        master = master.merge(read_file(output_file, low_memory=False), how="left")

# Drop rows without a single dated record
export_csv(master.dropna(subset=["date"]), v2_folder / "master.csv")

# Create subsets with the last 30, 14 and 7 days of data
print("Creating last N days subsets...")
for n_days in (30, 14, 7):
    n_days_folder = v2_folder / str(n_days)
    n_days_folder.mkdir(exist_ok=True)
    for csv_file in (v2_folder).glob("*.csv"):
        data = read_file(csv_file, low_memory=False)
        export_csv(subset_last_days(data, n_days), n_days_folder / csv_file.name)

# Create a subset with the latest known day of data for each key
print("Creating the latest subset...")
latest_folder = v2_folder / "latest"
latest_folder.mkdir(exist_ok=True)
for csv_file in (v2_folder).glob("*.csv"):
    data = read_file(csv_file, low_memory=False)
    export_csv(subset_latest(data), latest_folder / csv_file.name)

# Convert all CSV files to JSON using values format
print("Converting V2 CSV to JSON...")
for csv_file in (v2_folder).glob("**/*.csv"):
    data = read_file(csv_file, low_memory=False)
    json_path = str(csv_file).replace("csv", "json")
    # The main master.json is too big for GitHub Pages...
    if json_path != str(v2_folder / "master.json"):
        data.to_json(json_path, orient="values")

# Perform data transformations for backwards compatibility
v1_folder = public_folder  # Same as root
print("Performing backwards compatibility transformations...")

# Create the v1 data.csv file
data = master[master.aggregation_level < 2]
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

# Create the v1 data_minimal.csv file
export_csv(data[["Date", "Key", "Confirmed", "Deaths"]], v1_folder / "data_minimal.csv")

# Create the v1 data_latest.csv file
export_csv(data.groupby("Key").last().reset_index(), v1_folder / "data_latest.csv")

# Create the v1 weather.csv file
weather = read_file(v2_folder / "weather.csv")
weather = weather[weather.key.apply(lambda x: len(x.split("_")) < 3)]
weather = weather.rename(columns={"noaa_distance": "distance", "noaa_station": "station",})
rename_columns = {col: snake_to_camel_case(col) for col in weather.columns}
export_csv(weather.rename(columns=rename_columns), v1_folder / "weather.csv")

# TMP: Get mobility from V1
mobility = read_file("https://open-covid-19.github.io/data/mobility.csv")
export_csv(mobility, v1_folder / "mobility.csv")

# Create the v1 CSV files which only require column mapping
v1_v2_name_map = {"response": "oxford-government-response"}
for v1_name, v2_name in v1_v2_name_map.items():
    data = read_file(v2_folder / f"{v2_name}.csv")
    rename_columns = {col: snake_to_camel_case(col) for col in data.columns}
    export_csv(data.rename(columns=rename_columns), v1_folder / f"{v1_name}.csv")

# Create the v1 forecast.csv file
print("Computing forecast...")
export_csv(
    build_forecast(read_file(v1_folder / "data_minimal.csv")), v1_folder / "data_forecast.csv"
)

# Convert all v1 CSV files to JSON using record format
print("Converting V1 CSV to JSON...")
for csv_file in (v1_folder).glob("*.csv"):
    data = read_file(csv_file, low_memory=False)
    json_path = str(csv_file).replace("csv", "json")
    data.to_json(json_path, orient="records")
