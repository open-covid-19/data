#!/usr/bin/env python
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re
import json
import shutil
import datetime
from pathlib import Path
from functools import partial

from tqdm import tqdm
from pandas import DataFrame

from lib.concurrent import thread_map
from lib.forecast import main as build_forecast
from lib.io import read_file, export_csv
from lib.utils import ROOT, drop_na_records


def snake_to_camel_case(txt: str) -> str:
    """ Used to convert V2 column names to V1 column names for backwards compatibility """
    return re.sub(r"_(\w)", lambda m: m.group(1).upper(), txt.capitalize())


def camel_to_snake_case(txt: str) -> str:
    """ Used to convert V1 column names to V2 column names for backwards compatibility """
    txt = txt[0].lower() + txt[1:]
    return re.sub(r"([A-Z])", lambda m: "_" + m.group(1).lower(), txt)


def subset_last_days(output_folder: Path, days: int) -> None:
    """ Outputs last N days of data """
    n_days_folder = output_folder / str(days)
    n_days_folder.mkdir(exist_ok=True)
    for csv_file in (output_folder).glob("*.csv"):
        table = read_file(csv_file, low_memory=False)

        # Degenerate case: this table has no date
        if not "date" in table.columns or len(table.date.dropna()) == 0:
            export_csv(table, n_days_folder / csv_file.name)
        else:
            last_date = datetime.date.fromisoformat(max(table.date))
            # Since APAC is almost always +1 days ahead, increase the window by 1
            first_date = last_date - datetime.timedelta(days=days + 1)
            export_csv(table[table.date >= first_date.isoformat()], n_days_folder / csv_file.name)


def subset_latest(output_folder: Path, csv_file: Path) -> DataFrame:
    """ Outputs latest data for each key """
    latest_folder = output_folder / "latest"
    latest_folder.mkdir(exist_ok=True)
    table = read_file(csv_file, low_memory=False)

    # Degenerate case: this table has no date
    if not "date" in table.columns or len(table.date.dropna()) == 0:
        return export_csv(table, latest_folder / csv_file.name)
    else:
        non_null_columns = [col for col in table.columns if not col in ("key", "date")]
        table = table.dropna(subset=non_null_columns, how="all")
        table = table.sort_values("date").groupby("key").last().reset_index()
        export_csv(table, latest_folder / csv_file.name)


def subset_grouped_key(table_indexed: DataFrame, output_folder: Path, key: str) -> None:
    """ Outputs a subset of the table with only records with the given key """
    key_folder = output_folder / key
    key_folder.mkdir(exist_ok=True)
    export_csv(table_indexed.loc[key:key].reset_index(), key_folder / "master.csv")


def export_json_without_index(csv_file: Path) -> None:
    table = read_file(csv_file, low_memory=False)
    json_path = str(csv_file).replace("csv", "json")
    json_dict = json.loads(table.to_json(orient="split"))
    del json_dict["index"]
    with open(json_path, "w") as fd:
        json.dump(json_dict, fd)


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
for output_file in tqdm([*(ROOT / "output" / "tables").glob("*.csv")], desc="Copy tables"):
    shutil.copy(output_file, v2_folder / output_file.name)

# Merge all output files into a single master table
master = read_file(v2_folder / "index.csv")
exclude_from_master = (
    "master.csv",
    "index.csv",
    "worldbank.csv",
    "worldpop.csv",
    "by-age.csv",
    "by-sex.csv",
)
for output_file in tqdm([*v2_folder.glob("*.csv")], desc="Master table"):
    if output_file.name not in exclude_from_master:
        master = master.merge(read_file(output_file, low_memory=False), how="left")

# # Drop rows without a single dated record
export_csv(master.dropna(subset=["date"]), v2_folder / "master.csv")

# # Create subsets with the last 30, 14 and 7 days of data
map_func = partial(subset_last_days, v2_folder)
for _ in thread_map(map_func, (30, 14, 7), desc="Last N days subsets"):
    pass

# Create a subset with the latest known day of data for each key
map_func = partial(subset_latest, v2_folder)
for _ in thread_map(map_func, [*(v2_folder).glob("*.csv")], desc="Latest subset"):
    pass

# Create subsets with each known key
master_indexed = master.set_index("key")
map_func = partial(subset_grouped_key, master_indexed, v2_folder)
for _ in thread_map(map_func, master_indexed.index.unique(), desc="Grouped key subsets"):
    pass

# Convert all CSV files to JSON using values format
map_func = export_json_without_index
for _ in thread_map(map_func, [*(v2_folder).glob("**/*.csv")], desc="JSON conversion"):
    pass

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
latest = master[master.aggregation_level < 2]
latest = latest.sort_values("date").groupby("key").last().reset_index()
latest = latest[rename_columns.keys()].rename(columns=rename_columns)
latest = latest.dropna(subset=["Confirmed", "Deaths"], how="all")
latest = latest.sort_values(["Key", "Date"])
export_csv(latest, v1_folder / "data_latest.csv")

# Create the v1 weather.csv file
weather = read_file(v2_folder / "weather.csv")
weather = weather[weather.key.apply(lambda x: len(x.split("_")) < 3)]
weather = weather.rename(columns={"noaa_distance": "distance", "noaa_station": "station"})
rename_columns = {col: snake_to_camel_case(col) for col in weather.columns}
export_csv(weather.rename(columns=rename_columns), v1_folder / "weather.csv")

# Create the v1 mobility.csv file
mobility = read_file(v2_folder / "mobility.csv")
mobility = mobility[mobility.key.apply(lambda x: len(x.split("_")) < 3)]
mobility = drop_na_records(mobility, ["date", "key"])
rename_columns = {col: snake_to_camel_case(col).replace("Mobility", "") for col in mobility.columns}
export_csv(mobility.rename(columns=rename_columns), v1_folder / "mobility.csv")

# Create the v1 CSV files which only require simple column mapping
v1_v2_name_map = {"response": "oxford-government-response"}
for v1_name, v2_name in v1_v2_name_map.items():
    data = read_file(v2_folder / f"{v2_name}.csv")
    rename_columns = {col: snake_to_camel_case(col) for col in data.columns}
    export_csv(data.rename(columns=rename_columns), v1_folder / f"{v1_name}.csv")

# Create the v1 forecast.csv file
export_csv(
    build_forecast(read_file(v1_folder / "data_minimal.csv")), v1_folder / "data_forecast.csv"
)

# Convert all v1 CSV files to JSON using record format
for csv_file in tqdm([*(v1_folder).glob("*.csv")], desc="V1 JSON conversion"):
    data = read_file(csv_file, low_memory=False)
    json_path = str(csv_file).replace("csv", "json")
    data.to_json(json_path, orient="records")
