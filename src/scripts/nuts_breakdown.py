#!/usr/bin/env python

import os
import sys
from argparse import ArgumentParser
import datacommons as dc

# This script must be run from /src
sys.path.append(os.getcwd())
from lib.io import read_file
from lib.utils import ROOT


# Parse arguments from the command line
argparser = ArgumentParser()
argparser.add_argument("country_code", type=str)
argparser.add_argument("--nuts-level", type=int, default=2)
argparser.add_argument("--dc-api-key", type=str, default=os.environ["DATACOMMONS_API_KEY"])
args = argparser.parse_args()

# Get the country name
aux = read_file(ROOT / "src" / "data" / "metadata.csv").set_index("key")
country_name = aux.loc[args.country_code, "country_name"]

# Convert 2-letter to 3-letter country code
iso_codes = read_file(ROOT / "src" / "data" / "country_codes.csv").set_index("key")
country_code_alpha_3 = iso_codes.loc[args.country_code, "3166-1-alpha-3"]

dc.set_api_key(args.dc_api_key)
country = "country/{}".format(country_code_alpha_3)
nuts_name = "EurostatNUTS{}".format(args.nuts_level)
regions = dc.get_places_in([country], nuts_name)[country]
names = dc.get_property_values(regions, "name")
for key, name in names.items():
    region_name = name[0]
    region_code = key.split("/")[-1][3:]
    print(
        (
            "{country_code}_{region_code},"
            "{country_code},"
            "{country_name},"
            "{region_code},"
            "{region_name},"
            ",,,0"
        ).format(
            **{
                "country_code": args.country_code,
                "region_code": region_code,
                "country_name": country_name,
                "region_name": region_name,
            }
        )
    )
