#!/bin/bash

# Use Github data to fetch and parse the region-level GB data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_covid-19-uk-data_gb.py"
