#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level CL data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR"/parse_wikipedia_country.py CL \
    --date_format '%Y-%m-%d' \
    --table_index 0 \
    --skiprows 1 \
    --null_deaths true \
    --drop_rows "Date"