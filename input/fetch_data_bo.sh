#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level BO data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR"/parse_wikipedia_country.py BO \
    --date_format '%b %d' \
    --table_index 1 \
    --skiprows 1 \
    --drop_rows 'Date(2020)'