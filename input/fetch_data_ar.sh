#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level AR data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR"/parse_wikipedia_country.py AR \
    --date_format '%d %b' \
    --table_index 0 \
    --skiprows 1 \
    --cumsum true \
    --drop_rows 'Date'