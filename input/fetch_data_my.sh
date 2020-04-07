#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level MY data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR"/parse_wikipedia_country.py MY \
    --article '2020_coronavirus_pandemic_in_Malaysia' \
    --date_format '%d/%m' \
    --table_index 1 \
    --skiprows 0 \
    --skipcols 1 \
    --cumsum true