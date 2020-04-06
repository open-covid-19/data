#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level IN data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR"/parse_wikipedia_country.py IN \
    --date_format '%b-%d' \
    --table_index 0 \
    --skip_head 1