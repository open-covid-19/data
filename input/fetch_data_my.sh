#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level MY data

BASE_DIR=`dirname "$0"`
URL="https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Malaysia"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension html $@)
python "$BASE_DIR"/parse_wikipedia_country.py "$SNAPSHOT" \
    --country-code MY \
    --date-format '%d/%m' \
    --skiprows 0 \
    --skipcols 1 \
    --cumsum