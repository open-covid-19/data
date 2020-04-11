#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level BO data

BASE_DIR=`dirname "$0"`
URL="https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data/Bolivia_medical_cases"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension html $@)
python "$BASE_DIR"/parse_wikipedia_country.py "$SNAPSHOT" \
    --country-code BO \
    --date-format '%b %d' \
    --skiprows 1 \
    --droprows 'Date(2020)'