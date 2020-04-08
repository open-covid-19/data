#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level AR data

BASE_DIR=`dirname "$0"`
URL="https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data/Argentina_medical_cases"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension html $@)
python "$BASE_DIR"/parse_wikipedia_country.py "$SNAPSHOT" \
    --country-code AR \
    --date-format '%d %b' \
    --table-index 0 \
    --skiprows 1 \
    --cumsum true \
    --droprows 'Date'