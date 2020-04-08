#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level CL data

BASE_DIR=`dirname "$0"`
URL="https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data/Chile_medical_cases"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension html $@)
python "$BASE_DIR"/parse_wikipedia_country.py "$SNAPSHOT" \
    --country-code CL \
    --date-format '%Y-%m-%d' \
    --table-index 0 \
    --skiprows 1 \
    --null-deaths true \
    --droprows "Date"
