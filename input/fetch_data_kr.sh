#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level KR data

BASE_DIR=`dirname "$0"`
URL="https://en.wikipedia.org/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data/South_Korea_medical_cases"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension html $@)
python "$BASE_DIR"/parse_wikipedia_country.py "$SNAPSHOT" \
    --country-code KR \
    --date-format '%Y-%m-%d' \
    --table-index 3 \
    --skiprows 1