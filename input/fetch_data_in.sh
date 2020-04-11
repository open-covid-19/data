#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level IN data

BASE_DIR=`dirname "$0"`
# URL="https://en.wikipedia.org/wiki/Template:2019–20_coronavirus_pandemic_data/India_medical_cases"
URL="https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_India"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension html $@)
python "$BASE_DIR"/parse_wikipedia_country.py "$SNAPSHOT" \
    --country-code IN \
    --date-format '%b-%d' \
    --skiprows 1