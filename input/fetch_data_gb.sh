#!/bin/bash

# Use Github data to fetch and parse the region-level GB data

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/tomwhite/covid-19-uk-data/master/data/covid-19-indicators-uk.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_gb_covid-19-uk-data.py" "$SNAPSHOT"