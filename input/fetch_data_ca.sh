#!/bin/bash

# Use official government data to fetch and parse the region-level CA data

BASE_DIR=`dirname "$0"`
URL="https://health-infobase.canada.ca/src/data/covidLive/covid19.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_ca_canada.py" "$SNAPSHOT"