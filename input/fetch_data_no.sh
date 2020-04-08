#!/bin/bash

# Use COVID19-EU-Data data to fetch and parse the region-level NO data

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-no.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR"/parse_no_covid19-eu-data.py "$SNAPSHOT"