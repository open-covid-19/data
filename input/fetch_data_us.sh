#!/bin/bash

# Call the covidtracking.com API and parse the state-level USA data

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/COVID19Tracking/covid-tracking-data/master/data/states_daily_4pm_et.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_us_covidtracking.py" "$SNAPSHOT"