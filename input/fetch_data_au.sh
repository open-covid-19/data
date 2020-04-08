#!/bin/bash

# Call the covidtracking.com API and parse the state-level Australia data

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/covid-19-au/covid-19-au.github.io/prod/src/data/state.json"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_au_covid-19-au.py" "$SNAPSHOT"
