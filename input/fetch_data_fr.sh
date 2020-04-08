#!/bin/bash

# Use opencovid-fr GitHub project to parse the state-level France data

BASE_DIR=`dirname "$0"`
URL_BASE="https://raw.github.com/cedricguadalupe/FRANCE-COVID-19/master"
CONFIRMED=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/france_coronavirus_time_series-confirmed.csv" $@)
DEATHS=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/france_coronavirus_time_series-deaths.csv" $@)
python "$BASE_DIR/parse_fr_france-covid-19.py" "$CONFIRMED" "$DEATHS"