#!/bin/bash

# Use Github data to fetch and parse the region-level BR data

BASE_DIR=`dirname "$0"`
URL_BASE="https://raw.github.com/elhenrico/covid19-Brazil-timeseries/master"
CONFIRMED=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/confirmed-cases.csv" $@)
DEATHS=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/deaths.csv" $@)
python "$BASE_DIR/parse_br_covid19-brazil-timeseries.py" "$CONFIRMED" "$DEATHS"