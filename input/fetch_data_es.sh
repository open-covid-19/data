#!/bin/bash

# Parse the datadista data from Github for both country and region-level data

BASE_DIR=`dirname "$0"`
URL_BASE="https://raw.github.com/datadista/datasets/master"
CONFIRMED=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/COVID%2019/ccaa_covid19_casos_long.csv" $@)
DEATHS=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/COVID%2019/ccaa_covid19_fallecidos_long.csv" $@)
PREVIOUS=$(python "$BASE_DIR"/download_snapshot.py "https://open-covid-19.github.io/data/data.csv" $@)
python "$BASE_DIR/parse_es_datadista.py" "$CONFIRMED" "$DEATHS" "$PREVIOUS"