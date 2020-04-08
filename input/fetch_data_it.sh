#!/bin/bash

# Parse the DPC data from Github for both country and region-level data

BASE_DIR=`dirname "$0"`
URL_BASE="https://raw.github.com/pcm-dpc/COVID-19/master/dati-json"
REGION=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/dpc-covid19-ita-regioni.json" $@)
COUNTRY=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/dpc-covid19-ita-andamento-nazionale.json" $@)
PREVIOUS=$(python "$BASE_DIR"/download_snapshot.py "https://open-covid-19.github.io/data/data.csv" $@)
python "$BASE_DIR/parse_it_pcm-dpc_region.py" "$REGION"
python "$BASE_DIR/parse_it_pcm-dpc_country.py" "$COUNTRY" "$PREVIOUS"