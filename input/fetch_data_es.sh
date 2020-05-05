#!/bin/bash

# Parse the datadista data from Github for both country and region-level data

BASE_DIR=`dirname "$0"`
URL="https://covid19.isciii.es/resources/serie_historica_acumulados.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_es_iscii.py" "$SNAPSHOT"