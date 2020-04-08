#!/bin/bash

# Use @jgehrcke GitHub project to parse the state-level Germany data

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/jgehrcke/covid-19-germany-gae/master/data.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_de_covid-19-germany-gae.py" "$SNAPSHOT"