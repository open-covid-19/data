#!/bin/bash

# Use GitHub repo data to fetch and parse the region-level MX data

BASE_DIR=`dirname "$0"`
URL="https://raw.githubusercontent.com/carranco-sga/Mexico-COVID-19/master/Mexico_COVID19_CTD.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR"/parse_mx_mexico-covid-19.py "$SNAPSHOT"