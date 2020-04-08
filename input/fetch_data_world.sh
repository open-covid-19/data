#!/bin/bash

# Crawl the ECDC daily reports website and parse the latest report

BASE_DIR=`dirname "$0"`
URL="https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension csv $@)
python "$BASE_DIR/parse_world_ecdc.py" "$SNAPSHOT"