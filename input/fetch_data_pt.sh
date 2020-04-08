#!/bin/bash

# Use Github data to fetch and parse the region-level PT data

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/dssg-pt/covid19pt-data/master/data.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_pt_dssg-pt.py" "$SNAPSHOT"