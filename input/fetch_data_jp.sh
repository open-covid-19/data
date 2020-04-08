#!/bin/bash

# Use GitHub data to fetch and parse the region-level JP data

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/swsoyee/2019-ncov-japan/master/Data/byDate.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR"/parse_jp_2019-ncov-japan.py "$SNAPSHOT"