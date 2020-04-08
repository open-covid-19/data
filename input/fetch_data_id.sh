#!/bin/bash

# Use crowd-sourced data to fetch and parse the region-level IT data

BASE_DIR=`dirname "$0"`
URL="https://docs.google.com/spreadsheets/d/1sgiz8x71QyIVJZQguYtG9n6xBEKdM4fXuDs_d8zKOmY/gviz/tq?tqx=out:csv&sheet=Data%20Provinsi"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension csv $@)
python "$BASE_DIR/parse_id_catchmeup.py" "$SNAPSHOT"