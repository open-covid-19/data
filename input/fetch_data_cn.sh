#!/bin/bash

# Call the DXY scrapping API to get the latest data from China.

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYArea.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_cn_dxy.py" "$SNAPSHOT"