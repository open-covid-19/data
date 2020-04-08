#!/bin/bash

# Parse data from the OpenZH GitHub repo

BASE_DIR=`dirname "$0"`
URL="https://raw.github.com/openZH/covid_19/master/COVID19_Fallzahlen_CH_total.csv"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" $@)
python "$BASE_DIR/parse_ch_openzh.py" "$SNAPSHOT"
