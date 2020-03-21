#!/bin/bash
set -xe

# Call all the fetch data scripts and aggregate them into a single CSV file

BASE_DIR=`dirname "$0"`

TMP_FILE=`mktemp`
cat "$BASE_DIR/output_columns.csv" > "$BASE_DIR/../output/data.csv"
FETCH_SCRIPT_LIST="$BASE_DIR/fetch_data_*.sh"
for FETCH_SCRIPT in $FETCH_SCRIPT_LIST; do
    sh $FETCH_SCRIPT >> $TMP_FILE
done
sort -u $TMP_FILE >> "$BASE_DIR/../output/data.csv"

# Create the JSON and _latest versions + old versions of datasets
python "$BASE_DIR/backwards_compatibility.py"
