#!/bin/bash
set -xe

# Call all the fetch data scripts and aggregate them into a single CSV file

BASE_DIR=`dirname "$0"`

# Delete all prior data from output folder
rm -rf "$BASE_DIR/../output/*.csv" "$BASE_DIR/../output/*.json"

# Run all fetch scripts from the input folder
TMP_FILE=`mktemp`
cat "$BASE_DIR/output_columns.csv" > "$BASE_DIR/../output/data.csv"
FETCH_SCRIPT_LIST="$BASE_DIR/fetch_data_*.sh"
for FETCH_SCRIPT in $FETCH_SCRIPT_LIST; do
    sh $FETCH_SCRIPT >> $TMP_FILE
done

# Aggregate all outputs into a single CSV file
sort -u $TMP_FILE >> "$BASE_DIR/../output/data.csv"
