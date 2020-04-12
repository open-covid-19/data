#!/bin/bash

# Use opencovid-fr GitHub project to parse the state-level France data

BASE_DIR=`dirname "$0"`

# ISO_URL="https://en.wikipedia.org/wiki/ISO_3166-2:FR"
# DATA_URL_BASE="https://www.data.gouv.fr/fr/datasets/r"
# CONFIRMED_URL="$DATA_URL_BASE/b4ea7b4b-b7d1-4885-a099-71852291ff20"
# DEATHS_URL="$DATA_URL_BASE/6fadff46-9efd-4c53-942a-54aca783c30c"

# ISO=$(python "$BASE_DIR"/download_snapshot.py "$ISO_URL" --extension html $@)
# CONFIRMED=$(python "$BASE_DIR"/download_snapshot.py "$CONFIRMED_URL" --extension csv $@)
# DEATHS=$(python "$BASE_DIR"/download_snapshot.py "$DEATHS_URL" --extension csv $@)
# PREVIOUS=$(python "$BASE_DIR"/download_snapshot.py "https://open-covid-19.github.io/data/data.csv" $@)
# python "$BASE_DIR/parse_fr_gouv.py" "$ISO" "$CONFIRMED" "$DEATHS" "$PREVIOUS"

URL_BASE="https://raw.github.com/cedricguadalupe/FRANCE-COVID-19/master"
CONFIRMED=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/france_coronavirus_time_series-confirmed.csv" $@)
DEATHS=$(python "$BASE_DIR"/download_snapshot.py "$URL_BASE/france_coronavirus_time_series-deaths.csv" $@)
python "$BASE_DIR/parse_fr_france-covid-19.py" "$CONFIRMED" "$DEATHS"