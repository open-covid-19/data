#!/bin/bash

# Call the covidtracking.com API and parse the state-level USA data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_covid-19-uk-data_gb.py"
