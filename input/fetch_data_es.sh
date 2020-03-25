#!/bin/bash

# Parse the datadista data from Github for both country and region-level data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_datadista_es.py" "region"
python "$BASE_DIR/parse_datadista_es.py" "country"