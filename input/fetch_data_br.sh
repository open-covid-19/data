#!/bin/bash

# Use Github data to fetch and parse the region-level BR data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_covid19-brazil-timeseries_br.py"