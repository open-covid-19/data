#!/bin/bash

# Parse the DPC data from Github for both country and region-level data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_pcm-dpc_it.py" "region"
python "$BASE_DIR/parse_pcm-dpc_it.py" "country"