#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level AR data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_wikipedia_ar.py"