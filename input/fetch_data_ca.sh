#!/bin/bash

# Use official government data to fetch and parse the region-level CA data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_canada_ca.py"