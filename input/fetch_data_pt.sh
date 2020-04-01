#!/bin/bash

# Use Github data to fetch and parse the region-level PT data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_dssg-pt_pt.py"
