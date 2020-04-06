#!/bin/bash

# Use GitHub repo data to fetch and parse the region-level MX data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_carranco-sga_mx.py"