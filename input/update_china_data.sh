#!/bin/bash
set -xe

# Crawl the WHO situation reports website and fetch the latest report to update
# our local dataset with the changes.

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_dxy_api_china.py"
