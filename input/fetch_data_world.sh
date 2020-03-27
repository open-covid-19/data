#!/bin/bash

# Crawl the ECDC daily reports website and parse the latest report

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_ecdc_world.py"