#!/bin/bash
set -xe

# Crawl the ECDC daily reports website and fetch the latest report to update
# our local dataset with the changes.

BASE_DIR=`dirname "$0"`
python -m scrapy runspider "$BASE_DIR/crawl_ecdc_data.py" 2> /dev/null | \
    xargs wget -qq -O - | \
    python "$BASE_DIR/parse_ecdc_report.py"

