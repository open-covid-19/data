#!/bin/bash
set -xe

# Crawl the WHO situation reports website and fetch the latest report.

BASE_DIR=`dirname "$0"`
python -m scrapy runspider "$BASE_DIR/crawl_ecdc_data.py" 2> /dev/null | \
    xargs wget -qq -O - | \
    python "$BASE_DIR/parse_ecdc_report.py"

