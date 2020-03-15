#!/bin/bash
set -xe

# Crawl the WHO situation reports website and fetch the latest report to update
# our local dataset with the changes.

BASE_DIR=`dirname "$0"`
python -m scrapy runspider "$BASE_DIR/crawl_who_data.py" | \
    xargs wget -qq -O - | \
    gs -sDEVICE=txtwrite -sOutputFile=- -q -dNOPAUSE -dBATCH - | \
    python "$BASE_DIR/parse_who_report.py"

