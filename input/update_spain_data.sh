#!/bin/bash
set -xe

# Crawl the MSCBS daily reports website and fetch the latest report to update
# our local dataset with the changes.

BASE_DIR=`dirname "$0"`
python -m scrapy runspider "$BASE_DIR/crawl_mscbs_data.py" 2> /dev/null | \
    xargs wget -qq --no-check-certificate -O - | \
    gs -sDEVICE=txtwrite -sOutputFile=- -q -dNOPAUSE -dBATCH - | \
    python "$BASE_DIR/parse_mscbs_report_spain.py"
