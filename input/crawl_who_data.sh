#!/bin/bash
set -xe

# This script crawls the WHO situation reports website and fetches the last
# report that matches the provided date parameter in the format YYYYMMDD. E.g.:
# > sh crawl_who_data.sh 20200314

BASE_DIR=`dirname "$0"`
python -m scrapy runspider "$BASE_DIR/crawl_who_data.py" -a date=$1 | \
    xargs wget -qq -O - | \
    gs -sDEVICE=txtwrite -sOutputFile=- -q -dNOPAUSE -dBATCH - | \
    python "$BASE_DIR/parse_who_report.py" $1

