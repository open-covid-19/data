#!/bin/bash

# Crawl the MSCBS daily report website and parse the latest one

BASE_DIR=`dirname "$0"`
python -m scrapy runspider "$BASE_DIR/crawl_mscbs_data.py" 2> /dev/null | \
    xargs wget -qq --no-check-certificate -O - | \
    gs -sDEVICE=txtwrite -sOutputFile=- -q -dNOPAUSE -dBATCH - | \
    python "$BASE_DIR/parse_mscbs_report_es.py"
