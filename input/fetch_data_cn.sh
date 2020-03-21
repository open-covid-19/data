#!/bin/bash

# Call the DXY scrapping API to get the latest data from China.

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_dxy_api_cn.py"
