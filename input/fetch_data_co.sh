#!/bin/bash

# Parse the data from infogram for region-level data

BASE_DIR=`dirname "$0"`
URL="https://e.infogram.com/api/live/flex/bc384047-e71c-47d9-b606-1eb6a29962e3/664bc407-2569-4ab8-b7fb-9deb668ddb7a"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension json $@)
python "$BASE_DIR/parse_co_infogram.py" "$SNAPSHOT"