#!/bin/bash

# Use Wikipedia data to fetch and parse the region-level PE data

BASE_DIR=`dirname "$0"`
URL="https://es.wikipedia.org/wiki/Pandemia_de_enfermedad_por_coronavirus_de_2020_en_Per%C3%BA"
SNAPSHOT=$(python "$BASE_DIR"/download_snapshot.py "$URL" --extension html $@)
python "$BASE_DIR"/parse_wikipedia_country.py "$SNAPSHOT" \
    --locale 'es_ES' \
    --country-code 'PE' \
    --date-format '%d de %B' \
    --table-index 1 \
    --skiprows 1