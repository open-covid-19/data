#!/bin/bash

# Use opencovid-fr GitHub project to parse the state-level France data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_opencovid19-fr_fr.py"