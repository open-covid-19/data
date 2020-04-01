#!/bin/bash

# Use opencovid-fr GitHub project to parse the state-level France data

BASE_DIR=`dirname "$0"`
python "$BASE_DIR/parse_france-covid-19_fr.py"