#!/usr/bin/env python

'''
This script loads the latest CSV from CDC's website and extracts the confirmed
cases for each state. The output is saved both in CSV and JSON format under the 
`output` folder.
'''

import os
import sys
import pandas as pd
from io import BytesIO
from pathlib import Path
import tempfile

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read CSV file from CDC's website
df = pd.read_csv('https://www.cdc.gov/coronavirus/2019-ncov/map-data-cases.csv')

# TODO: find a better source, CDC's does not contain state-level death data