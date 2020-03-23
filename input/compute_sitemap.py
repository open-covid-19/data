#!/usr/bin/env python

import os
import sys
from pathlib import Path

# Establish root of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Get a baseline for output folder root
OUTPUT = ROOT / 'output'

# Write a link to each file in the output folder
for fname in OUTPUT.glob('**/*.*'):
    print('- <{0}>'.format(fname.relative_to(OUTPUT)), file=sys.stdout)