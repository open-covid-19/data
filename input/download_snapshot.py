#!/usr/bin/env python

'''
This script downloads a file into the snapshots folder and outputs the hashed
file name based on the input URL. This is used to ensure reproducibility in the
processing scripts, which will not require to network access.
'''

import sys
import uuid
from argparse import ArgumentParser

import requests
from utils import ROOT

# Parse arguments
parser = ArgumentParser()
parser.add_argument('url', type=str)
parser.add_argument('--extension', type=str, default=None)
parser.add_argument('--offline', type=bool, default=False)
args = parser.parse_args(sys.argv[1:])

# Determine output path
url: str = args.url
ext: str = args.extension or url.split('.')[-1]
file_path = ROOT / 'snapshot' / ('%s.%s' % (uuid.uuid5(uuid.NAMESPACE_DNS, url), ext))

# Download the file if online
if not args.offline:
    with open(file_path, 'wb') as file_handle:
        file_handle.write(requests.get(url).content)

# Output the downloaded file path
print(file_path.relative_to(ROOT))