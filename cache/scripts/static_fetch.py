#!/usr/bin/env python

"""
This script downloads a resource into the specified output file.
"""

from argparse import ArgumentParser
import requests

# Parse arguments
parser = ArgumentParser()
parser.add_argument("--url", type=str, required=True)
parser.add_argument("--output", type=str, required=True)
args = parser.parse_args()

# Download the resource into the output file
with open(args.output, "wb") as fd:
    fd.write(requests.get(args.url).content)
