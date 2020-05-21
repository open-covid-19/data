#!/usr/bin/env python

"""
This script downloads a resource into the specified output file.
"""

import json
from argparse import ArgumentParser
import requests

# Parse arguments
parser = ArgumentParser()
parser.add_argument("--url", type=str, required=True)
parser.add_argument("--output", type=str, required=True)
parser.add_argument("--method", type=str, default="GET")
parser.add_argument("--headers", type=str, default="{}")
parser.add_argument("--data", type=str, default="{}")
args = parser.parse_args()

# Download the resource into the output file
output = requests.request(
    args.method, args.url, headers=json.loads(args.headers), data=args.data
).content

with open(args.output, "wb") as fd:
    fd.write(output)
