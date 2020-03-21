#!/usr/bin/env python

'''
This script performs some operations on data to ensure backwards-compatibility.
'''

import os
import shutil
from pathlib import Path

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# china* -> cn* and Region -> RegionName
shutil.copyfile(ROOT / 'output' / 'cn.csv', ROOT / 'output' / 'china.csv')
shutil.copyfile(ROOT / 'output' / 'cn_latest.csv', ROOT / 'output' / 'china_latest.csv')
shutil.copyfile(ROOT / 'output' / 'cn.json', ROOT / 'output' / 'china.json')
shutil.copyfile(ROOT / 'output' / 'cn_latest.json', ROOT / 'output' / 'china_latest.json')
for fname in ('china.csv', 'china_latest.csv', 'china.json', 'china_latest.json'):
    fh_in = open(ROOT / 'output' / fname, 'rt')
    data = fh_in.read().replace('RegionName', 'Region')
    fh_in.close()
    fh_out = open(ROOT / 'output' / fname, 'wt')
    fh_out.write(data)
    fh_out.close()

# usa* -> us* and RegionName -> RegionCode
shutil.copyfile(ROOT / 'output' / 'us.csv', ROOT / 'output' / 'usa.csv')
shutil.copyfile(ROOT / 'output' / 'us_latest.csv', ROOT / 'output' / 'usa_latest.csv')
shutil.copyfile(ROOT / 'output' / 'us.json', ROOT / 'output' / 'usa.json')
shutil.copyfile(ROOT / 'output' / 'us_latest.json', ROOT / 'output' / 'usa_latest.json')
for fname in ('usa.csv', 'usa_latest.csv', 'usa.json', 'usa_latest.json'):
    fh_in = open(ROOT / 'output' / fname, 'rt')
    data = fh_in.read().replace('RegionCode', 'Region')
    fh_in.close()
    fh_out = open(ROOT / 'output' / fname, 'wt')
    fh_out.write(data)
    fh_out.close()
