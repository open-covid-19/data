#!/usr/bin/env python

import re
import sys
from pandas import read_csv, concat
from utils import read_metadata

metadata = read_metadata()
url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
data = read_csv(url).rename(columns={'date': 'Date', 'country_region_code': 'CountryCode'})

# Process the countries first
countries = data[data.sub_region_1.isna()].merge(metadata[metadata.RegionCode.isna()])

# Then process regional data for the records we can match
regions = []
for match_column in ('RegionName', '_RegionLabel1', '_RegionLabel2'):
    data_ = data[~data.sub_region_1.isna()]
    data_ = data_.rename(columns={'sub_region_1': match_column})
    regions.append(data_.merge(metadata[~metadata.RegionCode.isna()]))
regions = concat(regions)

# Put all records back together
data = concat([countries, regions])

# Make sure column names are consistent
column_canary = '_percent_change_from_baseline'
data = data[['Date', 'Key'] + [col for col in data.columns if column_canary in col]]
columns = [col.replace(column_canary, '') for col in data.columns]
columns = [re.sub(r'_(\w)', lambda m: m.group(1).upper(), col) for col in columns]
columns = [col[0].upper() + col[1:] for col in columns]
data.columns = columns

# Load all available keys from metadata and output mobility report all keys
data.sort_values(['Date', 'Key']).to_csv(sys.stdout, index=False)
