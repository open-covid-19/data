#!/usr/bin/env python

import re
import sys
from pandas import read_csv
from utils import ROOT, datetime_isoformat, read_metadata, safe_int_cast


data = read_csv('https://raw.github.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv')
data = data.drop(columns=['CountryName', 'ConfirmedCases', 'ConfirmedDeaths'])
data = data.drop(columns=[col for col in data.columns if col.endswith('_Notes')])
data = data.drop(columns=[col for col in data.columns if col.endswith('_IsGeneral')])
data['Date'] = data['Date'].apply(lambda x: datetime_isoformat(x, '%Y%m%d'))

# Drop redundant flag columns
data = data.drop(columns=[col for col in data.columns if '_Flag' in col])

# Join with ISO data
iso = read_csv(ROOT / 'input' / 'ISO-3166-2.csv')[['3166-2-Alpha-2', '3166-2-Alpha-3']]
data = data.rename(columns={'CountryCode': '3166-2-Alpha-3'}).merge(iso)

# Join with our metadata
metadata = read_metadata()[['Key', 'CountryCode']]
data = data.rename(columns={'3166-2-Alpha-2': 'CountryCode'}).merge(metadata)

# Use consistent naming convention for columns
data = data[[col for col in data.columns
             if '_' in col or col in ('Date', 'Key', 'StringencyIndex')]]
data.columns = [col.split('_')[-1] for col in data.columns]
data.columns = [re.sub(r'\s(\w)', lambda m: m.group(1).upper(), col) for col in data.columns]

# Fix column typo
data = data.rename(columns={'ClosePublicTransport': 'PublicTransportClosing'})

# Use appropriate data type for each column
for col in data.columns:
    if col in ('Date', 'Key'):
        continue
    elif col in ('MonetaryMeasures', 'StringencyIndex'):
        data[col] = data[col].astype(float)
    else:
        data[col] = data[col].apply(safe_int_cast).astype('Int64')

# Reorder columns and output result
first_columns = ['Date', 'Key']
data = data[first_columns + [col for col in data.columns if col not in first_columns]]
data.sort_values(first_columns).to_csv(sys.stdout, index=False)
