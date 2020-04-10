#!/usr/bin/env python

import sys
from datetime import datetime
from pandas import DataFrame
from covid_io import read_file
from utils import cumsum, dataframe_output


# Read the ISO mappings for department -> region
iso = read_file(sys.argv[1], table_index=2, header=True)
region_column = [col for col in iso.columns if 'region' in col.lower()][0]
dep_map = {idx[3:]: code for idx, code in zip(iso['Code'], iso[region_column])}

# Read the data from data.gouv.fr
data = read_file(sys.argv[2], sep=';').rename(columns={
    'jour': 'Date',
    'dep': 'RegionName',
    'incid_dc': 'Deaths',
    'incid_rea': 'Critical',
})

# Map the department to the region
data['RegionName'] = data['RegionName'].apply(lambda dep: dep_map.get(dep))

# Estimate confirmed cases from the critical ones
data['Confirmed'] = data['Critical'].apply(lambda x: x / .075)

# Data is new cases, perform the cumsum to get total
keys = ['RegionName', 'Date']
data = cumsum(data.dropna(subset=keys), keys)

# Output the results
dataframe_output(data.reset_index(), 'FR')
