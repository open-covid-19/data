#!/usr/bin/env python

from datetime import datetime
from pandas import DataFrame
from covid_io import read_argv
from utils import dataframe_output, merge_previous


data = read_argv().transpose()

# Transform the data from non-tabulated format to record format
records = []
for idx, row in data.iterrows():
    for code in data.columns:
        subset = row[code]
        record = {'Date': idx.date().isoformat(), 'RegionCode': code, 'Confirmed': subset[0]}
        if len(subset) > 1:
            record['Deaths'] = subset[1]
        if len(subset) > 2:
            record['Recovered'] = subset[2]
        if len(subset) > 3:
            record['Tested'] = subset[3]
        records.append(record)
data = DataFrame.from_records(records)

# Output the results
dataframe_output(data, 'AU')
