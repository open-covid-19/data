import os
from pathlib import Path
from datetime import datetime

from pandas import DataFrame

from utils import github_raw_dataframe, dataframe_output, merge_previous

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'
df = github_raw_dataframe(
    'covid-19-au/covid-19-au.github.io', 'src/data/state.json', branch='prod').transpose()

# Transform the data from non-tabulated format to record format
records = []
for idx, row in df.iterrows():
    for code in df.columns:
        data = row[code]
        record = {'Date': idx.date().isoformat(), 'RegionCode': code, 'Confirmed': data[0]}
        if len(data) > 1: record['Deaths'] = data[1]
        if len(data) > 2: record['Recovered'] = data[2]
        if len(data) > 3: record['Tested'] = data[3]
        records.append(record)
df = DataFrame.from_records(records)

# Output the results
dataframe_output(df, ROOT, 'AU')