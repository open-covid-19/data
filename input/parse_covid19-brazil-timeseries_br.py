import os
from pathlib import Path
from datetime import datetime

from pandas import DataFrame

from utils import github_raw_dataframe, dataframe_output

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read data from GitHub repo
confirmed = github_raw_dataframe('elhenrico/covid19-Brazil-timeseries', 'confirmed-cases.csv')
deaths = github_raw_dataframe('elhenrico/covid19-Brazil-timeseries', 'deaths.csv')
for df in (confirmed, deaths):
    df.rename(columns={'Unnamed: 1': 'RegionCode'}, inplace=True)
    df.set_index('RegionCode', inplace=True)

# Transform the data from non-tabulated format to record format
records = []
for region_code in confirmed.index.unique():
    for col in confirmed.columns[1:]:
        date = col + '/' + str(datetime.now().year)
        date = datetime.strptime(date, '%d/%m/%Y').date().isoformat()
        records.append({
            'Date': date,
            'RegionCode': region_code,
            'Confirmed': confirmed.loc[region_code, col],
            'Deaths': confirmed.loc[region_code, col]})
df = DataFrame.from_records(records)

# Output the results
dataframe_output(df, ROOT, 'BR')