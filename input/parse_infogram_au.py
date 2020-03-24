import os
import re
import sys
import json
import time
import datetime
from pathlib import Path

import requests
import pandas as pd

from utils import dataframe_output, merge_previous, timezone_adjust

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

INFOGRAM_AU_SNAPSHOT_URL = 'https://infogram.com/api/live/data/349276203/%s/' % int(time.time())
INFOGRAM_AU_CONFIRMED_URL = 'https://infogram.com/api/live/data/365471073/%s/' % int(time.time())
res = requests.get(INFOGRAM_AU_SNAPSHOT_URL, headers={'User-agent': 'Mozilla/5.0'}).text

def parse_response(res: str):
    res = json.loads(res)
    columns = res['data'][0][0]
    records = res['data'][0][1:]
    records = [{col: val for col, val in zip(columns, record)} for record in records]
    return pd.DataFrame.from_records(records)

df = parse_response(res).rename(columns={
    'State': 'RegionCode',
    'COVID-19 cases': 'Confirmed'
})[['RegionCode', 'Confirmed', 'Deaths']].dropna()

# Extract report date and adjust 7 hour difference between Australia's GMT+11 and GMT+1
res_json = json.loads(res)
df['Date'] = datetime.datetime.fromtimestamp(int(res_json['updated']) // 1000).isoformat()
df['Date'] = df['Date'].apply(lambda date: timezone_adjust(date, 10))


# Merge the new data with the existing data (prefer new data if duplicates)
filter_function = lambda row: row['CountryCode'] == 'AU' and not pd.isna(row['RegionCode'])
df = merge_previous(df, ['Date', 'RegionCode'], filter_function)

# Only keep the necessary columns prior to merging with metadata
df = df[['Date', 'RegionCode', 'Confirmed', 'Deaths']]

# Output the results
dataframe_output(df, ROOT, 'au')