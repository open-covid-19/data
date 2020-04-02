#!/usr/bin/env python

import re
import sys
from datetime import datetime

import pandas
import requests
from bs4 import BeautifulSoup

from utils import github_raw_dataframe, dataframe_output


URL_WIKI_BASE = 'https://en.wikipedia.org'
def fetch_url(path: str, base: str = URL_WIKI_BASE):
    ''' Helper function used to extract the contents from a URL '''
    return BeautifulSoup(requests.get(URL_WIKI_BASE + path).content, 'lxml')

# Get the table with confirmed cases
path_table = '/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data/Russia_medical_cases'
table = fetch_url(path_table).find_all('table', {'class': 'wikitable'})[1]
rows = table.find_all('tr')[1:]

# Extract the header row as the region names
header = rows[0]
region_names = header.find_all('th', {'class': 'unsortable'})
region_names = map(lambda elem: elem.get_text().strip(), region_names)
region_names = list(region_names)

# Get dates column
dates = map(lambda row: row.find_all('th')[0].get_text(), rows[1:-1])
# Add the current year to the end of the column
dates = map(lambda date: date.strip() + ' ' + str(datetime.now().year), dates)
# Convert the dates to ISO format
dates = map(lambda date: datetime.strptime(date, '%d %b %Y').date().isoformat(), dates)
# Put the dates into a list
dates = list(dates)

records = []
# Go through the table column by column
for idx, name in enumerate(region_names):
    # Perform the cumsum operation as we roll through the column values
    current_value = 0
    confirmed = map(lambda row: row.find_all('td')[idx].get_text(), rows[1:-1])
    confirmed = map(lambda value: re.sub('\D', '', value), confirmed)
    confirmed = map(lambda value: 0 if not value else int(value), confirmed)
    # Iterate each value to build individual records
    for date, value in zip(dates, confirmed):
        current_value += value
        records.append({'Date': date, '_RegionLabel': name, 'Confirmed': current_value})

# Convert the records into a dataframe
df = pandas.DataFrame.from_records(records)

# We don't have death information (yet), so fill with NA
df['Deaths'] = None

# Reindex and sort the data
df = df.sort_values(['Date', '_RegionLabel'])

# Output the results
dataframe_output(df, 'RU')