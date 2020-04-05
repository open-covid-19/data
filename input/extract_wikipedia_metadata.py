#!/usr/bin/env python

import re
import sys
import pandas
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup

from utils import read_html


# %%
country_code = 'RU'
country_name = 'Russia'

url = 'https://en.wikipedia.org/wiki/ISO_3166-2:' + country_code
def cell_parser(elem, row_idx, col_idx):
    try:
        return elem.find('a').attrs['href']
    except Exception as exc:
        return elem.get_text().strip()

data = read_html(url, table_index=0, header=True, parser=cell_parser)
url_col = '/wiki/English_language'
# url_col = '/wiki/Spanish_language'
# url_col = '/wiki/Russian_language'
data[url_col] = 'https://en.wikipedia.org' + data[url_col]
data['Code'] = data['Code'].apply(lambda x: x[3:])
data = data.iloc[1:]
data


# %%
def extract_coords(soup):
    degs = soup.find_all('span', {'class': 'geo-dec'})[0].get_text()
    if 'S' in degs: degs = '-' + degs
    if 'W' in degs: degs = degs.replace(' ', ' -')
    lat, lon = re.sub(r'[^\d\.\-\s]', '', degs).split(' ')
    return {'Latitude': lat, 'Longitude': lon}


def extract_population(soup):
    sibling = list(filter(lambda elem: 'Total' in elem.get_text(), soup.find_all('th')))[1]
    population = list(sibling.parents)[0].find_all('td')[0].get_text().strip().split(' ')[0]
    population = re.sub(r',', '', population)
    return {'Population': population}


#%%
records = []
for code, url in tqdm(zip(data['Code'], data[url_col]), total=len(data)):
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    record = {
        'Key': country_code + '_' + code,
        'RegionCode': code,
        'RegionName': soup.select_one('h1#firstHeading').get_text().strip().split(', %s' % country_name)[0],
    }
    for extractor in (extract_coords, extract_population):
        try: record = {**record, **extractor(soup)}
        except: pass
    records.append(record)
metadata = pandas.DataFrame.from_records(records)

# %%
metadata['CountryCode'] = country_code
metadata['CountryName'] = country_name
metadata['_RegionLabel'] = None
metadata['Key'] = metadata['CountryCode'] + '_' + metadata['RegionCode']
metadata = metadata[[
    'Key',
    'CountryCode',
    'CountryName',
    'RegionCode',
    'RegionName',
    '_RegionLabel',
    'Latitude',
    'Longitude',
    'Population']]

metadata.sort_values('Key').to_csv(sys.stdout, index=False)