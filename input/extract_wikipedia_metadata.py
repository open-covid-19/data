#!/usr/bin/env python

import re
import sys
import pandas
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


# %%
BASE_URL = 'https://en.wikipedia.org'
def fetch_url(path: str, base: str = BASE_URL):
    return BeautifulSoup(requests.get(BASE_URL + path).content, 'lxml')


# %%
path_table = '/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data/Russia_medical_cases'
table = fetch_url(path_table).find_all('table', {'class': 'wikitable'})[1]


# %%
header = table.find_all('tr')[1]
region_names = list(map(lambda elem: elem.get_text().strip(), header.find_all('th', {'class': 'unsortable'})))
links = list(map(lambda elem: elem.find('a').attrs['href'], header.find_all('th', {'class': 'unsortable'})))


# %%
def extract_coords(soup):
    degs = soup.find_all('span', {'class': 'geo-dec'})[0].get_text()
    if 'S' in degs: degs = '-' + degs
    if 'W' in degs: degs = degs.replace(' ', ' -')
    lat, lon = re.sub(r'[^\d\.\-\s]', '', degs).split(' ')
    return {'Latitude': lat, 'Longitude': lon}


# %%
def extract_ISO3166(soup):
    sibling = soup.find_all('a', {'title': 'ISO 3166'})[0]
    code = list(sibling.parents)[1].find_all('td')[0].get_text().strip().split('-')[-1]
    return {'RegionCode': code}


# %%
def extract_population(soup):
    sibling = list(filter(lambda elem: 'Estimate' in elem.get_text(), soup.find_all('th')))[0]
    population = list(sibling.parents)[0].find_all('td')[0].get_text().strip().split(' ')[0]
    population = re.sub(r',', '', population)
    return {'Population': population}


# %%
records = []
for name, link in tqdm(zip(region_names, links), total=len(region_names)):
    soup = fetch_url(link)
    record = {'RegionName': name}
    for extractor in (extract_ISO3166, extract_coords, extract_population):
        try: record = {**record, **extractor(soup)}
        except: pass
    records.append(record)
metadata = pandas.DataFrame.from_records(records)


# %%
metadata['CountryCode'] = 'RU'
metadata['CountryName'] = 'Russia'
metadata['_RegionLabel'] = metadata['RegionName']
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

metadata.to_csv(sys.stdout, index=False)


