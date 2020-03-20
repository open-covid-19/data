import sys
import datetime
from io import StringIO
import requests
import pandas as pd

df = pd.read_csv(StringIO(requests.get(
    'https://covid19.isciii.es/resources/data.csv', headers={'User-agent': 'Mozilla/5.0'}).text))
df = df.rename(columns={'Casos': 'Confirmed', 'Defunciones': 'Deaths'})
df['CountryCode'] = 'ES'
df['Date'] = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
df = df.merge(pd.read_csv('country_coordinates.csv', dtype=str))
df = df[['Date', 'CountryCode', 'CountryName', 'Confirmed', 'Deaths', 'Latitude', 'Longitude']]
df.to_csv(sys.stdout, header=False, index=False)