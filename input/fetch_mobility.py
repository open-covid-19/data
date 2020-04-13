import sys
import json
from functools import reduce
from multiprocessing.pool import ThreadPool

import requests
from tqdm import tqdm
from pandas import concat, DataFrame

from utils import read_metadata


def get_mobility_report(key: str):
    ''' Reads Google's mobility report parsed by @pastelsky '''

    # Load data from GitHub
    url = ('https://pastelsky.github.io/covid-19-mobility-tracker'
           '/output/%s/mobility.json' % key.replace('_', '/'))
    data = json.loads(requests.get(url).text)

    # Remove the outer wrapper
    data = data[list(data.keys())[0]]

    # Get the values out of the inner wrapper
    data_ = {name: values['points'] for name, values in data.items()}

    # Convert each subset to a dataframe
    dfs = [DataFrame.from_dict(data_[name]).rename(columns={'value': name})
           for name in data_.keys()]

    # Merge all datasets together
    data = reduce(lambda df1, df2: df1.merge(df2, on='date'), dfs)

    # Use consistent naming convention for columns
    data.columns = map(lambda name: name[0].upper() + name[1:], data.columns)

    # Add key column and return
    data['Key'] = key
    first_columns = ['Date', 'Key']
    return data[first_columns + list(set(data.columns) - set(first_columns))]


def _get_mobility_report(key: str):
    try:
        return get_mobility_report(key)
    except:
        return DataFrame()


# Load all available keys from metadata and output mobility report all keys
keys = read_metadata().Key.unique()
data = list(tqdm(ThreadPool(4).imap_unordered(_get_mobility_report, keys), total=len(keys)))
concat(data).sort_values(['Date', 'Key']).to_csv(sys.stdout, index=False)
