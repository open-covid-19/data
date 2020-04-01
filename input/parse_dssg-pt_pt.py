import os
from pathlib import Path
from datetime import datetime

import pandas

from utils import github_raw_dataframe, dataframe_output

# Root path of the project
ROOT = Path(os.path.dirname(__file__)) / '..'

# Read data from GitHub repo
df = github_raw_dataframe('dssg-pt/covid19pt-data', 'data.csv')
df['Date'] = df['data'].apply(lambda date: datetime.strptime(date, '%d-%m-%Y').date().isoformat())

# Extract regions from the data
regions = [col.split('_')[-1] for col in df.columns if col.startswith('confirmados_')]
regions = [region for region in regions if len(region) > 2 and region not in ('novos', 'estrangeiro')]

# Aggregate regions into a single data frame
subsets = []
for region in regions:
    subset = df[['Date', 'confirmados_%s' % region, 'obitos_%s' % region]]
    subset = subset.copy()
    subset['_RegionLabel'] = region.replace('ars', '')
    subset = subset.rename(
        columns={'confirmados_%s' % region: 'Confirmed', 'obitos_%s' % region: 'Deaths'})
    subsets.append(subset)
df = pandas.concat(subsets)

# Output the results
dataframe_output(df, ROOT, 'PT')