import os
from pathlib import Path
from pandas import DataFrame


ROOT = Path(os.path.dirname(__file__)) / '..' / '..'


def pivot_table(data: DataFrame, pivot_name: str = 'pivot'):
    ''' Put a table in our preferred format when the regions are columns and date is index '''
    dates = data.index.tolist() * len(data.columns)
    pivots = sum([[name] * len(column) for name, column in data.iteritems()], [])
    values = sum([column.tolist() for name, column in data.iteritems()], [])
    records = zip(dates, pivots, values)
    return DataFrame.from_records(records, columns=['date', pivot_name, 'value'])
