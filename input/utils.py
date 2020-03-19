from pandas import DataFrame
from pathlib import Path

def dataframe_to_json(data: DataFrame, path: Path, **kwargs):
    with open(path, 'w', encoding='UTF-8') as file:
        data.to_json(file, force_ascii=False, **kwargs)