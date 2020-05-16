from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff


class CovidTrackingPipeline(DefaultPipeline):
    data_urls: List[str] = [
        "https://raw.github.com/COVID19Tracking/covid-tracking-data/master/data/states_daily_4pm_et.csv"
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "date": "date",
                "state": "subregion1_code",
                "positive": "confirmed",
                "death": "deceased",
                "total": "tested",
                "recovered": "recovered",
            }
        )

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y%m%d"))

        # Keep only columns we can process
        data['key'] = 'US_' + data['subregion1_code']
        data = data[["date", 'key', "confirmed", "deceased", "tested", "recovered"]]

        # Output the results
        return grouped_diff(data, ["key", "date"])
