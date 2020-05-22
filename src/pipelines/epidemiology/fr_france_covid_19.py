import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_cumsum


class FranceCovid19Pipeline(DefaultPipeline):
    url_base = "https://raw.github.com/cedricguadalupe/FRANCE-COVID-19/master"
    data_urls: List[str] = [
        "{}/france_coronavirus_time_series-confirmed.csv".format(url_base),
        "{}/france_coronavirus_time_series-deaths.csv".format(url_base),
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        confirmed, deaths = dataframes
        for df in (confirmed, deaths):
            df.rename(columns={"Date": "date"}, inplace=True)
            df.set_index("date", inplace=True)

        # Iterate through all date-region combinations
        regions = confirmed.columns.tolist()
        df = DataFrame(columns=["date", "match_string", "confirmed", "deceased"]).set_index(
            ["date", "match_string"]
        )
        for region in regions:
            if region == "Total":
                continue
            for date, value in zip(confirmed.index, confirmed[region]):
                df.loc[(date, region), "confirmed"] = value
            for date, value in zip(deaths.index, deaths[region]):
                df.loc[(date, region), "deceased"] = value

        # Dates need converted to ISO format
        df = df.sort_values(["date", "match_string"]).reset_index()
        df["date"] = df["date"].apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))

        # Output the results
        df = grouped_cumsum(df, ["match_string", "date"])
        df["subregion2_code"] = None
        df["country_code"] = "FR"
        return df
