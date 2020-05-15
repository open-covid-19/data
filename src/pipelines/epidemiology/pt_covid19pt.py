import datetime
from typing import Any, Dict, List
from numpy import unique
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff, pivot_table


class Covid19PtPipeline(DefaultPipeline):
    data_urls: List[str] = ["https://raw.github.com/dssg-pt/covid19pt-data/master/data.csv"]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        df = dataframes[0]
        df["date"] = df["data"].apply(lambda x: datetime_isoformat(x, "%d-%m-%Y"))

        # Extract regions from the data
        regions = [col.split("_")[-1] for col in df.columns if col.startswith("confirmados_")]
        regions = [
            region
            for region in regions
            if len(region) > 2 and region not in ("novos", "estrangeiro")
        ]

        # Aggregate regions into a single data frame
        subsets = []
        for region in regions:
            subset = df[
                ["date"]
                + ["{}_{}".format(col, region) for col in ("confirmados", "obitos", "recuperados")]
            ].copy()
            subset["match_string"] = region.replace("ars", "")
            subset = subset.rename(
                columns={
                    "confirmados_%s" % region: "confirmed",
                    "obitos_%s" % region: "deceased",
                    "recuperados_%s" % region: "recovered",
                }
            )
            subsets.append(subset)
        df = concat(subsets)

        df = grouped_diff(df, ["match_string", "date"])
        df["country_code"] = "PT"
        return df
