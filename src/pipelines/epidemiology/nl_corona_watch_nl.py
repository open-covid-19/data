from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff

_gh_url_base = "https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data"
_column_mapping = {
    "Datum": "date",
    "Gemeentecode": "subregion2_code",
    "Gemeentenaam": "subregion2_name",
    "Provincienaam": "subregion1_name",
    "Provinciecode": "subregion1_code",
}


class CoronaWatchNlPipeline(DefaultPipeline):
    data_urls: List[str] = [
        "{}/rivm_NL_covid19_total_municipality.csv".format(_gh_url_base),
        "{}/rivm_NL_covid19_fatalities_municipality.csv".format(_gh_url_base),
        "{}/rivm_NL_covid19_hosp_municipality.csv".format(_gh_url_base),
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        count_column = {0: "confirmed", 1: "deceased", 2: "hospitalized"}
        for idx, df in enumerate(dataframes):
            df.rename(columns={**_column_mapping, "Aantal": count_column[idx]}, inplace=True)

        data = dataframes[0]
        for df in dataframes[1:]:
            data = data.merge(df, how="outer")

        # Drop data without a clear demarcation
        data = data[~data.subregion1_code.isna()]
        data = data[~data.subregion1_name.isna()]
        data = data[~data.subregion2_code.isna()]
        data = data[~data.subregion2_name.isna()]

        # Make sure the region code is zero-padded
        data["subregion2_code"] = data["subregion2_code"].apply(lambda x: "{0:04d}".format(int(x)))

        data = data.drop(columns=["subregion1_code", "subregion1_name", "subregion2_name"])
        data = data.merge(aux["metadata"], on="subregion2_code")

        # We only need to keep key-date pair for identification
        data = data[["date", "key", "confirmed", "deceased", "hospitalized"]]

        # Compute the daily counts
        data = grouped_diff(data, ["key", "date"])

        # Group by level 2 region, and add the parts
        l2 = data.copy()
        l2["key"] = l2.key.apply(lambda x: x[:5])
        l2 = l2.groupby(["key", "date"]).sum().reset_index()

        # Output the results
        return concat([l2, data])
