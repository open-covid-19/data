from datetime import datetime
from typing import Any, Dict, List, Tuple
from pandas import DataFrame, concat, merge
from lib.pipeline import DefaultPipeline
from lib.time import datetime_isoformat
from lib.utils import grouped_diff


class Covid19BrazilTimeseriesPipeline(DefaultPipeline):
    url_base = "https://raw.github.com/elhenrico/covid19-Brazil-timeseries/master"
    data_urls: List[str] = [
        "{}/confirmed-cases.csv".format(url_base),
        "{}/deaths.csv".format(url_base),
        "{}/confirmed-new.csv".format(url_base),
        "{}/deaths-new.csv".format(url_base),
    ]

    def _parse_dataframes(self, dataframes: Tuple[DataFrame, DataFrame], prefix: str):

        # Read data from GitHub repo
        confirmed, deaths = dataframes
        for df in (confirmed, deaths):
            df.rename(columns={"Unnamed: 1": "subregion1_code"}, inplace=True)
            df.set_index("subregion1_code", inplace=True)

        # Transform the data from non-tabulated format to record format
        records = []
        for region_code in confirmed.index.unique():
            if "(" in region_code or region_code == "BR":
                continue
            for col in confirmed.columns[1:]:
                date = col + "/" + str(datetime.now().year)
                date = datetime.strptime(date, "%d/%m/%Y").date().isoformat()
                records.append(
                    {
                        "date": date,
                        "country_code": "BR",
                        "subregion1_code": region_code,
                        prefix + "confirmed": confirmed.loc[region_code, col],
                        prefix + "deceased": deaths.loc[region_code, col],
                    }
                )

        return DataFrame.from_records(records)

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Parse cumsum and daily separately
        data_cum = self._parse_dataframes((dataframes[0], dataframes[1]), "total_").set_index(
            ["date", "country_code", "subregion1_code"]
        )
        data_new = self._parse_dataframes((dataframes[2], dataframes[3]), "new_").set_index(
            ["date", "country_code", "subregion1_code"]
        )
        return data_new.join(data_cum).reset_index()
