import re
from typing import Any, Dict, List, Tuple
from pandas import DataFrame, Int64Dtype, merge
from lib.cast import safe_int_cast
from lib.pipeline import DataPipeline, DefaultPipeline, PipelineChain
from lib.time import datetime_isoformat
from lib.utils import ROOT


class StringencyPipeline(DefaultPipeline):
    data_urls: List[str] = [
        "https://raw.github.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv"
    ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]
        data = data.drop(columns=["CountryName", "ConfirmedCases", "ConfirmedDeaths"])
        data = data.drop(columns=[col for col in data.columns if col.endswith("_Notes")])
        data = data.drop(columns=[col for col in data.columns if col.endswith("_IsGeneral")])
        data["date"] = data["Date"].apply(lambda x: datetime_isoformat(x, "%Y%m%d"))

        # Drop redundant flag columns
        data = data.drop(columns=[col for col in data.columns if "_Flag" in col])

        # Join with ISO data
        data = data.rename(columns={"CountryCode": "3166-1-alpha-3"}).merge(aux["country_codes"])

        # Join with our metadata
        metadata = aux["metadata"][["key", "country_code"]]
        data = data.rename(columns={"3166-1-alpha-2": "country_code"}).merge(metadata)

        # Use consistent naming convention for columns
        data = data[
            [col for col in data.columns if "_" in col or col in ("date", "key", "StringencyIndex")]
        ]
        data.columns = [col.split("_")[-1].lower() for col in data.columns]
        data.columns = [re.sub(r"\s", "_", col) for col in data.columns]

        # Fix column typos
        data = data.rename(
            columns={
                "stringencyindex": "stringency_index",
                "close_public_transport": "public_transport_closing",
                "debt/contract_relief": "debt_relief",
            }
        )

        # Remove unneeded columns
        data = data.drop(columns=["wildcard"])

        # Reorder columns and output result
        first_columns = ["date", "key"]
        data = data[first_columns + [col for col in data.columns if col not in first_columns]]
        return data.sort_values(first_columns)


class StringencyPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "date": str,
        "key": str,
        "school_closing": Int64Dtype(),
        "workplace_closing": Int64Dtype(),
        "cancel_public_events": Int64Dtype(),
        "restrictions_on_gatherings": Int64Dtype(),
        "public_transport_closing": Int64Dtype(),
        "stay_at_home_requirements": Int64Dtype(),
        "restrictions_on_internal_movement": Int64Dtype(),
        "international_travel_controls": Int64Dtype(),
        "income_support": Int64Dtype(),
        "debt_relief": Int64Dtype(),
        "fiscal_measures": float,
        "international_support": Int64Dtype(),
        "public_information_campaigns": Int64Dtype(),
        "testing_policy": Int64Dtype(),
        "contact_tracing": Int64Dtype(),
        "emergency_investment_in_healthcare": Int64Dtype(),
        "investment_in_vaccines": Int64Dtype(),
        "stringency_index": float,
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [(StringencyPipeline(), {})]
