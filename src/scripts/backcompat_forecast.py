#!/usr/bin/env python

import os
import sys
import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm
from scipy import optimize

# This script must be run from /src
sys.path.append(os.getcwd())
from lib.io import read_file
from lib.utils import ROOT


def _get_outbreak_mask(data: pd.DataFrame, threshold: int = 10):
    """ Returns a mask for > N confirmed cases. Used to filter out uninteresting dates """
    return data["Confirmed"] > threshold


def _logistic_function(X: float, a: float, b: float, c: float):
    """
    Used for prediction model. Uses the function:
    `f(x) = a * e^(-b * e^(-cx))`
    """
    return a * np.exp(-b * np.exp(-c * X))


def _forward_indices(indices: list, window: int):
    """ Adds `window` indices to a list of dates """
    date_indices = [datetime.date.fromisoformat(idx) for idx in indices]
    for _ in range(window):
        date_indices.append(date_indices[-1] + datetime.timedelta(days=1))
    return [idx.isoformat() for idx in date_indices]


def _compute_forecast(data: pd.Series, window: int):
    """
    Perform a forecast of `window` days past the last day of `data`, including a model estimate of
    all days already existing in `data`.
    """

    # Some of the parameter fittings result in overflow
    np.seterr(all="ignore")

    # Perform a simple fit of all available data up to this date
    X, y = list(range(len(data))), data.tolist()
    # Providing a reasonable initial guess is crucial for this model
    params, _ = optimize.curve_fit(
        _logistic_function, X, y, maxfev=int(1e6), p0=[max(y), np.median(X), 0.1]
    )

    # Append N new days to our indices
    date_indices = _forward_indices(data.index, window)

    # Perform projection with the previously estimated parameters
    projected = [_logistic_function(x, *params) for x in range(len(X) + window)]
    return pd.Series(projected, index=date_indices, name="Estimated")


def _compute_record_key(record: dict):
    """ Outputs the primary key for a dataframe row """
    region_code = record.get("RegionCode")
    country_code = record["CountryCode"]
    key_suffix = "" if not region_code or pd.isna(region_code) else "_%s" % region_code
    return country_code + key_suffix


def main():
    # Parse parameters
    PREDICT_WINDOW = 7
    DATAPOINT_COUNT = 28 + PREDICT_WINDOW

    # Read data from the open COVID-19 dataset
    df = pd.read_csv(ROOT / "public" / "data_minimal.csv").set_index("Date")

    # Create the output dataframe ahead, we will fill it one row at a time
    forecast_columns = ["ForecastDate", "Date", "Key", "Estimated", "Confirmed"]
    df_forecast = pd.DataFrame(columns=forecast_columns).set_index(["Date", "Key"])

    # Loop through each unique combination of country / region
    for key in tqdm(df["Key"].unique()):

        # Filter dataset
        cols = ["Key", "Confirmed"]
        # Get data only for the selected country / region
        subset = df[df["Key"] == key][cols]
        # Get data only after the outbreak begun
        subset = subset[_get_outbreak_mask(subset)]
        # Early exit: no outbreak found
        if not len(subset):
            continue
        # Get a list of dates for existing data
        date_range = map(
            lambda datetime: datetime.date().isoformat(),
            pd.date_range(subset.index[0], subset.index[-1]),
        )

        # Forecast date is equal to the date of the last known datapoint, unless manually supplied
        forecast_date = subset.index[-1]
        subset = subset[subset.index <= forecast_date].sort_index()

        # Sometimes our data appears to have duplicate values for specific cases, work around that
        subset = subset.query("~index.duplicated()")

        # Early exit: If there are less than DATAPOINT_COUNT output datapoints
        if len(subset) < DATAPOINT_COUNT - PREDICT_WINDOW:
            continue

        # Perform forecast
        forecast_data = _compute_forecast(subset["Confirmed"], PREDICT_WINDOW)

        # Capture only the last DATAPOINT_COUNT days
        forecast_data = forecast_data.sort_index().iloc[-DATAPOINT_COUNT:]

        # Fill out the corresponding index in the output forecast
        for idx in forecast_data.index:
            df_forecast.loc[(idx, key), "ForecastDate"] = forecast_date
            df_forecast.loc[(idx, key), "Estimated"] = forecast_data.loc[idx]
            if idx in subset.index:
                df_forecast.loc[(idx, key), "Confirmed"] = int(subset.loc[idx, "Confirmed"])

    # Do data cleanup here
    data = df_forecast.reset_index()
    forecast_columns = ["ForecastDate", "Date", "Key", "Estimated", "Confirmed"]
    data = data.sort_values(["Key", "Date"])[forecast_columns]

    # Output resulting dataframe
    return data
