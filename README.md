# Open COVID-19 Dataset
This repository contains datasets of historical data related to COVID-19.

## Explore the data
A simple visualization tool was built to explore the Open COVID-19 datasets:
The [Open COVID-19 Explorer](https://open-covid-19.github.io/explorer).

![Explorer Screenshot](https://github.com/open-covid-19/explorer/raw/master/screenshots/explorer.png)

## Use the data
The data is available as CSV and JSON files, which are published in Github
Pages so they can be served directly to Javascript applications without the
need of a proxy to set the correct headers for CORS and content type. Some
datasets have a version with all historical data, and another version with only
the latest daily data. Some only have the latest data available. The datasets
curated by this project are:

| Dataset | CSV URL | JSON URL |
| ------- | ------- | -------- |
| Data | [Latest](https://open-covid-19.github.io/data/data_latest.csv), [Historical](https://open-covid-19.github.io/data/data.csv) | [Latest](https://open-covid-19.github.io/data/data_latest.json), [Historical](https://open-covid-19.github.io/data/data.json) |
| Forecast | [Latest](https://open-covid-19.github.io/data/forecast_latest.csv) | [Latest](https://open-covid-19.github.io/data/forecast_latest.json) |

## Understand the data
The columns of the main dataset are:

| Name | Description | Example |
| ---- | ----------- | ------- |
| **Date**\* | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-21 |
| **CountryCode** | ISO 3166-1 code of the country | CN |
| **CountryName** | American English name of the country | China |
| **RegionCode** | (Optional) ISO 3166-2 code of the region | HB |
| **RegionName** | (Optional) American English name of the region | Hubei |
| **Confirmed**\*\* | Total number of cases confirmed after positive test | 67800 |
| **Deaths**\*\* | Total number of deaths from a positive COVID-19 case | 3139 |
| **Latitude** | Floating point representing the geographic coordinate | 30.9756 |
| **Longitude** | Floating point representing the geographic coordinate | 112.2707 |
| **Population** | Total count of humans living in the region | TODO |

\*Date used is **reporting** date, which generally lags a day from the actual
date and is subject to timezone adjustments. Whenever possible, dates
consistent with the ECDC daily reports are used.

\*\*Missing values will be represented as nulls, whereas zeroes are used when
a true value of zero is reported. For example, US states where deaths are not
being reported have null values.

For countries where both country-level and region-level data is available, the
entry which has a null value for the `RegionCode` and `RegionName` columns
indicates country-level aggregation. Please note that, sometimes, the
country-level data and the region-level data come from different sources so
adding up all region-level values may not equal exactly to the reported
country-level value. See the [data loading tutorial][7] for more information.

#### Forecasting
There is also a short-term forecast dataset available in the output folder as
[data_forecast.csv](https://open-covid-19.github.io/data/data_forecast.csv),
which has the following columns:

| Name | Description | Example |
| ---- | ----------- | ------- |
| **ForecastDate** | ISO 8601 date (YYYY-MM-DD) of last known datapoint | 2020-03-21 |
| **Date**\* | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-25 |
| **CountryCode** | ISO 3166-1 code of the country | CN |
| **CountryName** | American English name of the country | China |
| **RegionCode** | (Optional) ISO 3166-2 code of the region | HB |
| **RegionName** | (Optional) American English name of the region | Hubei |
| **Estimated**\*\* | Total number of cases estimated from forecasting model | 66804.567 |
| **Confirmed** | Total number of cases confirmed after positive test | 67800 |

\*Date used is **reporting** date, which generally lags a day from the actual
date and is subject to timezone adjustments. Whenever possible, dates
consistent with the ECDC daily reports are used.

\*\*An estimate is also provided for dates before the forecast date, which
corresponds to the output of the fitted model; this is the *a priori*
estimate. True forecast values are those that have a **Date** higher than
**ForecastDate**; which are the *a posteriori* estimates. Another way to
distinguish between *a priori* and *a posteriori* estimates is to see if a
given date has a value for both **Confirmed** and **Estimated** (*a
priori*) or if the **Confirmed** value is null (*a posteriori*).

#### Backwards compatibility
Please note that the following datasets are maintained only to preserve
backwards compatibility, but shouldn't be used in any new projects:
* [World (deprecated version)](output/world_latest.csv)
* [USA (deprecated version)](output/usa_latest.csv)
* [China (deprecated version)](output/china_latest.csv)

## Analyze the data
You may also want to load the data and perform your own analysis on it.
You can find Jupyter Notebooks in the [examples subfolder](examples) with
examples of how to load and analyze the data.

You can even use Google Colab if you want to run your analysis without having
to install anything in your computer, simply go to this URL:
https://colab.research.google.com/github/open-covid-19/data.

## Sources of data
| Data | Source |
| ---- | ------ |
| Country-level worldwide | Daily reports from the [ECDC portal][2] |
| Region-level China | [DXY COVID-19 dataset][3] |
| Region-level USA | [COVID Tracking Project][9] |
| Region-level Australia | <https://covid-19-au.github.io> |
| Country and region-level Italy | [Italy's Department of Civil Protection][10] |
| Country and region-level Spain | [Datadista COVID-19 dataset][11] |
| Country and region-level metadata | [Wikipedia](https://wikipedia.org) |

The data is automatically scraped and parsed using the scripts found in the
[input folder](input). This is done daily, and as part of the processing
some additional columns are added, like region-level coordinates.

Before updating the outputs, data is spot-checked using various data sources
including data from local authorities like [Italy's ministry of health][4] and
the [reports from WHO][5].

## Why another dataset?
This dataset is heavily inspired by the dataset maintained by
[Johns Hopkins University][1]. Unfortunately, that dataset has intermittently
experienced maintenance issues and a lot of applications depend on this
critical data being available in a timely manner. Further, the true sources
of data for that dataset are still unclear.

## Update the data
To update the contents of the [output folder](output), first install the
dependencies:
```sh
# Install Ghostscript
apt-get install -y ghostscript
# Install Python dependencies
pip install -r requirements.txt
```

Then run the following scripts to update all datasets:
```sh
sh input/update_data.sh
```

[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.ecdc.europa.eu
[3]: https://github.com/BlankerL/DXY-COVID-19-Data
[4]: https://web.archive.org/web/20200314143253/http://www.salute.gov.it/nuovocoronavirus
[5]: https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports
[6]: https://github.com/open-covid-19/data/issues/16
[7]: https://github.com/open-covid-19/data/examples/data_loading.ipynb
[8]: https://web.archive.org/web/20200320122944/https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm
[9]: https://covidtracking.com
[10]: https://github.com/pcm-dpc/COVID-19
[11]: https://github.com/datadista/datasets/tree/master/COVID%2019