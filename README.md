# Open COVID-19 Dataset
This repo contains free datasets of historical data related to COVID-19.

## Explore the data
A simple visualization tool was built to explore the Open COVID-19 datasets:
https://open-covid-19.github.io/explorer/.

![Explorer Screenshot](https://github.com/open-covid-19/explorer/raw/master/screenshots/explorer.png)

## Use the data
The data is available as CSV and JSON files, which are published in Github
Pages so they can be served directly to Javascript applications without the
need of a proxy to set the correct headers for CORS and content type. Each
dataset has a version with all historical data, and another version with only
the latest daily data. The datasets currently available are:

| Dataset | CSV URL | JSON URL |
| ------- | ------- | -------- |
| Data | [Latest](https://open-covid-19.github.io/data/data_latest.csv), [Historical](https://open-covid-19.github.io/data/data.csv) | [Latest](https://open-covid-19.github.io/data/data_latest.json), [Historical](https://open-covid-19.github.io/data/data.json) |

## Understand the data
The columns of the datasets are:

| Name | Description | Example |
| ---- | ----------- | ------- |
| **Date** | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-21 |
| **RegionCode** | (Optional) ISO 3166-2 code of the region | HB |
| **RegionName** | (Optional) American English name of the region | Hubei |
| **CountryCode** | ISO 3166-1 code of the country | CN |
| **CountryName** | American English name of the country | China |
| **Confirmed** | Total number of cases confirmed after positive test | 67800 |
| **Deaths** | Total number of deaths from a positive COVID-19 case | 3139 |
| **Latitude** | Floating point representing the geographic coordinate | 30.9756 |
| **Longitude** | Floating point representing the geographic coordinate | 112.2707 |
| **Population** | Total count of humans living in the region | TODO |

For countries were both country-level and region-level data is available, the
entry which has a null value for the `RegionCode` and `RegionName` columns
indicates country-level aggregation. Please note that, sometimes, the
country-level data and the region-level data come from different sources so
adding up all region-level values may not equal exactly to the reported
country-level value.

#### Backwards compatibility
Please note that the following datasets are maintained only to preserve
backwards compatibility, but shouldn't be used in any new projects:
* [World (deprecated version)](output/world_latest.csv)
* [USA (deprecated version)](output/usa_latest.csv)
* [China (deprecated version)](output/china_latest.csv)

## Analyze the data
You may also want to load the data and perform your own analysis on it.
You can find Jupyter Notebooks in the
[analysis repository](https://github.com/open-covid-19/analysis) with examples
of how to load and analyze the data.

You can even use Google Colab if you want to run your analysis without having
to install anything in your computer, simply go to this URL:
https://colab.research.google.com/github/open-covid-19/analysis.

## Forecasting
There are also short-term forecasting datasets available in the
[forecasting repository](https://github.com/open-covid-19/forecasting), which
includes datasets of future predicted confirmed cases.

## Source of data
The world data comes from the daily reports at the [ECDC portal][2].
The XLS file is downloaded and parsed using `scrapy` and `pandas`.

Data for Chinese regions and Italy (see [#12][6]) comes from the
[DXY scraped dataset][3], which is parsed using `pandas`.

The data is automatically crawled and parsed using the scripts found in the
[input folder](input). This is done daily, and as part of the processing
some additional columns are added, like region-level coordinates.

Before updating the outputs, data is spot-checked using various data sources
including data from local authorities like [Italy's ministry of health][4] and
the [reports from WHO][5].

## Why another dataset?
This dataset is heavily inspired by the dataset maintained by
[Johns Hopkins University][1]. Unfortunately, that dataset has intermittently
experiencing maintenance issues and a lot of applications depend on this
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