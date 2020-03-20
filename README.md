# Open COVID-19 Dataset
This repo contains free datasets of historical data related to COVID-19.

## Explore the data
A simple visualization tool was built to explore the Open COVID-19 datasets:
https://open-covid-19.github.io/explorer/.

![Explorer Screenshot](https://github.com/open-covid-19/explorer/raw/master/screenshots/explorer.png)

## Understand the data
The current datasets and their respective columns are:

* [World](output/world_latest.csv):
  - **Date**: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - **CountryCode**: 2-letter ISO 3166-1 code of the country
  - **CountryName**: American English name of the country
  - **Confirmed**: total number of cases confirmed after positive test
  - **Deaths**: total number of deaths from a positive COVID-19 case
  - **Latitude**: floating point representing the geographic coordinate
  - **Longitude**: floating point representing the geographic coordinate
  - **Population**: total count of humans living in the country

* [China](output/china_latest.csv):
  - **Date**: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - **Region**: American English name of the province
  - **CountryCode**: 2-letter ISO 3166-1 code of the country
  - **CountryName**: American English name of the country
  - **Confirmed**: total number of cases confirmed after positive test
  - **Deaths**: total number of deaths from a positive COVID-19 case
  - **Latitude**: floating point representing the geographic coordinate
  - **Longitude**: floating point representing the geographic coordinate

* [USA](output/usa_latest.csv):
  - **Date**: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - **Region**: 2-letter state code (e.g. CA, FL, NY)
  - **CountryCode**: 2-letter ISO 3166-1 code of the country
  - **CountryName**: American English name of the country
  - **Confirmed**: total number of cases confirmed after positive test
  - **Deaths**: total number of deaths from a positive COVID-19 case
  - **Latitude**: floating point representing the geographic coordinate
  - **Longitude**: floating point representing the geographic coordinate

* [Spain](output/spain_latest.csv):
  - **Date**: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - **Region**: Local name of the province / state
  - **CountryCode**: 2-letter ISO 3166-1 code of the country
  - **CountryName**: American English name of the country
  - **Confirmed**: total number of cases confirmed after positive test
  - **Deaths**: total number of deaths from a positive COVID-19 case
  - **Latitude**: floating point representing the geographic coordinate
  - **Longitude**: floating point representing the geographic coordinate

## Use the data
The data is available as CSV and JSON files, which are published in Github
Pages so they can be served directly to Javascript applications without the
need of a proxy to set the correct headers for CORS and content type. Each
dataset has a version with all historical data, and another version with only
the latest daily data. The datasets currently available are:

* **World**
  - [Historical CSV](https://open-covid-19.github.io/data/world.csv)
  - [Historical JSON](https://open-covid-19.github.io/data/world.json)
  - [Latest CSV](https://open-covid-19.github.io/data/world_latest.csv)
  - [Latest JSON](https://open-covid-19.github.io/data/world_latest.json)

* **China**
  - [Historical CSV](https://open-covid-19.github.io/data/china.csv)
  - [Historical JSON](https://open-covid-19.github.io/data/china.json)
  - [Latest CSV](https://open-covid-19.github.io/data/china_latest.csv)
  - [Latest JSON](https://open-covid-19.github.io/data/china_latest.json)

* **USA**
  - [Historical CSV](https://open-covid-19.github.io/data/usa.csv)
  - [Historical JSON](https://open-covid-19.github.io/data/usa.json)
  - [Latest CSV](https://open-covid-19.github.io/data/usa_latest.csv)
  - [Latest JSON](https://open-covid-19.github.io/data/usa_latest.json)

* **Spain**
  - [Historical CSV](https://open-covid-19.github.io/data/spain.csv)
  - [Historical JSON](https://open-covid-19.github.io/data/spain.json)
  - [Latest CSV](https://open-covid-19.github.io/data/spain_latest.csv)
  - [Latest JSON](https://open-covid-19.github.io/data/spain_latest.json)

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
To update the contents of the [output folder](output), run the following:
```sh
# Install dependencies
pip install -r requirements.txt
# Update world data
sh input/update_world_data.sh
# Update China data
sh input/update_china_data.sh
# Update USA data
sh input/update_usa_data.sh
# Update Spain data
sh input/update_spain_data.sh
```

[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.ecdc.europa.eu
[3]: https://github.com/BlankerL/DXY-COVID-19-Data
[4]: https://web.archive.org/web/20200314143253/http://www.salute.gov.it/nuovocoronavirus
[5]: https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports
[6]: https://github.com/open-covid-19/data/issues/16