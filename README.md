# Open COVID-19 Dataset
This repo contains free datasets of historical data related to COVID-19.
The current datasets are:
* [World](output/world.csv):
  - Date: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - CountryCode: 2-letter ISO 3166-1 code of the country
  - CountryName: American English name of the country
  - Confirmed: total number of cases confirmed after positive test
  - Deaths: total number of deaths from a positive COVID-19 case
  - Latitude: floatig point representing the geographic coordinate
  - Longitude: floatig point representing the geographic coordinate

* [China](output/china.csv):
  - Date: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - Region: American English name of the province
  - CountryCode: 2-letter ISO 3166-1 code of the country
  - CountryName: American English name of the country
  - Confirmed: total number of cases confirmed after positive test
  - Deaths: total number of deaths from a positive COVID-19 case
  - Latitude: floatig point representing the geographic coordinate
  - Longitude: floatig point representing the geographic coordinate

* [USA](output/usa.csv):
  - Date: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - Region: 2-letter state code (e.g. CA, FL, NY)
  - CountryCode: 2-letter ISO 3166-1 code of the country
  - CountryName: American English name of the country
  - Confirmed: total number of cases confirmed after positive test
  - Deaths: total number of deaths from a positive COVID-19 case
  - Tested: total number of tests performed to determine COVID-19 case
  - Latitude: floatig point representing the geographic coordinate
  - Longitude: floatig point representing the geographic coordinate

## Analyze the data
You can find Jupyter Notebooks in the
[analysis repository](https://github.com/open-covid-19/analysis) with examples
of how to load and analyze the data. You can use Google Colab if you want to 
run your analysis without having to install anything in your computer, simply 
go to this URL: https://colab.research.google.com/github/open-covid-19/analysis

## Why another dataset?
This dataset is heavily inspired by the dataset maintained by 
[Johns Hopkins University][1]. Unfortunately, that dataset has intermittently
experiencing maintenance issues and a lot of applications depend on this 
critical data being available in a timely manner. Further, the true sources
of data for that dataset are still unclear.

## Source of data
The world data comes from the daily reports at the [ECDC portal][2].
The XLS file is downloaded and parsed using `scrapy` and `pandas`.

Data for Chinese regions and Italy (see #12) comes from the 
[DXY scraped dataset][3].

The data is automatically crawled and parsed using the scripts found in the
[input folder](input). This is done daily, and as part of the processing
some additional columns are added, like region-level coordinates.

Before updating the outputs, data is spot-checked using various data sources
including data from local authorities (like 
[this one](https://web.archive.org/web/20200314143253/http://www.salute.gov.it/nuovocoronavirus))
and the [reports from WHO][4].

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
```

None of the update scripts depend on previous days' data, and they can be run
at any cadence without affecting the output.

[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.ecdc.europa.eu
[3]: https://github.com/BlankerL/DXY-COVID-19-Data
[4]: https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports