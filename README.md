# Open COVID-19 Dataset
This repo contains free datasets of historical data related to COVID-19.
The current datasets are:
* [Worldwide cases](output/world.csv):
  - Date: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - CountryCode: ISO 3166-1 alpha-2 code of the country
  - CountryName: American English name of the country
  - Confirmed: total number of cases confirmed after positive test
  - Deaths: total number of deaths from a positive COVID-19 case
  - Latitude: floatig point representing the geographic coordinate
  - Longitude: floatig point representing the geographic coordinate

* [China cases](output/china.csv):
  - Date: ISO 8601 date (YYYY-MM-DD) of the datapoint
  - Region: American English name of the province
  - CountryCode: ISO 3166-1 alpha-2 code of the country
  - CountryName: American English name of the country
  - Confirmed: total number of cases confirmed after positive test
  - Deaths: total number of deaths from a positive COVID-19 case
  - Latitude: floatig point representing the geographic coordinate
  - Longitude: floatig point representing the geographic coordinate

## Analyze the data
You can find Jupyter Notebooks in the [analysis folder](input) with examples
of how to load and analyze the data. You can use Google Colab if you want to 
run your analysis without having to install anything in your computer, simply 
go to this URL: https://colab.research.google.com/github/open-covid-19/data/

## Why another dataset?
This dataset is heavily inspired by the dataset maintained by 
[Johns Hopkins University][1]. Unfortunately, that dataset is currently 
experiencing maintenance issues and a lot of applications depend on this 
critical data being available in a timely manner. Further, the true sources
of data for that dataset are still unclear.

## Source of data
The world data comes from the daily reports at the [ECDC portal][2].
The XLS file is downloaded and parsed using `scrapy` and `pandas`.

Data for Chinese regions comes from the daily [WHO situation reports][3],
which are automatically parsed from their PDF source using `scrapy` and
`ghostscript`.

The data is automatically crawled and parsed using the scripts found in the 
[input folder](input). This is done daily, and as part of the processing
some additional columns are added, like country-level coordinates.

## Update the data
To update the contents of the [output folder](output), run the following:
```sh
# Install dependencies
pip install -r requirements.txt
# Update world data
sh input/update_ecdc_data.sh
# Update China data
sh input/update_who_data.sh
```

Note that this will only fetch the latest report from the WHO and ECDC sources.
If a report is skipped or amended, manual operation will be required. 

[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide
[3]: https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports