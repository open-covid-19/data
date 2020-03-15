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
The world data comes from the [ECDC portal][2]. The XLS file is downloaded 
daily and some additional columns are added, like country-level coordinates. 
The data is automatically parsed with the scripts found in the 
[input folder](input).

Data for Chinese regions comes from the daily WHO situation reports.

## Update the data
To update the contents of the [output folder](output), get the latest URL from
the [ECDC portal][2] for the XLS report and run the following commands:
```sh
pip install -r requirements.txt
python input/load_xls_data.py <URL>
```

Then update the China data by running:
```sh
# The date must match one of the WHO reports
DATE="20200315"
sh input/crawl_who_data.sh $DATE
```

[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide