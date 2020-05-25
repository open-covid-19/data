# Open COVID-19 Dataset
This repository contains datasets of daily time-series data related to COVID-19, including
state/province epidemiology data for over 30 countries and county/municipality data for US, UK, NL
and CO.

The data is available as CSV and JSON files, which are published in Github Pages so they can be
served directly to Javascript applications without the need of a proxy to set the correct headers
for CORS and content type. Even if you only want the CSV files, using the URL served by Github Pages
is preferred in order to avoid caching issues and potential, future breaking changes.

For the purpose of making the data as easy to use as possible, there is a [master](#master) table
which contains the columns of all other tables joined by `key` and `date`. However,
performance-wise, it may be better to download the data separately and join the tables locally. The
datasets available from this project are:

| Table | CSV URL | JSON URL |
| ----- | ------- | -------- |
| [Master](#master) | [master.csv](https://open-covid-19.github.io/data/v2/master.csv) | N/A |
| [Index](#index) | [index.csv](https://open-covid-19.github.io/data/v2/index.csv) | [index.json](https://open-covid-19.github.io/data/v2/index.json) |
| [Demographics](#demographics) | [demographics.csv](https://open-covid-19.github.io/data/v2/demographics.csv) | [demographics.json](https://open-covid-19.github.io/data/v2/demographics.json) |
| [Economy](#economy) | [economy.csv](https://open-covid-19.github.io/data/v2/economy.csv) | [economy.json](https://open-covid-19.github.io/data/v2/economy.json) |
| [Epidemiology](#epidemiology) | [epidemiology.csv](https://open-covid-19.github.io/data/v2/epidemiology.csv) | [epidemiology.json](https://open-covid-19.github.io/data/v2/epidemiology.json) |
| [Geography](#geography) | [geography.csv](https://open-covid-19.github.io/data/v2/geography.csv) | [geography.json](https://open-covid-19.github.io/data/v2/geography.json) |
| [Mobility](#mobility) | [mobility.csv](https://open-covid-19.github.io/data/v2/mobility.csv) | [google-mobility.json](https://open-covid-19.github.io/data/v2/google-mobility.json) |
| [Oxford Government Response](#oxford-government-response) | [oxford-government-response.csv](https://open-covid-19.github.io/data/v2/oxford-government-response.csv) | [oxford-government-response.json](https://open-covid-19.github.io/data/v2/oxford-government-response.json) |
| [Weather](#weather) | [weather.csv](https://open-covid-19.github.io/data/v2/weather.csv) | [weather.json](https://open-covid-19.github.io/data/v2/weather.json) |

For more information about how to use these files see the section about
[using the data](#use-the-data), and for more details about each dataset see the section about
[understanding the data](#understand-the-data).

## Explore the data
|     |     |
| --- | --- |
| A simple visualization tool was built to explore the Open COVID-19 datasets, the [Open COVID-19 Explorer][12]: [![](https://github.com/open-covid-19/explorer/raw/master/screenshots/explorer.png)][12] | If you want to see [interactive charts with a unique UX][14], don't miss what [@Mahks](https://github.com/Mahks) built using the Open COVID-19 dataset: [![](https://i.imgur.com/cIODOtp.png)][14] |
| You can also check out the great work of [@quixote79](https://github.com/quixote79), [a MapBox-powered interactive map site][13]: [![](https://i.imgur.com/nFwxJId.png)][13] | Experience [clean, clear graphs with smooth animations][15] thanks to the work of [@jmullo](https://github.com/jmullo): [![](https://i.imgur.com/xdCzsUO.png)][15] |
| Become an armchair epidemiologist with the [COVID-19 timeline simulation tool][19] built by [@LeviticusMB](https://github.com/LeviticusMB): [![](https://i.imgur.com/4iWaP7E.png)][19] | Whether you want an interactive map, compare stats or look at charts, [@saadmas](https://github.com/saadmas) has you covered with a [COVID-19 Daily Tracking site][20]: [![](https://i.imgur.com/rAJvLSI.png)][20] |
| Compare per-million data at [Omnimodel][21] thanks to [@OmarJay1](https://github.com/OmarJay1): [![](https://i.imgur.com/RG7ZKXp.png)][21] |  |

If you are using this data, feel free to open an issue and let us know so we can give a call-out to
your project here.

## Use the data
Each table has a full version as well as subsets with only the last 30, 14, 7 and 1 days of data.
The full version is accessible at the URL described [in the table above](#open-covid-19-dataset).
The subsets can be found by appending the number of days to the path. For example, the subsets of
the master table are available at the following locations:
* Full version: https://open-covid-19.github.io/data/v2/master.csv
* Last 30 days: https://open-covid-19.github.io/data/v2/30/master.csv
* Last 14 days: https://open-covid-19.github.io/data/v2/14/master.csv
* Last 7 days: https://open-covid-19.github.io/data/v2/7/master.csv
* Latest: https://open-covid-19.github.io/data/v2/latest/master.csv

Note that the `latest` version contains the last non-null record for each key, whereas all others
contain the last `N` days of data (all of which could be null for some keys).

If you are trying to use this data alongside your own datasets, then you can use the [Index](#index)
table to get access to the ISO 3166 / NUTS / FIPS code, although administrative subdivisions are
not consistent among all reporting regions. For example, for the intra-country reporting, some EU
countries use NUTS2, others NUTS3 and many ISO 3166-2 codes.

You can find several examples in the [examples subfolder](examples) with code showcasing how to load
and analyze the data for several programming environments. If you want the short version, here are a
few snippets to get started.

### Google Colab
You can use Google Colab if you want to run your analysis without having to install anything in your
computer, simply go to this URL: https://colab.research.google.com/github/open-covid-19/data.

### R
If you prefer R, then this is all you need to do to load the epidemiology data:
```R
data <- read.csv("https://open-covid-19.github.io/data/v2/master.csv")
```

### Python
In Python, you need to have the package `pandas` installed to get started:
```python
import pandas
data = pandas.read_csv("https://open-covid-19.github.io/data/v2/master.csv")
```

### jQuery
Loading the JSON file using jQuery can be done directly from the output folder,
this code snippet loads the master table into the `data` variable:
```javascript
$.getJSON("https://open-covid-19.github.io/data/v2/master.json", data => { ... }
```

### Powershell
You can also use Powershell to get the latest data for a country directly from
the command line, for example to query the latest data for Australia:
```powershell
Invoke-WebRequest 'https://open-covid-19.github.io/data/v2/latest/master.csv' | ConvertFrom-Csv | `
    where Key -eq 'AU' | select country_name,date,total_confirmed,total_deceased,total_recovered
```

## Understand the data
Make sure that you are using the URL [linked at the table above](#open-covid-19-dataset) and not the
raw GitHub file, the latter is subject to change at any moment in non-compatible ways, and due to
the configuration of GitHub's raw file server you may run into potential caching issues.

Missing values will be represented as nulls, whereas zeroes are used when a true value of zero is
reported.

### Master
Flat table with records from all other tables joined by `key` and `date`. See below for information
about all the different tables and columns.

### Index
Non-temporal data related to countries and regions. It includes keys, codes and names for each
region, which is helpful for displaying purposes or when merging with other data:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | US_CA_06001 |
| **wikidata** | `string` | WikiData ID corresponding to this key | Q107146 |
| **country_code** | `string` | ISO 3166-1 alphanumeric 2-letter code of the country | US |
| **country_name** | `string` | American English name of the country, subject to change | United States of America |
| **subregion1_code** | `string` | (Optional) ISO 3166-2 or NUTS 2/3 code of the subregion | CA |
| **subregion1_name** | `string` | (Optional) American English name of the subregion, subject to change | California |
| **subregion2_code** | `string` | (Optional) FIPS code of the county (or local equivalent) | 06001 |
| **subregion2_name** | `string` | (Optional) American English name of the county (or local equivalent), subject to change | Alameda County |
| **3166-1-alpha-2** | `string` | ISO 3166-1 alphanumeric 2-letter code of the country | US |
| **3166-1-alpha-3** | `string` | ISO 3166-1 alphanumeric 3-letter code of the country | USA |
| **aggregation_level** | `integer` `[0-2]` | Level at which data is aggregated, i.e. country, state/province or county level | 2 |

### Demographics
Information related to the population demographics for each region:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **population** | `integer` | Total count of humans living in the region | 58500000 |
| **life_expectancy** | `double` `[years]` |  Average years that an individual is expected to live | 70.944 |
| **human_development_index** | `double` `[0-1]` | Composite index of life expectancy, education, and per capita income indicators | 0.773 |

### Economy
Information related to the economic development for each region:

| Name | Name | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **gdp** | `integer` `[USD]` | Gross domestic product; monetary value of all finished goods and services | 24450604878 |
| **gdp_per_capita** | `integer` `[USD]` | Gross domestic product divided by total population | 1148 |

### Epidemiology
Information related to the COVID-19 infections for each date-region pair:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **new_confirmed\*** | `integer` | Count of new cases confirmed after positive test on this date | 34 |
| **new_deceased\*** | `integer` | Count of new deaths from a positive COVID-19 case on this date | 2 |
| **new_recovered\*** | `integer` | Count of new recoveries from a positive COVID-19 case on this date | 13 |
| **total_confirmed\*\*** | `integer` | Cumulative sum of cases confirmed after positive test to date | 6447 |
| **total_deceased\*\*** | `integer` | Cumulative sum of deaths from a positive COVID-19 case to date | 133 |
| **total_recovered\*\*** | `integer` | Cumulative sum of recoveries from a positive COVID-19 case to date | 133 |

\*Values can be negative, typically indicating a correction or an adjustment in the way they were
measured. For example, a case might have been incorrectly flagged as recovered one date so it will
be subtracted from the following date.

\*\*Total count will not always amount to the sum of daily counts, because many authorities make
changes to criteria for counting cases, but not always make adjustments to the data. There is also
potential missing data. All of that makes the total counts *drift* away from the sum of all daily
counts over time, which is why the cumulative values, if reported, are kept in a separate column.

### Geography
Information related to the geography for each region:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **latitude** | `double` | Floating point representing the geographic coordinate | 30.9756 |
| **longitude** | `double` | Floating point representing the geographic coordinate | 112.2707 |
| **elevation** | `integer` [meters] | Elevation above the sea level | 875 |
| **area** | `integer` [squared kilometers] | Area encompassing this region | 3729 |

### Oxford Government Response
Summary of a government's response to the events, including a *stringency index*, collected from
[University of Oxford][18]:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **school_closing** | `integer` `[0-3]` | Schools are closed | 2 |
| **workplace_closing** | `integer` `[0-3]` | Workplaces are closed | 2 |
| **cancel_public_events** | `integer` `[0-3]` | Public events have been cancelled | 2 |
| **restrictions_on_gatherings** | `integer` `[0-3]` | Gatherings of non-household members are restricted | 2 |
| **public_transport_closing** | `integer` `[0-3]` | Public transport is not operational | 0 |
| **stay_at_home_requirements** | `integer` `[0-3]` | Self-quarantine at home is mandated for everyone | 0 |
| **restrictions_on_internal_movement** | `integer` `[0-3]` | Travel within country is restricted | 1 |
| **international_travel_controls** | `integer` `[0-3]` | International travel is restricted | 3 |
| **income_support** | `integer` `[USD]` | Value of fiscal stimuli, including spending or tax cuts | 20449287023 |
| **debt_relief** | `integer` `[0-3]` | Debt/contract relief for households | 0 |
| **fiscal_measures** | `integer` `[USD]` | Value of fiscal stimuli, including spending or tax cuts | 20449287023 |
| **international_support** | `integer` `[USD]` | Giving international support to other countries | 274000000 |
| **public_information_campaigns** | `integer` `[0-2]` | Government has launched public information campaigns | 1 |
| **testing_policy** | `integer` `[0-3]` | Country-wide COVID-19 testing policy | 1 |
| **contact_tracing** | `integer` `[0-2]` | Country-wide contact tracing policy | 1 |
| **emergency_investment_in_healthcare** | `integer` `[USD]` | Emergency funding allocated to healthcare | 500000 |
| **investment_in_vaccines** | `integer` `[USD]` | Emergency funding allocated to vaccine research | 100000 |
| **stringency_index** | `double` `[0-100]` | Overall stringency index | 71.43 |

For more information about each field and how the overall stringency index is
computed, see the [Oxford COVID-19 government response tracker][18].

### Weather
Daily weather information from nearest station reported by NOAA:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **noaa_station** | `string` | Identifier for the weather station | USC00206080 |
| **noaa_distance** | `double` `[kilometers]` | Distance between the location coordinates and the weather station | 28.693 |
| **minimum_temperature** | `double` `[celsius]` | Recorded hourly minimum temperature | 1.7 |
| **maximum_temperature** | `double` `[celsius]` | Recorded hourly maximum temperature | 19.4 |
| **rainfall** | `double` `[millimeters]` | Rainfall during the entire day | 51.0 |
| **snowfall** | `double` `[millimeters]` | Snowfall during the entire day | 0.0 |

### Mobility
[Google's][17] and [Apple's][22] Mobility Reports] are presented in CSV form as
[mobility.csv](https://open-covid-19.github.io/data/v2/mobility.csv) with the
following columns:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **mobility_driving** | `double` `[%]` |  Percentage change in movement via driving compared to baseline | -15 |
| **mobility_transit** | `double` `[%]` |  Percentage change in movement via public transit compared to baseline | -15 |
| **mobility_walking** | `double` `[%]` |  Percentage change in movement via walking compared to baseline | -15 |
| **mobility_transit_stations** | `double` `[%]` |  Percentage change in visits to transit station locations compared to baseline | -15 |
| **mobility_retail_and_recreation** | `double` `[%]` |  Percentage change in visits to retail and recreation locations compared to baseline | -15 |
| **mobility_grocery_and_pharmacy** | `double` `[%]` |  Percentage change in visits to grocery and pharmacy locations compared to baseline | -15 |
| **mobility_parks** | `double` `[%]` |  Percentage change in visits to park locations compared to baseline | -15 |
| **mobility_residential** | `double` `[%]` |  Percentage change in visits to residential locations compared to baseline | -15 |
| **mobility_workplaces** | `double` `[%]` |  Percentage change in visits to workplace locations compared to baseline | -15 |

### Notes about the data
For countries where both country-level and subregion-level data is available, the entry which has a
null value for the subregion level columns in the `index` table indicates upper-level aggregation.
For example, if a data point has values
`{country_code: US, subregion1_code: CA, subregion2_code: null, ...}` then that record will have
data aggregated at the subregion1 (i.e. state/province) level. If `subregion1_code`were null, then
it would be data aggregated at the country level.

Another way to tell the level of aggregation is the `aggregation_level` of the `index` table, see
the [schema documentation](#index) for more details about how to interpret it.

Please note that, sometimes, the country-level data and the region-level data come from different
sources so adding up all region-level values may not equal exactly to the reported country-level
value. See the [data loading tutorial][7] for more information.

There is also a [notices.csv](src/data/notices.csv) file which is manually updated with quirks about
the data. The goal is to be able to query by key and date, to get a list of applicable notices to
the requested subset.

### Backwards compatibility
Please note that the following datasets are maintained only to preserve backwards compatibility, but
shouldn't be used in any new projects:
* [Data](https://open-covid-19.github.io/data/data.csv)
* [Latest](https://open-covid-19.github.io/data/data_latest.csv)
* [Minimal](https://open-covid-19.github.io/data/data_minimal.csv)
* [Forecast](https://open-covid-19.github.io/data/data_forecast.csv)
* [Mobility](https://open-covid-19.github.io/data/mobility.csv)
* [Weather](https://open-covid-19.github.io/data/weather.csv)

## Contribute
The data from this repository has become increasingly reliant on Wikipedia sources. If you spot an
error in the data, or there's a country you would like to include, the best way to contribute to
this project is by helping maintain the data on the relevant Wikipedia article. Not only can that
data be parsed automatically by this project, but it will also help inform millions of others that
receive their information from Wikipedia. See the section below for a direct link to what Wikipedia
data is being parsed by this project.

For technical contributions, take a look at the [source directory](src/README.md) for more
information.

## Sources of data
All data in this repository is retrieved automatically. When possible, data is retrieved directly
from the relevant authorities, like a country's ministry of health.

| Data | Source | License |
| ---- | ------ | ------- |
| Metadata | [Wikipedia](https://wikipedia.org) | CC BY-SA |
| Demographics | [DataCommons](https://datacommons.org) | CC BY-SA |
| Weather | [NOAA](https://www.ncei.noaa.gov) | Custom (unrestricted for non-commercial use) |
| Google Mobility data | <https://github.com/pastelsky/covid-19-mobility-tracker> | N/A |
| Government response data | [Oxford COVID-19 government response tracker][18] | CC BY 4.0 |
| Country-level data | [ECDC](https://www.ecdc.europa.eu) | Custom (attribution required for non-commercial use) |
| Country-level data | [Our World in Data](https://ourworldindata.org) | CC BY 4.0 |
| Argentina | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/Argentina_medical_cases) | CC BY-SA |
| Australia | <https://covid-19-au.com/> | Various (unrestricted for non-commercial use) |
| Bolivia | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/Bolivia_medical_cases) | CC BY-SA |
| Brazil | <https://github.com/elhenrico/covid19-Brazil-timeseries> | N/A (written consent) |
| Canada | [Department of Health Canada](https://www.canada.ca/en/public-health) | Public Domain |
| Chile | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/Chile_medical_cases) | CC BY-SA |
| China | [DXY COVID-19 dataset](https://github.com/BlankerL/DXY-COVID-19-Data) | MIT |
| Colombia | [Government Authority](https://www.datos.gov.co) | Public Domain |
| France | <https://github.com/cedricguadalupe/FRANCE-COVID-19> | GPLv3 |
| Germany | <https://github.com/jgehrcke/covid-19-germany-gae> | MIT |
| India | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/India_medical_cases) | CC BY-SA |
| Indonesia | <https://catchmeup.id/covid-19> | N/A |
| Italy | [Italy's Department of Civil Protection](https://github.com/pcm-dpc/COVID-19) | CC BY 4.0 |
| Japan | <https://github.com/swsoyee/2019-ncov-japan> | MIT |
| Malaysia | [Wikipedia](https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Malaysia) | CC BY-SA |
| Mexico | <https://github.com/mexicovid19/Mexico-datos> | MIT |
| Netherlands | <https://github.com/J535D165/CoronaWatchNL> | CC0 |
| Norway | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | N/A |
| Pakistan | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/Pakistan_medical_cases) | CC BY-SA |
| Peru | [Wikipedia](https://es.wikipedia.org/wiki/Pandemia_de_enfermedad_por_coronavirus_de_2020_en_Per%C3%BA) | CC BY-SA |
| Poland | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | N/A |
| Portugal | <https://github.com/dssg-pt/covid19pt-data> | GPLv3 |
| Russia | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/Russia_medical_cases) | CC BY-SA |
| South Korea | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data/South_Korea_medical_cases) | CC BY-SA |
| Spain | [Government Authority](https://covid19.isciii.es) | Public Domain |
| Spain | [Datadista COVID-19 dataset](https://github.com/datadista/datasets) | AGPLv3 |
| Sweden | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | N/A |
| Switzerland | [OpenZH data](https://open.zh.ch) | CC 4.0 |
| United Kingdom | <https://github.com/tomwhite/covid-19-uk-data> | The Unlicense |
| USA | [NYT COVID Dataset](https://github.com/nytimes) | Custom (attribution required for non-commercial use) |
| USA | [COVID Tracking Project](https://covidtracking.com) | Apache 2.0 |

## Why another dataset?
This dataset is heavily inspired by the dataset maintained by [Johns Hopkins University][1].
Unfortunately, that dataset has intermittently experienced maintenance issues and a lot of
applications depend on this critical data being available in a timely manner. Further, the true
sources of data for that dataset are still unclear and the methodology used to process the data has
not been made open sourced (at least at the time of this writing).

## Update the data
To update the contents of the [output folder](output), first install the dependencies:
```sh
pip install -r requirements.txt
```

Then run the following script from the source folder to update all datasets:
```sh
cd src
python run.py
```

See the [source documentation](src) for more technical details.

[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.ecdc.europa.eu
[3]: https://github.com/BlankerL/DXY-COVID-19-Data
[4]: https://web.archive.org/web/20200314143253/https://www.salute.gov.it/nuovocoronavirus
[5]: https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports
[6]: https://github.com/open-covid-19/data/issues/16
[7]: https://github.com/open-covid-19/data/examples/data_loading.ipynb
[8]: https://web.archive.org/web/20200320122944/https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm
[9]: https://covidtracking.com
[10]: https://github.com/pcm-dpc/COVID-19
[11]: https://github.com/datadista/datasets/tree/master/COVID%2019
[12]: https://open-covid-19.github.io/explorer
[13]: https://kepler.gl/demo/map?mapUrl=https://dl.dropboxusercontent.com/s/lrb24g5cc1c15ja/COVID-19_Dataset.json
[14]: https://www.starlords3k.com/covid19.php
[15]: https://kiksu.net/covid-19/
[16]: https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html
[17]: https://www.google.com/covid19/mobility/
[18]: https://www.bsg.ox.ac.uk/research/research-projects/oxford-covid-19-government-response-tracker
[19]: https://auditter.info/covid-timeline
[20]: https://www.coronavirusdailytracker.info/
[21]: https://omnimodel.com/
[22]: https://www.apple.com/covid19/mobility
