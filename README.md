# Open COVID-19 Dataset
This is a free dataset of time-series, historical data related to COVID-19.
Currently, the following events are reported for each country / region:
* Confirmed cases: total number of cases confirmed after positive test
* Recovered cases: total number of cases that have recovered after illness
* Deaths: total number of deaths from a positive COVID-19 case

## Why another dataset?
This dataset builds upon the dataset maintained by 
[Johns Hopkins University][1]. Unfortunately, that dataset is currently 
experiencing maintenance issues and a lot of applications depend on this 
critical data being available in a timely manner. If there are any other
well-maintained datasets this one may be deprecated in favour of those.

## Crowd sourcing
This is a crowd-sorced dataset. It cannot be maintained by a single author, so
we hope to receive support from volunteers that can help keep this up to date.
Attempts to automate the processing of daily [situation reports from WHO][2]
were made, but they are in PDF format and have changed the formatting of the 
tables multiple times. If an automated way to keep this dataset up-to-date is 
found, that would be the preferred solution.

## How can I contribute?
Please read the following instructions closely, time is a limited resource for
all of us.

Contributions to the dataset are made via pull requests. For an example of what
those are expected to look like, please see this one: [PR#1][5].

If you want to volunteer some time and review pending pull requests, anyone is
welcome to comment on any open pull requests (please be civil and respectful).

A pull request must contain changes to the [time_series.csv][3] file **only**;
all other data files are derived automatically from this one. It must also
contain an [archive.org][4] link to the source that confirms the changes 
requested.

Sometimes, sources will contradict each other. The following is the order of
preference for data sources:
1. Official statement from local government
2. [Situation reports from WHO][1]
3. Local press reporting

## Known data sources
Here is a list of data sources for a number of countries where data can be queried, potentially scraped automatically:

| Country / Region  | Source |
| ----------------- | ------ |
| Australia         | https://www.health.gov.au/news/health-alerts/novel-coronavirus-2019-ncov-health-alert |
| Spain             | https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm |

[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports
[3]: https://github.com/open-covid-19/data/time_series.csv
[4]: https://archive.org
[5]: https://github.com/open-covid-19/data/pull/1
