# Open COVID-19 Dataset
This is a free dataset of time-series, historical data related to COVID-19.
Currently, the following events are reported for each country / region:
* Confirmed cases: total number of cases confirmed after positive test
* Deaths: total number of deaths from a positive COVID-19 case

## Why another dataset?
This dataset is heavily inspired by the dataset maintained by 
[Johns Hopkins University][1]. Unfortunately, that dataset is currently 
experiencing maintenance issues and a lot of applications depend on this 
critical data being available in a timely manner.

## Source of data
The data comes from the [ECDC portal][2]. The XLS file is downloaded daily and
the data is automatically parsed with the scripts that can be found in the
[input folder](input).

## Update the data
To update the contents of the [output folder](output), get the latest URL from
the [ECDC portal][2] for the XLS report and run the following commands:
```sh
pip install -r requirements.txt
python input/load_xls_data.py <URL>
```

[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide