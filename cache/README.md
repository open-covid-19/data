# Open COVID-19 Cache
This repo contains very simple code used to cache data sources related to COVID-19. The main purpose
is to download data that is updated daily and potentially overwritten. In order to ensure
reliability, the logic is kept as simple as possible.

## Data Sources
The list of data sources is maintained in the [`cached_sources.json`](cached_sources.json) file. It
contains a list of records with the following fields:

| Name | Description | Example |
| ---- | ----------- | ------- |
| url | The URL of the resource that should be downloaded / crawled | https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/Race-Ethnicity.aspx |
| cmd | Command that will process the URL and download a cached copy | python |
| args | List of options passed to `cmd` | `[scripts/download.py, --extension html]` |

Each data source will then be fetched by executing:
```sh
${cmd} ${args} ${url}
```

Each command + arguments is responsible for writing the output to the current folder.

## License
Each data source cached here will retain its original license. The [LICENSE file](../LICENSE) only
applies to the code and data owned and published by this repository.
