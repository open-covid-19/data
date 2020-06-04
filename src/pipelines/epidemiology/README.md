# Epidemiology
This table is probably the most important one in this dataset. Because of that, additional
instructions are provided for how to add new data sources. A canonical example is
[this commit](https://github.com/open-covid-19/data/commit/0d1d978ce9c6afaecbf5fc9ac9fb386f77d16d4f)
which adds a new data source to for epidemiology data broken down into subregions of Afghanistan.


# Step 0: identify a data source
Before adding new data, we must have a data source


## Step 1: populate metadata table
Only records which have a corresponding key in the [metadata.csv](../../data/metadata.csv) table
will be ingested. When the pipeline is run, you will see output similar to this for records which do
not have a corresponding entry in the metadata table:
```
.../lib/pipeline.py:158: UserWarning: No key match found for:
match_string        La Guaira
date               2020-04-16
total_confirmed            14
new_confirmed             NaN
country_code               VE
_vec                La Guaira
Name: 0, dtype: object
```

This indicates that a record with the `match_string` value of "La Guaira" was not matched with any
entries in the metadata table. Sometimes there simply isn't a good match; in this case "La Guaira"
is a city which does not have a good region correspondence -- subregion1 is state/province and
subregion2 is county/municipality.

Most countries use ISO 3166-2 codes to report epidemiology data at the subregion level. If that's
the case for the data source you are trying to add, then you can look for the subregions declared
in the [iso_3166_2_codes.csv](../../data/iso_3166_2_codes.csv) table and copy/paste them into the
metadata table (don't forget to reorder the columns and add missing fields, since metadata.csv has
more columns than iso_3166_2_codes.csv).

For extra credit, make sure that there is also a corresponding entry in the
[knowledge_graph.csv](../../data/knowledge_graph.csv) table for all the keys added into
metadata.csv.


## Step 2: add an entry to the config YAML
TODO


## Step 3: create a parsing script for your source
TODO


## Step 4: test your script
TODO
