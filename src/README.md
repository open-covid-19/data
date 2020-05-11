# src
This folder contains all the code necessary to update the data tables. It is a single Python package
with the following modules:
* [data](data): auxiliary and help data files used during processing of input
* [lib](lib): core functions and utilities used across the package
* [pipelines](pipelines): contains all the individual pipelines that produce the data tables
* [test](test): unit testing of core functions

## Running
To run all the pipelines, execute the `run.py` script:
```sh
python run.py
```

## Testing
Run execute the tests, run the following command from this directory:
```sh
python -m unittest
```

## Contributing
To create a new table, it is recommended to start by making a copy of the c
folder. The name of the folder will determine the file name of the output table, which will be
placed under the output folder. For the pipeline chain to be automatically run, add an import
statement to the [pipelines init file](pipelines/__init__.py).

## Architecture
### Pipeline
Individual data sources are encoded as a `Pipeline`. Each pipeline goes through the following steps,
executed in order:
1. Fetch: download resources into raw data
1. Parse: convert raw data to structured format
1. Merge: associate each record with a known `key`
1. Filter: filter out unneeded data and keep only desired output columns
1. Patch: apply hotfixes to specific data issues

The majority of the processing in a pipeline will likely take place in the `parse` step. All
individual records output by the pipeline have to follow the following guidelines:
* Each record **must** be matched with a known `key` present in the
  [auxiliary table](data/auxiliary.csv). Otherwise, it will be dropped from the output.
* Each record **may** include a `date` column, which must be ISO 8601 format (i.e. `YYYY-MM-DD`).

To make writing pipelines easier, a default implementation [`DefaultPipeline`](lib/pipeline.py)
already includes a lot of the functionality that is likely to be used by a standard data parsing
routine, including downloading and conversion of raw resources into a pandas `DataFrame`. See the
[template pipeline](pipelines/_template/srcname_pipeline.py) for a trivial example which subclasses
the `DefaultPipeline` implementation.

### Pipeline Chain
A `PipelineChain` object wraps a list of individual `Pipelines`. Each pipeline is executed in order
and the output is combined into a single data table. When values for the same `key` (or, if present,
`key`-`data` pair) overlap, the value that was present in the last pipeline in the list is chosen.
For example, if `pipeline1` outputs `{key: AA, value: 1}` and `pipeline2` outputs
`{key: AA, value: 2}`, then the combined output will be `{key: AA, value: 2}` -- assuming that
`pipeline2` has a higher index than `pipeline1` in the list.

### Overview
The following diagram summarizes the architecture:
![](data/architecture.png)