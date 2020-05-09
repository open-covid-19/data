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
To create a new table, it is recommended to start by making a copy of the [_template](_template)
folder. The name of the folder will correspond to the name of the table in the output folder at
the root of the project.
