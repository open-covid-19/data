#!/usr/bin/env python

import sys
from pathlib import Path
from lib.io import read_file
from lib.utils import ROOT
from pipelines import epidemiology

# Ensure that there is an output folder to put the data in
(ROOT / 'output').mkdir(exist_ok=True)
(ROOT / 'snapshot').mkdir(exist_ok=True)

# Read the auxiliary input files into memory
aux = read_file(ROOT / 'src' / 'data' / 'auxiliary.csv')

# Run all the pipelines and place their outputs into the output folder
for pipeline_chain in [epidemiology]:
    data = pipeline_chain.run(aux)
    pipeline_name = pipeline_chain.__name__.split('.')[-1]
    data.to_csv(ROOT / 'output' / '{}.csv'.format(pipeline_name), index=False)
