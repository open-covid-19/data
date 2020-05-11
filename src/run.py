#!/usr/bin/env python

import sys
import inspect
from pathlib import Path
from lib.pipeline import PipelineChain
from lib.io import read_file, fuzzy_text
from lib.utils import ROOT
import pipelines

# Ensure that there is an output folder to put the data in
(ROOT / 'output').mkdir(exist_ok=True)
(ROOT / 'snapshot').mkdir(exist_ok=True)

# Run all the pipelines and place their outputs into the output folder
# All the pipelines imported in /src/pipelines/__init__.py which subclass PipelineChain are run
# The output name for each pipeline chain will be the name of the directory that the chain is in
for pipeline_chain_class in PipelineChain.__subclasses__():
    pipeline_chain = pipeline_chain_class()
    pipeline_name = Path(inspect.getsourcefile(type(pipeline_chain))).parent.name
    pipeline_chain.run().to_csv(ROOT / 'output' / '{}.csv'.format(pipeline_name), index=False)
