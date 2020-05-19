#!/usr/bin/env python

import sys
import inspect
import cProfile
from pstats import Stats
from pathlib import Path
from argparse import ArgumentParser
from typing import List

from lib.pipeline import PipelineChain
from lib.utils import ROOT

# Step 1: Add your pipeline chain to this import block
from pipelines.demographics.demographics_pipeline import DemographicsPipelineChain
from pipelines.economy.economy_pipeline import EconomyPipelineChain
from pipelines.epidemiology.pipeline_chain import EpidemiologyPipelineChain
from pipelines.geography.geography_pipeline import GeographyPipelineChain
from pipelines.index.index_pipeline import IndexPipelineChain
from pipelines.oxford_government_response.oxford_government_response_pipeline import (
    OxfordGovernmentResponsePipelineChain,
)
from pipelines.weather.weather_pipeline import WeatherPipelineChain

# Step 2: After adding the import statement above, add your pipeline chain to this list
all_pipeline_chains: List[PipelineChain] = [
    DemographicsPipelineChain,
    EconomyPipelineChain,
    EpidemiologyPipelineChain,
    GeographyPipelineChain,
    IndexPipelineChain,
    OxfordGovernmentResponsePipelineChain,
    WeatherPipelineChain,
]

# Process command-line arguments
argarser = ArgumentParser()
argarser.add_argument("--only", type=str, default=None)
argarser.add_argument("--exclude", type=str, default=None)
argarser.add_argument("--profile", action="store_true")
args = argarser.parse_args()

assert not (
    args.only is not None and args.exclude is not None
), "--only and --exclude options cannot be used simultaneously"


# Ensure that there is an output folder to put the data in
(ROOT / "output").mkdir(exist_ok=True)
(ROOT / "snapshot").mkdir(exist_ok=True)

if args.profile:
    profiler = cProfile.Profile()
    profiler.enable()

# Run all the pipelines and place their outputs into the output folder
# All the pipelines imported in this file which subclass PipelineChain are run
# The output name for each pipeline chain will be the name of the directory that the chain is in
for pipeline_chain_class in all_pipeline_chains:
    pipeline_chain = pipeline_chain_class()
    pipeline_path = Path(str(inspect.getsourcefile(type(pipeline_chain))))
    pipeline_name = pipeline_path.parent.name.replace("_", "-")
    if args.only and not pipeline_name in args.only.split(","):
        continue
    if args.exclude and pipeline_name in args.exclude.split(","):
        continue
    pipeline_chain.run().to_csv(ROOT / "output" / "{}.csv".format(pipeline_name), index=False)

if args.profile:
    stats = Stats(profiler)
    stats.strip_dirs()
    stats.sort_stats("cumtime")
    stats.print_stats(20)
