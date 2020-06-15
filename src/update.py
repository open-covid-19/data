#!/usr/bin/env python
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import sys
import inspect
import cProfile
from pstats import Stats
from pathlib import Path
from argparse import ArgumentParser
from typing import List
from multiprocessing import cpu_count

from lib.io import export_csv
from lib.pipeline import DataPipeline
from lib.utils import ROOT

# Step 1: Add your pipeline chain to this import block
import pipelines.epidemiology

# Process command-line arguments
argarser = ArgumentParser()
argarser.add_argument("--only", type=str, default=None)
argarser.add_argument("--exclude", type=str, default=None)
argarser.add_argument("--verify", type=str, default=None)
argarser.add_argument("--profile", action="store_true")
argarser.add_argument("--no-progress", action="store_true")
argarser.add_argument("--process-count", type=int, default=cpu_count())
args = argarser.parse_args()

assert not (
    args.only is not None and args.exclude is not None
), "--only and --exclude options cannot be used simultaneously"


# Ensure that there is an output folder to put the data in
(ROOT / "output" / "tables").mkdir(parents=True, exist_ok=True)
(ROOT / "output" / "snapshot").mkdir(parents=True, exist_ok=True)

if args.profile:
    profiler = cProfile.Profile()
    profiler.enable()

# A pipeline chain is any subfolder not starting with "_" in the pipelines folder
all_pipeline_chains = []
for item in (ROOT / "src" / "pipelines").iterdir():
    if not item.name.startswith("_") and not item.is_file():
        all_pipeline_chains.append(item.name)

# Run all the pipelines and place their outputs into the output folder
# The output name for each pipeline chain will be the name of the directory that the chain is in
for pipeline_name in all_pipeline_chains:
    table_name = pipeline_name.replace("_", "-")
    if args.only and not table_name in args.only.split(","):
        continue
    if args.exclude and table_name in args.exclude.split(","):
        continue
    pipeline_chain = DataPipeline.load(pipeline_name)
    show_progress = not args.no_progress
    pipeline_output = pipeline_chain.run(
        pipeline_name, verify=args.verify, process_count=args.process_count, progress=show_progress
    )
    export_csv(pipeline_output, ROOT / "output" / "tables" / f"{table_name}.csv")

if args.profile:
    stats = Stats(profiler)
    stats.strip_dirs()
    stats.sort_stats("cumtime")
    stats.print_stats(20)
