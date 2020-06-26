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


def main(
    output_folder: Path,
    verify: str = None,
    only: List[str] = None,
    exclude: List[str] = None,
    process_count: int = cpu_count(),
    show_progress: bool = True,
) -> None:
    """
    Executes the data pipelines and places all outputs into `output_folder`. This is typically
    followed by publishing of the contents of the output folder to a server.

    Args:
        output_folder: Root folder where snapshot, intermediate and tables will be placed.
        verify: Run anomaly detection on the outputs using this strategy. Value must be one of:
            - None: (default) perform no anomaly detection
            - "simple": perform only fast anomaly detection
            - "full": perform exhaustive anomaly detection (can be very slow)
        only: If provided, only pipelines with a name appearing in this list will be run.
        exclude: If provided, pipelines with a name appearing in this list will not be run.
        process_count: Maximum number of processes to use during the data pipeline execution.
        show_progress: Display progress for the execution of individual DataSources within this
            pipeline.
    """

    assert not (
        only is not None and exclude is not None
    ), "--only and --exclude options cannot be used simultaneously"

    # Ensure that there is an output folder to put the data in
    (output_folder / "snapshot").mkdir(parents=True, exist_ok=True)
    (output_folder / "intermediate").mkdir(parents=True, exist_ok=True)
    (output_folder / "tables").mkdir(parents=True, exist_ok=True)

    # A pipeline chain is any subfolder not starting with "_" in the pipelines folder
    all_pipeline_names = []
    for item in (ROOT / "src" / "pipelines").iterdir():
        if not item.name.startswith("_") and not item.is_file():
            all_pipeline_names.append(item.name)

    # Verify that all of the provided pipeline names exist as pipelines
    only = only.split(",") if only is not None else []
    exclude = exclude.split(",") if exclude is not None else []
    for pipeline_name in only + exclude:
        assert pipeline_name in all_pipeline_names, f'"{pipeline_name}" pipeline does not exist'

    # Run all the pipelines and place their outputs into the output folder
    # The output name for each pipeline chain will be the name of the directory that the chain is in
    for pipeline_name in all_pipeline_names:
        table_name = pipeline_name.replace("_", "-")
        if table_name in exclude or not table_name in only:
            continue
        pipeline_chain = DataPipeline.load(pipeline_name)
        pipeline_output = pipeline_chain.run(
            pipeline_name,
            output_folder,
            verify=verify,
            process_count=process_count,
            progress=show_progress,
        )
        export_csv(pipeline_output, output_folder / "tables" / f"{table_name}.csv")


if __name__ == "__main__":
    # Process command-line arguments
    argparser = ArgumentParser()
    argparser.add_argument("--only", type=str, default=None)
    argparser.add_argument("--exclude", type=str, default=None)
    argparser.add_argument("--verify", type=str, default=None)
    argparser.add_argument("--profile", action="store_true")
    argparser.add_argument("--no-progress", action="store_true")
    argparser.add_argument("--process-count", type=int, default=cpu_count())
    argparser.add_argument("--output-folder", type=str, default=str(ROOT / "output"))
    args = argparser.parse_args()

    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()

    main(
        Path(args.output_folder),
        verify=args.verify,
        only=args.only,
        exclude=args.exclude,
        process_count=args.process_count,
        show_progress=not args.no_progress,
    )

    if args.profile:
        stats = Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats("cumtime")
        stats.print_stats(20)
