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
import os.path
from pathlib import Path
from functools import partial
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from contextlib import contextmanager
from typing import Iterator

from google.cloud import storage
from google.oauth2.credentials import Credentials
from google.cloud.storage.blob import Blob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# pylint: disable=wrong-import-position
from lib.concurrent import thread_map
from lib.utils import SRC
from update import main as update_main

TOKEN = None
BLOB_OP_MAX_RETRIES = 3


def get_storage_client():
    """
    Creates an instance of google.cloud.storage.Client using a token if provided, otherwise
    the default credentials are used.
    """
    if TOKEN is None:
        return storage.Client()
    else:
        credentials = Credentials(TOKEN)
        return storage.Client(credentials=credentials)


def download_folder(bucket_name: str, remote_path: str, local_folder: Path) -> None:
    client = get_storage_client()
    bucket = client.get_bucket(bucket_name)

    def _download_blob(local_folder: Path, blob: Blob) -> None:
        # Remove the prefix from the remote path
        rel_path = blob.name.split(f"{remote_path}/", 2)[-1]
        print(f"Downloading {rel_path} to {local_folder}/")
        file_path = local_folder / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(BLOB_OP_MAX_RETRIES):
            try:
                return blob.download_to_filename(file_path)
            except Exception as exc:
                print(exc, file=sys.stderr)

    map_func = partial(_download_blob, local_folder)
    thread_map(map_func, bucket.list_blobs(prefix=remote_path), total=None, disable=True)


def upload_folder(bucket_name: str, remote_path: str, local_folder: Path) -> None:
    client = get_storage_client()
    bucket = client.get_bucket(bucket_name)

    def _upload_file(remote_path: str, file_path: Path):
        print(f"Uploading {file_path} to {remote_path}/")
        target_path = file_path.relative_to(local_folder)
        blob = bucket.blob(os.path.join(remote_path, target_path))
        for _ in range(BLOB_OP_MAX_RETRIES):
            try:
                return blob.upload_from_filename(file_path)
            except Exception as exc:
                print(exc, file=sys.stderr)

    map_func = partial(_upload_file, remote_path)
    thread_map(map_func, local_folder.glob("**/*"), total=None, disable=True)


@contextmanager
def sync_output_folder(bucket_name: str = None) -> Iterator[Path]:
    """
    Provide a temporary output folder which is synced with our cloud storage
    """
    # If bucket name is not provided, read it from env var
    bucket_env_key = "GCS_BUCKET_NAME"
    bucket_name = bucket_name or os.getenv(bucket_env_key)
    assert bucket_name is not None, f"{bucket_env_key} not set"
    with TemporaryDirectory() as output_folder:
        output_folder = Path(output_folder)

        try:
            # Download all the snapshots and intermediate files
            download_folder(bucket_name, "snapshot", output_folder / "snapshot")
            download_folder(bucket_name, "intermediate", output_folder / "intermediate")

            # Provide access to the temporary output folder
            yield output_folder

        finally:
            # Upload snapshots, intermediate files and results
            upload_folder(bucket_name, "snapshot", output_folder / "snapshot")
            upload_folder(bucket_name, "intermediate", output_folder / "intermediate")
            upload_folder(bucket_name, "v2", output_folder / "tables")


def get_table_names() -> Iterator[str]:
    for item in (SRC / "pipelines").iterdir():
        if not item.name.startswith("_") and not item.is_file():
            yield item.name.replace("_", "-")


def update(table_name: str) -> None:
    assert table_name in list(get_table_names())
    with sync_output_folder() as output_folder:
        # Run the pipeline for the provided table name
        update_main(output_folder, only=[table_name])


if __name__ == "__main__":

    # Process command-line arguments
    argparser = ArgumentParser()
    argparser.add_argument("command", type=str, help="[update, publish, cache]")
    argparser.add_argument("--table", type=str, default=None, help="Used by `update`")
    args = argparser.parse_args()

    allowed_commands = ["update", "publish", "cache"]
    assert (
        args.command in allowed_commands
    ), f"Unknown command ${args.command}, it must be one of ${allowed_commands}."

    if args.command == "update":
        assert args.table is not None, f"Missing mandatory argument --table"
        update(args.table)

    elif args.command == "publish":
        raise NotImplementedError()

    elif args.command == "cache":
        raise NotImplementedError()
