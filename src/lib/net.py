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



import uuid
from pathlib import Path
from typing import Union

import requests
from .utils import ROOT


def download(url: Union[Path, str], ext: str = None, offline: bool = False) -> str:
    """
    This function downloads a file into the snapshots folder and outputs the
    hashed file name based on the input URL. This is used to ensure
    reproducibility in downstream processing, which will not require to network
    access.
    """
    url = str(url)
    if ext is None:
        ext = url.split(".")[-1]
    file_path = (
        ROOT / "output" / "snapshot" / ("%s.%s" % (uuid.uuid5(uuid.NAMESPACE_DNS, url), ext))
    )

    # Download the file if online
    if not offline:
        with open(file_path, "wb") as file_handle:
            file_handle.write(requests.get(url).content)

    # Output the downloaded file path
    return str(file_path.absolute())
