#!/usr/bin/env python


import uuid

import requests
from .utils import ROOT


def download(url: str, ext: str = None, offline: bool = False):
    '''
    This function downloads a file into the snapshots folder and outputs the
    hashed file name based on the input URL. This is used to ensure
    reproducibility in downstream processing, which will not require to network
    access.
    '''
    if ext is None: ext = url.split('.')[-1]
    file_path = ROOT / 'snapshot' / ('%s.%s' % (uuid.uuid5(uuid.NAMESPACE_DNS, url), ext))

    # Download the file if online
    if not offline:
        with open(file_path, 'wb') as file_handle:
            file_handle.write(requests.get(url).content)

    # Output the downloaded file path
    return file_path.absolute()
