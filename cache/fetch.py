import json
import warnings
import subprocess
from pathlib import Path
from os.path import relpath
from datetime import datetime
from functools import partial
from argparse import ArgumentParser
from typing import Any, Callable, Dict, List

# Declare the ROOT as the path of this file
ROOT = Path(__file__).parent
ROOT_PLACEHOLDER = "__ROOT__"


def process_source(cwd: Path, error_handler: Callable[[str], None], data_source: Dict[str, Any]):
    """
    Call the `cmd` and `args` from `data_source` using `cwd` as the current working directory.
    All instances of `__ROOT__` in `cmd` or `args` are replaced by the cache project's root
    directory.
    """

    cmd = data_source["cmd"]
    args = data_source["args"] + [data_source["url"]]
    relative_root = relpath(ROOT.absolute(), cwd.absolute())
    tokens = [token.replace(ROOT_PLACEHOLDER, str(relative_root)) for token in [cmd] + args]
    print(">", " ".join(tokens))
    process = subprocess.Popen(tokens, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait for process to finish and get err streams
    stdout, stderr = process.communicate()

    # Write error to our stderr output
    if stderr:
        error_handler(stderr.decode("UTF-8"))

    # Decode stdout as a string and pipe to STDOUT
    if stdout:
        print(stdout.decode("UTF-8"))

    # Verify that the return code is zero
    if process.returncode != 0:
        error_handler(f"Exit code: {process.returncode}")


# Parse arguments
parser = ArgumentParser()
parser.add_argument("--continue-on-error", action="store_true", default=False)
args = parser.parse_args()


def error_handler(error_message: str):
    """ Define error handling behavior depending on arguments """
    if args.continue_on_error:
        warnings.warn(error_message)
    else:
        raise RuntimeError(error_message)


# Create the output folder for the nearest hour in UTC time
now = datetime.utcnow()
output_name = now.strftime("%Y-%m-%d-%H")
output_path = ROOT / "output"
snapshot_path = output_path / output_name
snapshot_path.mkdir(parents=True)

# Iterate over each source and process it
map_func = partial(process_source, snapshot_path, error_handler)
for source in json.loads(open("cached_sources.json", "r").read()):
    map_func(source)

# Build a "sitemap" of the cache output folder
sitemap: Dict[str, List[str]] = {}
for snapshot in output_path.iterdir():
    if snapshot.is_file():
        warnings.warn(f"Unexpected file seen in root of {snapshot}")
        continue
    for cached_file in snapshot.iterdir():
        if not cached_file.is_file():
            warnings.warn(f"Unexpected folder seen in directory {cached_file}")
            continue
        sitemap_key = cached_file.stem
        snapshot_list = sitemap.get(sitemap_key, [])
        snapshot_list.append(str(cached_file.relative_to(output_path)))
        sitemap[sitemap_key] = snapshot_list

# Output the sitemap
with open(output_path / "sitemap.json", "w") as fd:
    json.dump(sitemap, fd)
