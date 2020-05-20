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


def parse_command(cmd: str) -> List[str]:
    if cmd == "curl":
        return ["python", str(ROOT / "scripts" / "curl_fetch.py")]
    if cmd == "static_download":
        return ["python", str(ROOT / "scripts" / "static_fetch.py")]
    if cmd == "dynamic_download":
        return ["node", str(ROOT / "scripts" / "dynamic_fetch.js")]
    raise ValueError(f"Unknown command {cmd}")


def process_source(cwd: Path, error_handler: Callable[[str], None], data_source: Dict[str, Any]):
    """
    Use the appropriate download command for the given data source.
    """

    cmd_tokens = parse_command(data_source["cmd"])
    cmd_tokens += ["--output", str(cwd / data_source["output"])]
    for option, value in data_source.items():
        if not option in ("cmd", "output"):
            value = value if isinstance(value, str) else json.dumps(value)
            cmd_tokens += [f"--{option}", value]

    print(">", " ".join(cmd_tokens))
    process = subprocess.Popen(cmd_tokens, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait for process to finish and get err streams
    stdout, stderr = process.communicate()

    # Write error to our stderr output
    if stderr:
        error_handler(stderr.decode("UTF-8"))

    # If there's any output, pipe it through
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
    if str(snapshot).startswith(".") or snapshot.name == "sitemap.json":
        continue
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
