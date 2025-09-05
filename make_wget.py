#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate a Bash wget script from YAML of the form:

data:
  ...
  paths:
    - http://...
    - http://...
"""

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse
import shlex
import sys
import yaml

HEADER = """#!/usr/bin/env bash
set -Eeuo pipefail

if ! command -v wget >/dev/null 2>&1; then
  echo "Error: wget is not installed." >&2
  exit 1
fi

DATA_DIR={data_dir}
echo "Saving to: $DATA_DIR"
mkdir -p "$DATA_DIR"
"""

LINE = """
# {url}
mkdir -p "$(dirname "$DATA_DIR/{rel}")"
wget -c --retry-connrefused -t 5 --timeout=60 "{url}" -O "$DATA_DIR/{rel}"
"""

def normalize_relative_path(url: str, trim_prefix: str) -> str:
    parsed = urlparse(url)
    rel = (parsed.path or "/").lstrip("/")
    trim = (trim_prefix or "").lstrip("/")
    if trim and rel.startswith(trim):
        rel = rel[len(trim):].lstrip("/")
    return rel

def normalize_prefixes(raw_list):
    out = []
    for raw in raw_list or []:
        for token in raw.split(","):
            token = token.strip()
            if token:
                out.append(token.lstrip("/"))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--yaml", required=True)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--trim-prefix", default="/preprocessed/proof-pile-2/")
    ap.add_argument("--include-prefix", action="append", default=[])
    ap.add_argument("--out", default="download_data.sh")
    args = ap.parse_args()

    with open(args.yaml, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)

    if "data" not in y or "paths" not in y["data"]:
        print("YAML must have data.paths list", file=sys.stderr)
        sys.exit(1)

    urls = y["data"]["paths"]
    include_prefixes = normalize_prefixes(args.include_prefix)

    pairs = []
    for url in urls:
        rel = normalize_relative_path(url, args.trim_prefix)
        if include_prefixes and not any(rel.startswith(p) for p in include_prefixes):
            continue
        pairs.append((url, rel))

    if not pairs:
        print("No matching URLs", file=sys.stderr)
        sys.exit(2)

    script = HEADER.format(data_dir=shlex.quote(str(Path(args.data_dir).resolve())))
    for url, rel in pairs:
        script += LINE.format(url=url, rel=rel)

    out = Path(args.out).resolve()
    out.write_text(script, encoding="utf-8")
    os.chmod(out, os.stat(out).st_mode | 0o111)
    print(f"Wrote wget script with {len(pairs)} items: {out}")

if __name__ == "__main__":
    main()

