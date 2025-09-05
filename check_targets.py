#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Check targets referenced in a wget-based .sh script.

What it does
------------
- Extract every path that follows the '-O' argument from lines in your .sh file.
  Examples it understands:
    wget ... -O "$DATA_DIR/some/rel/path.npy"
    wget ... -O ${DATA_DIR}/some/rel/path.npy
    wget ... -O /absolute/path/inside/data_dir/file.npy
- Resolve DATA_DIR in this precedence:
    1) --data-dir CLI argument
    2) environment variable DATA_DIR
    3) first 'DATA_DIR=...' assignment found in the .sh
- For each extracted target, build the absolute path (join with DATA_DIR when needed),
  check existence, and:
    * print all missing ones
    * print the last existing one by the **order they appear in the script**.

Notes
-----
- No assumption on filename patterns (works for any names, not just part-A-B.npy).
- Deduplicates targets while preserving first-seen order.
- Optionally filter by a relative prefix after DATA_DIR with --filter-prefix.
"""

import argparse
import os
import re
import shlex
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


ASSIGN_RE = re.compile(
    r"""^\s*(?:export\s+)?DATA_DIR\s*=\s*(['"]?)(.*?)\1\s*$"""
)

def parse_data_dir(cli_data_dir: Optional[str], script_path: Path) -> Optional[str]:
    """Resolve DATA_DIR by CLI > env > parse from script; return normalized absolute path or None."""
    # 1) CLI
    if cli_data_dir:
        return str(Path(cli_data_dir).expanduser().resolve())
    # 2) env
    env = os.environ.get("DATA_DIR")
    if env:
        return str(Path(env).expanduser().resolve())
    # 3) parse from script
    try:
        with script_path.open("r", encoding="utf-8") as f:
            for line in f:
                m = ASSIGN_RE.match(line)
                if m:
                    value = m.group(2).strip()
                    if value:
                        return str(Path(value).expanduser().resolve())
    except FileNotFoundError:
        pass
    return None


def extract_targets_from_sh(script_path: Path) -> List[str]:
    """
    Scan the .sh file and extract values that follow '-O'.
    Returns a list of tokens exactly as they appear (quotes are removed by shlex).
    """
    targets: List[str] = []
    with script_path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # Try to split like a shell line; ignore lines that aren't simple commands
            try:
                tokens = shlex.split(line, posix=True)
            except ValueError:
                # If shlex chokes on complex shell syntax, skip this line
                continue
            # Find '-O' followed by the path token
            for i, tok in enumerate(tokens):
                if tok == "-O" and i + 1 < len(tokens):
                    targets.append(tokens[i + 1])
    return targets


def normalize_to_relative(target_token: str, data_dir: Optional[str]) -> Tuple[str, Optional[str]]:
    """
    Convert a token after -O into (relative_path, absolute_path_or_None).
    - If token looks like $DATA_DIR/<rel> or ${DATA_DIR}/<rel> -> return rel, and abs if data_dir is known.
    - If token is an absolute path: try to make it relative to data_dir (if provided), otherwise keep as-is
      (relative_path will still be the tail after data_dir if possible; else the basename).
    """
    # Handle $DATA_DIR and ${DATA_DIR}
    dd_vars = ("$DATA_DIR/", "${DATA_DIR}/")
    for prefix in dd_vars:
        if target_token.startswith(prefix):
            rel = target_token[len(prefix):]
            abs_path = str(Path(data_dir, rel)) if data_dir else None
            return rel, abs_path

    # Absolute path case (e.g., -O /data/.../file.npy)
    p = Path(target_token)
    if p.is_absolute():
        if data_dir:
            try:
                rel = str(Path(target_token).resolve().relative_to(Path(data_dir).resolve()))
            except Exception:
                # Not under DATA_DIR; treat the absolute as "rel" for reporting purposes
                rel = str(Path(target_token).name)
            return rel, str(Path(target_token).resolve())
        else:
            return str(p), str(p)

    # Fallback: treat as a plain relative path (rare)
    return target_token, (str(Path(data_dir, target_token)) if data_dir else None)


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    """Deduplicate while preserving first-seen order."""
    return list(OrderedDict.fromkeys(items).keys())


def main():
    ap = argparse.ArgumentParser(description="Check which targets from a wget .sh exist under DATA_DIR.")
    ap.add_argument("--script", required=True, help="Path to the wget-based .sh file")
    ap.add_argument("--data-dir", default=None, help="Override DATA_DIR (otherwise env or parse from script)")
    ap.add_argument("--filter-prefix", default=None,
                    help="Optional relative prefix (after DATA_DIR) to filter targets, e.g. "
                         "'dclm/.../dolma2-tokenizer/part-151-'")
    ap.add_argument("--show-existing", action="store_true", help="Also print all existing targets (can be long)")
    args = ap.parse_args()

    script_path = Path(args.script).expanduser().resolve()
    if not script_path.exists():
        print(f"ERROR: script not found: {script_path}", file=sys.stderr)
        sys.exit(1)

    data_dir = parse_data_dir(args.data_dir, script_path)
    if not data_dir:
        print("ERROR: DATA_DIR not provided, not in env, and not found in the script.", file=sys.stderr)
        sys.exit(2)

    targets_tokens = extract_targets_from_sh(script_path)
    if not targets_tokens:
        print("No -O targets found in the script.", file=sys.stderr)
        sys.exit(3)

    # Normalize into (rel, abs) and dedupe by rel to avoid duplicates
    rel_abs_pairs: List[Tuple[str, Optional[str]]] = []
    for tok in targets_tokens:
        rel, abs_path = normalize_to_relative(tok, data_dir)
        if args.filter_prefix and not rel.startswith(args.filter_prefix):
            continue
        rel_abs_pairs.append((rel, abs_path))
    if not rel_abs_pairs:
        print("No targets remain after filtering.", file=sys.stderr)
        sys.exit(4)

    # Deduplicate by relative path while preserving order
    seen = set()
    pairs_unique: List[Tuple[str, Optional[str]]] = []
    for rel, ab in rel_abs_pairs:
        if rel in seen:
            continue
        seen.add(rel)
        pairs_unique.append((rel, ab))

    # Check existence in script order
    missing: List[Tuple[str, str]] = []     # (rel, abs)
    existing: List[Tuple[str, str]] = []    # (rel, abs)
    for rel, ab in pairs_unique:
        abs_path = ab if ab else str(Path(data_dir, rel))
        if Path(abs_path).is_file():
            existing.append((rel, abs_path))
        else:
            missing.append((rel, abs_path))

    # Report
    print(f"DATA_DIR: {data_dir}")
    print(f"Total unique targets: {len(pairs_unique)}")
    print(f"Existing: {len(existing)}  |  Missing: {len(missing)}")

    if missing:
        print("\n--- Missing files (relative -> absolute) ---")
        for rel, ab in missing:
            print(f"{rel} -> {ab}")

    if existing:
        last_rel, last_abs = existing[-1]   # last by script order
        print("\n--- Last existing (by script order) ---")
        print(last_rel)
        print(last_abs)
        if args.show_existing:
            print("\n--- All existing (in script order) ---")
            for rel, ab in existing:
                print(f"{rel} -> {ab}")
    else:
        print("\nNo existing targets found.")

if __name__ == "__main__":
    main()
