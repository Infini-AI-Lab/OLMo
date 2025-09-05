#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download remote files listed in a YAML file into a local DATA_DIR.

Behavior:
- Read the YAML and collect URLs found under keys like `data-path(s)` / `data_path(s)`.
  If none are found, fallback to scanning ALL http(s) URLs anywhere in the YAML.
- For each URL, take its path component (without the domain) and remove the given
  `--trim-prefix` from the beginning (if present). Save the file to:
      DATA_DIR / <remaining_path>
- Optional filtering: if `--include-prefix` is provided, only download files whose
  relative path (after trimming) starts with one of the given prefixes.
- Supports concurrent downloads, auto-retry, and skipping files that already exist
  with the same Content-Length.

Install deps:
    pip install pyyaml requests
"""

import argparse
from pathlib import Path
import re
import sys
from typing import Any, List, Set
from urllib.parse import urlparse

import yaml
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DATA_KEYS = {"data-path", "data_path", "data-paths", "data_paths"}


def make_session(total_retries: int = 5, backoff_factor: float = 0.5, pool_maxsize: int = 20) -> requests.Session:
    """Create a requests session with retry/backoff and connection pooling."""
    sess = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["HEAD", "GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_maxsize, pool_maxsize=pool_maxsize)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


def find_urls_from_data_keys(obj: Any) -> List[str]:
    """Prefer URLs located under keys named like data-path(s)/data_path(s), at any depth."""
    found: List[str] = []

    def _walk(o: Any):
        if isinstance(o, dict):
            for k, v in o.items():
                if str(k) in DATA_KEYS:
                    found.extend(extract_urls_from_any(v))
                _walk(v)
        elif isinstance(o, list):
            for it in o:
                _walk(it)

    _walk(obj)
    return found


def extract_urls_from_any(obj: Any) -> List[str]:
    """Extract http(s) URLs from any Python object."""
    urls: List[str] = []
    if isinstance(obj, str):
        urls.extend(re.findall(r"https?://\S+", obj))
    elif isinstance(obj, list):
        for s in obj:
            urls.extend(extract_urls_from_any(s))
    elif isinstance(obj, dict):
        for v in obj.values():
            urls.extend(extract_urls_from_any(v))
    return urls


def fallback_find_all_urls(obj: Any) -> List[str]:
    """Fallback: collect ALL http(s) URLs from the entire YAML object."""
    return extract_urls_from_any(obj)


def normalize_relative_path(url: str, trim_prefix: str) -> str:
    """
    Convert the URL path into a relative filesystem path.
    If the path starts with `trim_prefix`, remove that prefix.

    Example:
      url:         https://host/a/b/c/file.npy
      trim_prefix: /a/b/
      result:      c/file.npy
    """
    parsed = urlparse(url)
    path = parsed.path or "/"
    rel = path.lstrip("/")
    trim = (trim_prefix or "").lstrip("/")

    if trim and rel.startswith(trim):
        rel = rel[len(trim):].lstrip("/")

    return rel


def ensure_parent_dir(p: Path) -> None:
    """Create the parent directory for file `p` if necessary."""
    p.parent.mkdir(parents=True, exist_ok=True)


def file_size(p: Path) -> int:
    """Return file size in bytes, or -1 if it doesn't exist."""
    try:
        return p.stat().st_size
    except FileNotFoundError:
        return -1


def should_skip(session: requests.Session, url: str, dst: Path) -> bool:
    """
    Skip download if the file already exists locally AND its size equals the remote Content-Length.
    If Content-Length is missing or can't be retrieved, don't skip.
    """
    if not dst.exists():
        return False
    local = file_size(dst)
    if local < 0:
        return False
    try:
        resp = session.head(url, timeout=20)
        cl = resp.headers.get("Content-Length")
        if cl is not None:
            try:
                remote = int(cl)
                return remote == local
            except ValueError:
                return False
    except requests.RequestException:
        return False
    return False


def gather_urls(yaml_obj: Any) -> List[str]:
    """Collect unique URLs, preferring `data-path(s)` keys, otherwise scanning all."""
    urls = find_urls_from_data_keys(yaml_obj)
    if not urls:
        urls = fallback_find_all_urls(yaml_obj)
    # Deduplicate while preserving order
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in urls:
        if u not in seen and u.startswith(("http://", "https://")):
            seen.add(u)
            uniq.append(u)
    return uniq


def normalize_prefixes(prefix_args: List[str]) -> List[str]:
    """
    Normalize a list of include-prefix arguments which may contain comma-separated values.
    - Remove leading slashes
    - Drop empty items
    """
    out: List[str] = []
    for raw in prefix_args or []:
        for token in raw.split(","):
            token = token.strip()
            if token:
                out.append(token.lstrip("/"))
    return out


def main():
    parser = argparse.ArgumentParser(description="Download files listed in YAML to DATA_DIR, trimming a URL path prefix.")
    parser.add_argument("--yaml", required=True, help="Path to the YAML file")
    parser.add_argument("--data-dir", required=True, help="Target DATA_DIR directory")
    parser.add_argument("--trim-prefix", default="/preprocessed/proof-pile-2/",
                        help="Prefix to strip from the URL path (default: /preprocessed/proof-pile-2/; set to empty string to keep full path)")
    parser.add_argument("--include-prefix", action="append", default=[],
                        help="(Optional) Subpath prefix(es) AFTER trimming. Only matching files will be downloaded. "
                             "Can be provided multiple times or as a comma-separated list. "
                             "Example: v0_decontaminated/arxiv/")
    parser.add_argument("--workers", type=int, default=8, help="Number of concurrent download threads (default: 8)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    with open(args.yaml, "r", encoding="utf-8") as f:
        yaml_obj = yaml.safe_load(f)

    urls = gather_urls(yaml_obj)
    if not urls:
        print("No URLs found in YAML. Please check your data-path / data_path entries or content.", file=sys.stderr)
        sys.exit(1)

    include_prefixes = normalize_prefixes(args.include_prefix)

    print(f"Found {len(urls)} URLs. Target directory: {data_dir}")
    if args.trim_prefix:
        print(f"Trim prefix: {args.trim_prefix}")
    else:
        print("No trim prefix: keeping full URL paths under DATA_DIR.")
    if include_prefixes:
        print("Include prefixes (after trimming):")
        for p in include_prefixes:
            print(f"  - {p}")
    else:
        print("No include-prefix provided: will download ALL items after trimming.")

    session = make_session()

    # Prepare download tasks with optional filtering by include-prefix
    tasks = []
    kept = 0
    for url in urls:
        rel = normalize_relative_path(url, args.trim_prefix)
        if include_prefixes and not any(rel.startswith(p) for p in include_prefixes):
            continue
        dst = data_dir / rel
        tasks.append((url, dst))
        kept += 1

    if kept == 0:
        print("No URLs match the provided include-prefix(es). Nothing to do.", file=sys.stderr)
        sys.exit(2)

    print(f"{kept} URL(s) match the filters and will be downloaded.")

    # Concurrent downloads
    from concurrent.futures import ThreadPoolExecutor, as_completed

    errors = 0
    with ThreadPoolExecutor(max_workers=max(args.workers, 1)) as ex:
        futs = [ex.submit(download_one, session, url, dst) for url, dst in tasks]
        for _ in as_completed(futs):
            pass  # output happens inside download_one

    print("All download tasks have been dispatched.")
    # Rough post-check: count files missing or zero-sized as failures
    for _, dst in tasks:
        if not dst.exists() or file_size(dst) <= 0:
            errors += 1

    if errors:
        print(f"Completed with {errors} files failed or incomplete. You can re-run this script; it will skip finished files.")
    else:
        print("All files downloaded successfully.")


if __name__ == "__main__":
    main()
