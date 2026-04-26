"""Operator-facing S3 LIST helper invoked by bin/bronze-status via docker exec.

Prints one tab-separated line: ``<count>\\t<newest_filename>`` for the
configured prefix, or ``0\\t`` if the prefix is empty. Uses the same
``Config`` + S3 client plumbing as the writer, so credentials and endpoint
come from the container's env — the host never needs them.

Designed to be called as ``python -m bluesky_ingest.s3_status``.
"""
from __future__ import annotations

import sys

from bluesky_ingest.config import load_config
from bluesky_ingest.s3 import make_s3_client


def main() -> int:
    cfg = load_config()
    client = make_s3_client(cfg)
    pag = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in pag.paginate(Bucket=cfg.s3_bucket, Prefix=cfg.bronze_s3_prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    if keys:
        # Filenames are `<time_us>.parquet` (fixed-width integer microseconds
        # since epoch + constant suffix), so lex sort = numeric sort.
        keys.sort()
        newest = keys[-1].rsplit("/", 1)[-1]
        print(f"{len(keys)}\t{newest}")
    else:
        print("0\t")
    return 0


if __name__ == "__main__":
    sys.exit(main())
