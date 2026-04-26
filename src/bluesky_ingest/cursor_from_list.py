"""S3-LIST cursor resolver — derive max time_us from parquet filenames.

On container startup, resolve the cursor by listing the S3 prefix
and parsing the maximum integer from `<last_time_us>.parquet` filenames.
Empty prefix returns None (cold start).
"""
from __future__ import annotations


def resolve_cursor_us(s3_client, bucket: str, prefix: str) -> int | None:
    """Resolve cursor from S3-LIST by parsing max time_us from filenames.

    Lists all objects in s3://<bucket>/<prefix>/, parses filenames as
    `<last_time_us>.parquet`, and returns the numeric maximum time_us
    value, or None if the prefix is empty.

    Malformed filenames (not parseable as int, wrong suffix) are silently
    skipped. Handles paginated results (>1000 files).

    Args:
        s3_client: boto3 S3 client.
        bucket: S3 bucket name.
        prefix: S3 prefix to list (should end with '/').

    Returns:
        Maximum time_us (int) found in the prefix, or None if empty.
    """
    max_time_us: int | None = None
    pag = s3_client.get_paginator("list_objects_v2")

    for page in pag.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Strip the prefix and extract the filename
            if key.startswith(prefix):
                filename = key[len(prefix) :]
            else:
                # Shouldn't happen, but skip if it does
                continue

            # Parse filename: expect format `<time_us>.parquet`
            if not filename.endswith(".parquet"):
                continue

            # Remove `.parquet` suffix
            name_without_ext = filename[: -len(".parquet")]

            # Try to parse as integer
            try:
                time_us = int(name_without_ext)
            except ValueError:
                # Malformed filename; skip it
                continue

            # Track the maximum
            if max_time_us is None or time_us > max_time_us:
                max_time_us = time_us

    return max_time_us
