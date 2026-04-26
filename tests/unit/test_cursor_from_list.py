"""S3-LIST cursor resolver — parse max time_us from parquet filenames."""
from __future__ import annotations

import boto3
from moto import mock_aws

from bluesky_ingest.cursor_from_list import resolve_cursor_us


@mock_aws
def test_empty_prefix_returns_none() -> None:
    """Empty prefix (cold start) should return None."""
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="test-bucket")

    result = resolve_cursor_us(client, "test-bucket", "bronze/")
    assert result is None


@mock_aws
def test_single_file() -> None:
    """Single file in prefix returns its time_us."""
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="test-bucket")
    client.put_object(Bucket="test-bucket", Key="bronze/1700000000000000.parquet", Body=b"")

    result = resolve_cursor_us(client, "test-bucket", "bronze/")
    assert result == 1700000000000000


@mock_aws
def test_multiple_files_numeric_max() -> None:
    """Multiple files returns numeric max, not lex-sort max.

    This tests the case where 9 and 100 both exist; numeric max is 100
    but lex-sort max would be "9".
    """
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="test-bucket")
    client.put_object(Bucket="test-bucket", Key="bronze/100.parquet", Body=b"")
    client.put_object(Bucket="test-bucket", Key="bronze/9.parquet", Body=b"")
    client.put_object(Bucket="test-bucket", Key="bronze/50.parquet", Body=b"")

    result = resolve_cursor_us(client, "test-bucket", "bronze/")
    assert result == 100


@mock_aws
def test_malformed_filenames_skipped() -> None:
    """Malformed filenames are ignored; valid ones extracted.

    Includes:
    - notanumber.parquet (not parseable as int)
    - something_else.txt (wrong suffix)
    - valid .parquet files
    """
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="test-bucket")
    # Valid files
    client.put_object(Bucket="test-bucket", Key="bronze/1000.parquet", Body=b"")
    client.put_object(Bucket="test-bucket", Key="bronze/2000.parquet", Body=b"")
    # Malformed files (should be skipped)
    client.put_object(Bucket="test-bucket", Key="bronze/notanumber.parquet", Body=b"")
    client.put_object(Bucket="test-bucket", Key="bronze/something_else.txt", Body=b"")

    result = resolve_cursor_us(client, "test-bucket", "bronze/")
    assert result == 2000


@mock_aws
def test_paginated_result_over_1000_files() -> None:
    """Paginated results (>1000 files) handled correctly.

    S3 LIST paginator returns up to 1000 items per page; resolver must
    iterate through all pages to find the true max.
    """
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="test-bucket")

    # Create 1100 files to exceed the 1000-item page boundary
    # We'll put them as 1000000, 1000001, ..., 1001099
    for i in range(1000, 2100):
        time_us = 1000000 + i
        client.put_object(
            Bucket="test-bucket",
            Key=f"bronze/{time_us}.parquet",
            Body=b"",
        )

    result = resolve_cursor_us(client, "test-bucket", "bronze/")
    # The max should be 1000000 + 2099 = 1002099
    assert result == 1002099
