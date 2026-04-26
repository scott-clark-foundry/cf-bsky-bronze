"""Integration tests for main.run_once — cursor resolution + WS stubbing.

Tests drive run_once directly with a moto S3 bucket and a stubbed
websockets.connect that yields no messages then raises asyncio.TimeoutError
(simulating the idle-timeout code path).

StreamingBronzeWriter uses pyarrow's internal S3 filesystem, which bypasses
boto3 and therefore bypasses moto.  For these cursor-resolution tests we only
care about:
  - which S3 LIST operations are performed (cursor resolution)
  - which log events are emitted
  - which WS URL is used (cursor +1 check)

We therefore stub StreamingBronzeWriter with a no-op in-memory stub so the
tests don't need a real or moto-interceptable pyarrow S3 filesystem.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from bluesky_ingest.config import Config
from bluesky_ingest.main import run_once


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

BUCKET = "test-bucket"
PREFIX = "bronze/"


def _make_cfg(**overrides) -> Config:
    defaults = dict(
        s3_endpoint="https://s3.amazonaws.com",
        s3_access_key="test-key",
        s3_secret_key="test-secret",
        s3_bucket=BUCKET,
        jetstream_url="wss://jetstream.test",
        ingest_batch_size=100_000,
        ingest_timeout_seconds=30,
        bronze_s3_prefix=PREFIX,
        sleep_seconds=180,
    )
    defaults.update(overrides)
    return Config(**defaults)


class _FakeWS:
    """Async context manager that immediately times out on recv."""

    def __init__(self, url: str, **kw):
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def recv(self):
        # Small sleep so asyncio.wait_for fires the timeout wrapper.
        await asyncio.sleep(0.01)
        raise asyncio.TimeoutError()


class _NoOpWriter:
    """Minimal in-memory stand-in for StreamingBronzeWriter.

    Never opens a real or moto S3 connection, so these tests don't
    depend on the pyarrow S3 filesystem layer.
    """

    def __init__(self, cfg):
        self._cfg = cfg
        self.last_time_us = None

    def add(self, row):
        self.last_time_us = row.time_us

    def abort(self, s3_client):
        pass

    def finalize(self, s3_client):
        # rows_in_batch is 0 in current tests so this path is unreachable;
        # real StreamingBronzeWriter returns (None, 0) for empty batches.
        return (None, 0)


@pytest.fixture
def captured_ws_urls():
    urls = []

    def _fake_connect(url, **kw):
        urls.append(url)
        return _FakeWS(url)

    with patch("websockets.connect", _fake_connect):
        yield urls


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_run_once_cold_start_logs_no_cursor_and_no_crash(
    captured_ws_urls,
) -> None:
    """Cold start (empty prefix) logs cold_start_no_cursor, not cursor_resumed_from_list."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        # No objects — empty prefix

        cfg = _make_cfg()
        log = MagicMock()

        with (
            patch("bluesky_ingest.main.make_s3_client", return_value=client),
            patch("bluesky_ingest.main.StreamingBronzeWriter", _NoOpWriter),
        ):
            result = await run_once(cfg, log=log)

    # Should return 0 events written (no messages delivered before timeout)
    assert result == 0

    # cold_start_no_cursor must appear exactly once
    cold_start_calls = [
        c for c in log.info.call_args_list
        if c.args and c.args[0] == "cold_start_no_cursor"
    ]
    assert len(cold_start_calls) == 1, (
        f"Expected exactly 1 cold_start_no_cursor log call, got {len(cold_start_calls)}. "
        f"All info calls: {log.info.call_args_list}"
    )

    # cursor_resumed_from_list must never appear
    resumed_calls = [
        c for c in log.info.call_args_list
        if c.args and c.args[0] == "cursor_resumed_from_list"
    ]
    assert len(resumed_calls) == 0, (
        f"Expected 0 cursor_resumed_from_list log calls, got {len(resumed_calls)}"
    )


async def test_run_once_resume_logs_cursor_resumed_with_correct_time_us(
    captured_ws_urls,
) -> None:
    """Resume from existing prefix: logs cursor_resumed_from_list with time_us=200.

    Three files: 100.parquet, 200.parquet, 9.parquet.  Numeric max is 200,
    not lex-sort max ("9").  Subscribe URL must contain cursor=201 (+1 offset).
    """
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        client.put_object(Bucket=BUCKET, Key=f"{PREFIX}100.parquet", Body=b"")
        client.put_object(Bucket=BUCKET, Key=f"{PREFIX}200.parquet", Body=b"")
        client.put_object(Bucket=BUCKET, Key=f"{PREFIX}9.parquet", Body=b"")

        cfg = _make_cfg()
        log = MagicMock()

        with (
            patch("bluesky_ingest.main.make_s3_client", return_value=client),
            patch("bluesky_ingest.main.StreamingBronzeWriter", _NoOpWriter),
        ):
            await run_once(cfg, log=log)

    # cursor_resumed_from_list logged with time_us=200
    resumed_calls = [
        c for c in log.info.call_args_list
        if c.args and c.args[0] == "cursor_resumed_from_list"
    ]
    assert len(resumed_calls) == 1, (
        f"Expected exactly 1 cursor_resumed_from_list, got {len(resumed_calls)}. "
        f"All info calls: {log.info.call_args_list}"
    )
    assert resumed_calls[0].kwargs.get("time_us") == 200, (
        f"Expected time_us=200, got {resumed_calls[0].kwargs}"
    )

    # Subscribe URL must contain cursor=201 (resolved + 1)
    assert captured_ws_urls, "websockets.connect was never called"
    url_used = captured_ws_urls[0]
    assert "cursor=201" in url_used, (
        f"Expected cursor=201 in WS URL, got: {url_used}"
    )
