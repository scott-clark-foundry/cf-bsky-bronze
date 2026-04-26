"""bluesky-ingest entry point.

Streaming, resilient design: one long-running process that loops forever,
draining a Jetstream WebSocket into S3-parquet batches.

Architecture
------------
  ``main()``
    Loads config, sets up logging, then delegates to ``main_loop()``.

  ``main_loop(cfg, log)``
    Runs ``run_once`` in a ``while True`` loop, sleeping ``cfg.sleep_seconds``
    between iterations. Catches and logs all exceptions so the process stays
    alive across transient failures (S3 blips, network hiccups, etc.).  The
    outer Docker / systemd restart policy is the last-resort failsafe.

  ``run_once(cfg, log)``
    Single ingest iteration:
    1. Build an S3 client and sweep stale tmp objects from prior crashes.
    2. Resolve the cursor from the S3 prefix (S3-LIST of ``<prefix>/*.parquet``
       filenames parsed as integers; numeric max = last committed event).
    3. Subscribe to Jetstream at ``cursor + 1`` (or live-tail if no prior
       files exist).
    4. Drain events into a ``StreamingBronzeWriter`` until:
       - The batch hits ``ingest_batch_size`` — finalize, log ``batch_full``
         (with ``lag_us`` so operators can tell catchup from steady state),
         open a fresh writer, keep reading.
       - ``ingest_timeout_seconds`` of read-idle elapses — live-tail reached;
         finalize partial batch (logs ``batch_written``) and return.
       - ``MAX_RUN_SECONDS`` wall-clock expires — flush and return.
       - Server closes the connection — commit partial batch, reconnect.

Durability invariant
--------------------
The successful S3 PUT inside ``writer.finalize`` is the commit.  The cursor is
re-resolved from S3-LIST on every ``run_once`` call so no on-disk cursor state
is needed. Any crash before finalize leaves events in the ``<prefix>-tmp/``
object prefix; ``cleanup_orphaned_tmp`` sweeps these on the next startup.
"""
from __future__ import annotations

import asyncio
import sys
import time

import botocore.exceptions
import websockets
import websockets.exceptions

from bluesky_ingest import cursor_from_list
from bluesky_ingest.config import Config, load_config
from bluesky_ingest.extract import extract_row
from bluesky_ingest.logging import setup_logging
from bluesky_ingest.parquet_writer import (
    StreamingBronzeWriter,
    cleanup_orphaned_tmp,
)
from bluesky_ingest.s3 import make_s3_client


# Wall-clock ceiling for a single run_once call.  Bounds the window during
# which the in-process cursor can drift from S3 truth: each iter exit
# re-resolves the cursor via S3 LIST, so a 120s ceiling means cursor
# divergence stays bounded by 2 minutes. Also bounds detection latency for
# a hung WS that doesn't raise ConnectionClosed.
MAX_RUN_SECONDS = 120


def _subscribe_url(base: str, cursor_us: int) -> str:
    return f"{base}/subscribe?wantedCollections=app.*&cursor={cursor_us}"


def _live_tail_url(base: str) -> str:
    return f"{base}/subscribe?wantedCollections=app.*"


async def run_once(
    cfg: Config,
    log,
) -> int:
    """Run one ingest iteration. Returns total events written across batches."""
    s3_client = make_s3_client(cfg)

    # Best-effort sweep of stale tmp parquet objects from prior crashes.
    try:
        orphans = cleanup_orphaned_tmp(s3_client, cfg, older_than_seconds=3600)
        if orphans:
            log.info("bronze_tmp_orphans_swept", count=orphans)
    except Exception as e:  # non-fatal
        log.warning("bronze_tmp_sweep_failed", error=repr(e))

    # Resolve cursor from S3 LIST.
    resolved = cursor_from_list.resolve_cursor_us(
        s3_client, cfg.s3_bucket, cfg.bronze_s3_prefix
    )
    if resolved is None:
        log.info("cold_start_no_cursor")
        cursor_us: int | None = None
    else:
        log.info("cursor_resumed_from_list", time_us=resolved)
        # +1: skip the event already committed (Jetstream delivers time_us > cursor)
        cursor_us = resolved + 1

    run_start = time.monotonic()
    total_written = 0
    batches_written = 0
    closed_early_count = 0

    # Per-connection batch state (hoisted so _flush_batch is defined once per run_once)
    rows_in_batch = 0
    writer: StreamingBronzeWriter | None = None
    batch_start = time.monotonic()

    def _flush_batch(closed_early: bool, full_batch: bool = False) -> None:
        nonlocal rows_in_batch, writer, batch_start
        nonlocal total_written, batches_written, closed_early_count, cursor_us
        if writer is None:
            return
        if rows_in_batch == 0:
            writer.abort(s3_client)
            writer = None
            return
        final_key, count = writer.finalize(s3_client)
        new_cursor = writer.last_time_us
        assert new_cursor is not None  # rows_in_batch > 0 ⇒ last_time_us set
        # Advance in-process cursor for reconnect within the same run.
        # On the next run_once call the cursor is re-resolved from S3 LIST.
        cursor_us = new_cursor + 1
        total_written += count
        batches_written += 1
        if closed_early:
            closed_early_count += 1
        # lag_us = how far behind live the just-written batch is.
        # ~0 means we're at firehose tail; large means bronze is catching up.
        lag_us = int(time.time() * 1_000_000) - new_cursor
        if full_batch:
            log.info(
                "batch_full",
                events=count,
                cursor=new_cursor,
                file_name=final_key,
                lag_us=lag_us,
            )
        else:
            log.info(
                "batch_written",
                events=count,
                batch_write_duration_ms=int((time.monotonic() - batch_start) * 1000),
                cursor=new_cursor,
                file_name=final_key,
                closed_early=closed_early,
                lag_us=lag_us,
            )
        writer = None
        rows_in_batch = 0

    # try/finally guarantees `run_complete` fires on every exit path —
    # idle timeout, MAX_RUN_SECONDS, or unexpected exception. Without this,
    # the inner `return total_written` calls below would skip the log.
    try:
        while time.monotonic() - run_start < MAX_RUN_SECONDS:
            # Build the WS URL from the current in-process cursor.
            if cursor_us is None:
                url = _live_tail_url(cfg.jetstream_url)
            else:
                url = _subscribe_url(cfg.jetstream_url, cursor_us)

            # Reset per-connection batch state on each reconnect
            rows_in_batch = 0
            writer = None
            batch_start = time.monotonic()

            try:
                async with websockets.connect(url, max_size=None) as ws:
                    log.info("ws_connected", cursor=cursor_us)
                    writer = StreamingBronzeWriter(cfg)
                    batch_start = time.monotonic()
                    while time.monotonic() - run_start < MAX_RUN_SECONDS:
                        remaining_overall = MAX_RUN_SECONDS - (time.monotonic() - run_start)
                        recv_timeout = min(cfg.ingest_timeout_seconds, remaining_overall)
                        if recv_timeout <= 0:
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                        except asyncio.TimeoutError:
                            # Reached live tail. Flush and exit this run.
                            log.info("ws_idle_timeout", rows_in_batch=rows_in_batch)
                            _flush_batch(closed_early=False)
                            return total_written

                        data = raw.encode() if isinstance(raw, str) else bytes(raw)
                        row = extract_row(data)
                        if row is None:
                            continue
                        if writer is None:
                            # Just after a full-batch flush — start a new writer.
                            writer = StreamingBronzeWriter(cfg)
                            batch_start = time.monotonic()
                            rows_in_batch = 0
                        writer.add(row)
                        rows_in_batch += 1

                        if rows_in_batch >= cfg.ingest_batch_size:
                            _flush_batch(closed_early=False, full_batch=True)
                            # writer now None; next message opens a new one

                    # Hit MAX_RUN_SECONDS mid-stream. Flush in-flight rows and exit.
                    _flush_batch(closed_early=False)
                    return total_written

            except websockets.exceptions.ConnectionClosed as e:
                # Ambient flake or proxy-enforced max-duration. Commit partial
                # batch and reconnect from the advanced cursor.
                code = None
                if e.rcvd is not None:
                    code = getattr(e.rcvd, "code", None)
                log.warning(
                    "ws_closed_by_server",
                    code=code,
                    exc=repr(e),
                    rows_in_batch=rows_in_batch,
                )
                _flush_batch(closed_early=True)
                continue  # outer while: reconnect with updated cursor_us
    finally:
        log.info(
            "run_complete",
            batches=batches_written,
            total_events=total_written,
            closed_early=closed_early_count,
            elapsed_s=int(time.monotonic() - run_start),
        )

    return total_written


def main_loop(cfg: Config, log) -> None:
    """Run run_once in a forever loop, sleeping cfg.sleep_seconds between iters."""
    while True:
        try:
            asyncio.run(run_once(cfg, log=log))
        except (botocore.exceptions.BotoCoreError, OSError) as e:
            log.error("s3_put_failed", error=repr(e))
        except Exception as e:
            log.error("iter_crashed", error=repr(e))
        time.sleep(cfg.sleep_seconds)


def main() -> int:
    cfg = load_config()
    log = setup_logging("ingest")
    log.info(
        "ingest_start",
        batch_size=cfg.ingest_batch_size,
        timeout_s=cfg.ingest_timeout_seconds,
        jetstream_url=cfg.jetstream_url,
        s3_bucket=cfg.s3_bucket,
        bronze_s3_prefix=cfg.bronze_s3_prefix,
        sleep_seconds=cfg.sleep_seconds,
        max_run_seconds=MAX_RUN_SECONDS,
    )
    try:
        main_loop(cfg, log)
    except Exception as e:
        log.error("ingest_crashed", error=repr(e))
        return 1
    log.info("ingest_end")
    return 0


if __name__ == "__main__":
    sys.exit(main())
