"""Bronze parquet schema and writer.

Bronze schema (columns written to s3://bucket/<prefix>/{last_time_us}.parquet):

    data        string  -- raw JSON line, UTF-8
    kind        string  -- 'commit' | 'identity' | 'account'
    time_us     int64   -- jetstream cursor (signed; safe through year 294276)
    created_ts  timestamp[us, UTC]  -- authoring-clock time, or NULL when the
                                       event has no record-level event time
                                       (deletes, updates, records lacking
                                       createdAt, parse failures)
    dedup_hash  string  -- 32-char hex xxh128

The filename is the last row's time_us, giving consumers a
lexicographically-sortable listing that doubles as the cursor.

Two APIs coexist here:

1. `StreamingBronzeWriter` — writes events to S3 via `pq.ParquetWriter` in
   small record-batch flushes, keeping peak memory at O(STREAM_CHUNK_ROWS)
   regardless of `batch_size`. This is what `main.run_once` uses in
   production to avoid OOM under large catchup batches.

   Because S3 has no rename, the writer streams to a `<tmp_prefix><uuid>.parquet`
   key and `finalize()` does a server-side copy to the final
   `<prefix><last_time_us>.parquet` plus delete of the tmp. Consumers that
   list only the final prefix never see partial/in-flight tmp files.

2. `build_table` + `write_parquet_bytes` + `parquet_key_for_batch` — a
   fully-materialized in-memory path, kept for test fixtures and
   integration tests that generate bronze parquet without an S3
   filesystem.
"""
from __future__ import annotations

import io
import uuid
from collections.abc import Iterable
from urllib.parse import urlparse
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from bluesky_ingest.extract import ExtractedRow

if TYPE_CHECKING:
    from bluesky_ingest.config import Config


# Per-flush record-batch size for streaming writes. Keeps peak memory
# small and steady across batches of any size. JSON-heavy bronze rows
# benefit from a smaller chunk for lower peak memory.
STREAM_CHUNK_ROWS = 10_000


BRONZE_SCHEMA = pa.schema(
    [
        pa.field("data", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("time_us", pa.int64(), nullable=False),
        pa.field("created_ts", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("dedup_hash", pa.string(), nullable=False),
    ]
)


def make_pa_s3_fs(cfg: "Config") -> pafs.S3FileSystem:
    """Construct a pyarrow.fs.S3FileSystem pointed at cfg's S3 endpoint."""
    parsed = urlparse(cfg.s3_endpoint)
    hostport = parsed.netloc or cfg.s3_endpoint
    scheme = parsed.scheme or "https"
    return pafs.S3FileSystem(
        access_key=cfg.s3_access_key,
        secret_key=cfg.s3_secret_key,
        endpoint_override=hostport,
        scheme=scheme,
        region="us-east-1",
    )


class StreamingBronzeWriter:
    """Stream ExtractedRow events to a single S3 parquet file.

    Accumulates up to STREAM_CHUNK_ROWS events in an in-memory list, then
    flushes them as a RecordBatch to the underlying `pq.ParquetWriter`.
    Peak memory is O(STREAM_CHUNK_ROWS), independent of the total batch
    size the caller drains.

    The writer targets a temporary S3 key at construction
    (`<tmp_prefix><uuid>.parquet`, derived from cfg.bronze_s3_tmp_prefix).
    On `finalize()`, the writer is closed (which finalizes the pyarrow
    multipart upload), then a server-side copy places the parquet at its
    final `<prefix><last_time_us>.parquet` key and the tmp key is deleted.
    On `abort()` the tmp is deleted without being promoted.

    Intended single-use per batch: instantiate → add() many → finalize()
    → throw away. Re-using after finalize raises.
    """

    def __init__(self, cfg: "Config") -> None:
        self._cfg = cfg
        self._fs = make_pa_s3_fs(cfg)
        self._tmp_key = f"{cfg.bronze_s3_tmp_prefix}{uuid.uuid4().hex}.parquet"
        path = f"{cfg.s3_bucket}/{self._tmp_key}"
        self._writer: pq.ParquetWriter | None = pq.ParquetWriter(
            path,
            schema=BRONZE_SCHEMA,
            filesystem=self._fs,
            compression="snappy",
        )
        self._buffer: list[ExtractedRow] = []
        self._rows_written = 0
        self._last_time_us: int | None = None

    @property
    def tmp_key(self) -> str:
        return self._tmp_key

    @property
    def last_time_us(self) -> int | None:
        return self._last_time_us

    @property
    def rows_written(self) -> int:
        """Total rows passed to add() (includes unflushed buffer)."""
        return self._rows_written + len(self._buffer)

    def add(self, row: ExtractedRow) -> None:
        """Buffer one event; auto-flush when buffer reaches STREAM_CHUNK_ROWS."""
        if self._writer is None:
            raise RuntimeError("writer already finalized")
        self._buffer.append(row)
        self._last_time_us = row.time_us
        if len(self._buffer) >= STREAM_CHUNK_ROWS:
            self._flush_chunk()

    def _flush_chunk(self) -> None:
        if not self._buffer or self._writer is None:
            return
        batch = pa.RecordBatch.from_pydict(
            {
                "data": [r.data for r in self._buffer],
                "kind": [r.kind for r in self._buffer],
                "time_us": [r.time_us for r in self._buffer],
                "created_ts": [r.created_ts for r in self._buffer],
                "dedup_hash": [r.dedup_hash for r in self._buffer],
            },
            schema=BRONZE_SCHEMA,
        )
        self._writer.write_batch(batch)
        self._rows_written += len(self._buffer)
        self._buffer.clear()

    def finalize(self, s3_client) -> tuple[str | None, int]:
        """Close writer, rename tmp → final bronze key. Returns (final_key, rows).

        Returns (None, 0) if no rows were ever added (the tmp is deleted).
        """
        if self._writer is None:
            raise RuntimeError("writer already finalized")
        self._flush_chunk()
        self._writer.close()
        self._writer = None

        if self._last_time_us is None or self._rows_written == 0:
            # Nothing written; delete the empty tmp object best-effort
            try:
                s3_client.delete_object(Bucket=self._cfg.s3_bucket, Key=self._tmp_key)
            except Exception:
                pass
            return (None, 0)

        final_key = f"{self._cfg.bronze_s3_prefix}{self._last_time_us}.parquet"
        s3_client.copy_object(
            Bucket=self._cfg.s3_bucket,
            CopySource={"Bucket": self._cfg.s3_bucket, "Key": self._tmp_key},
            Key=final_key,
        )
        s3_client.delete_object(Bucket=self._cfg.s3_bucket, Key=self._tmp_key)
        return (final_key, self._rows_written)

    def abort(self, s3_client) -> None:
        """Close writer without promoting tmp; best-effort tmp delete."""
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
            self._writer = None
        try:
            s3_client.delete_object(Bucket=self._cfg.s3_bucket, Key=self._tmp_key)
        except Exception:
            pass


def cleanup_orphaned_tmp(s3_client, cfg: "Config", older_than_seconds: int = 3600) -> int:
    """Delete orphaned tmp-prefix objects older than older_than_seconds.

    Called at process start so prior crashes don't leak temp objects
    forever. Returns the number of objects deleted.
    """
    import datetime
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        seconds=older_than_seconds
    )
    pag = s3_client.get_paginator("list_objects_v2")
    to_delete: list[dict] = []
    for page in pag.paginate(Bucket=cfg.s3_bucket, Prefix=cfg.bronze_s3_tmp_prefix):
        for o in page.get("Contents", []):
            if o["LastModified"] < cutoff:
                to_delete.append({"Key": o["Key"]})
    deleted = 0
    for i in range(0, len(to_delete), 1000):
        chunk = to_delete[i : i + 1000]
        s3_client.delete_objects(
            Bucket=cfg.s3_bucket,
            Delete={"Objects": chunk, "Quiet": True},
        )
        deleted += len(chunk)
    return deleted


# ---------------------------------------------------------------------------
# In-memory API — for test fixtures and any consumer that needs bronze
# parquet bytes without an S3 filesystem. Production ingest uses
# StreamingBronzeWriter above.
# ---------------------------------------------------------------------------


def build_table(rows: Iterable[ExtractedRow]) -> pa.Table:
    """Construct a PyArrow Table matching BRONZE_SCHEMA."""
    rows_list = list(rows)
    data = [r.data for r in rows_list]
    kind = [r.kind for r in rows_list]
    time_us = [r.time_us for r in rows_list]
    created_ts = [r.created_ts for r in rows_list]
    dedup_hash = [r.dedup_hash for r in rows_list]

    return pa.table(
        {
            "data": pa.array(data, type=pa.string()),
            "kind": pa.array(kind, type=pa.string()),
            "time_us": pa.array(time_us, type=pa.int64()),
            "created_ts": pa.array(created_ts, type=pa.timestamp("us", tz="UTC")),
            "dedup_hash": pa.array(dedup_hash, type=pa.string()),
        },
        schema=BRONZE_SCHEMA,
    )


def write_parquet_bytes(table: pa.Table) -> bytes:
    """Serialize the table as a snappy-compressed Parquet file in memory."""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def parquet_key_for_batch(rows: list[ExtractedRow], prefix: str) -> str:
    """Compute the S3 key for a batch: {prefix}{last_time_us}.parquet."""
    if not rows:
        raise ValueError("cannot derive parquet key from empty batch")
    return f"{prefix}{rows[-1].time_us}.parquet"
