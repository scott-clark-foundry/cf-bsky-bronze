"""Bronze parquet writer — builds PyArrow Table and writes snappy bytes."""
from __future__ import annotations

import io

import pyarrow.parquet as pq

from bluesky_ingest.extract import ExtractedRow
from bluesky_ingest.parquet_writer import (
    BRONZE_SCHEMA,
    build_table,
    parquet_key_for_batch,
    write_parquet_bytes,
)


def _row(time_us: int, kind: str = "commit") -> ExtractedRow:
    return ExtractedRow(
        data='{"stub":true}',
        kind=kind,
        time_us=time_us,
        created_ts=time_us,
        dedup_hash="0" * 32,
    )


def test_build_table_matches_bronze_schema() -> None:
    rows = [_row(100), _row(200)]
    table = build_table(rows)
    assert table.schema == BRONZE_SCHEMA
    assert table.num_rows == 2
    assert table.column("time_us").to_pylist() == [100, 200]


def test_write_parquet_bytes_roundtrip() -> None:
    rows = [_row(1_700_000_000_000_000), _row(1_700_000_000_000_100)]
    table = build_table(rows)
    data = write_parquet_bytes(table)
    reader = pq.ParquetFile(io.BytesIO(data))
    read_back = reader.read()
    assert read_back.num_rows == 2
    assert read_back.column("time_us").to_pylist() == [
        1_700_000_000_000_000,
        1_700_000_000_000_100,
    ]


def test_parquet_key_for_batch_uses_last_time_us() -> None:
    rows = [_row(100), _row(200), _row(300)]
    assert parquet_key_for_batch(rows, prefix="bronze/") == "bronze/300.parquet"


def test_build_table_handles_null_created_ts() -> None:
    rows = [
        ExtractedRow(
            data='{"a":1}', kind="commit", time_us=100,
            created_ts=None, dedup_hash="0" * 32,
        ),
        ExtractedRow(
            data='{"a":2}', kind="commit", time_us=200,
            created_ts=200, dedup_hash="1" * 32,
        ),
    ]
    table = build_table(rows)
    assert table.num_rows == 2
    assert table.column("created_ts").to_pylist()[0] is None
