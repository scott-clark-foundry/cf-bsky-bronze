"""Environment-driven configuration for bluesky-ingest.

Configuration is sourced entirely from environment variables. The cursor is
resolved from S3 LIST at startup (see cursor_from_list.py), so no on-disk
cursor state is read here. All variables are required unless documented
otherwise.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Config:
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    jetstream_url: str
    ingest_batch_size: int
    ingest_timeout_seconds: int
    bronze_s3_prefix: str
    sleep_seconds: int = 60

    @property
    def bronze_s3_tmp_prefix(self) -> str:
        """Derive the tmp prefix from bronze_s3_prefix (rstrip + -tmp/)."""
        return self.bronze_s3_prefix.rstrip("/") + "-tmp/"


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required env var: {name}")
    return value


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def load_config() -> Config:
    return Config(
        s3_endpoint=_required("HETZNER_S3_ENDPOINT"),
        s3_access_key=_required("HETZNER_S3_ACCESS_KEY"),
        s3_secret_key=_required("HETZNER_S3_SECRET_KEY"),
        s3_bucket=_required("HETZNER_S3_BUCKET"),
        jetstream_url=_required("JETSTREAM_URL"),
        ingest_batch_size=_int("INGEST_BATCH_SIZE", 100_000),
        ingest_timeout_seconds=_int("INGEST_TIMEOUT_SECONDS", 30),
        bronze_s3_prefix=_required("BRONZE_S3_PREFIX"),
        sleep_seconds=_int("BLUESKY_INGEST_SLEEP_SECONDS", 60),
    )
