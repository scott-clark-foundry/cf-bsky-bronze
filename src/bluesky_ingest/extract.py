"""JSON validation + materialized column extraction for ingest.

Each raw jetstream JSON line is parsed and reduced to an ExtractedRow with the
columns that land in bronze Parquet. Invalid JSON returns None and the caller
silently skips the message.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import orjson

from bluesky_ingest.hash import dedup_hash


@dataclass(frozen=True, slots=True)
class ExtractedRow:
    data: str               # raw JSON line (decoded UTF-8)
    kind: str               # 'commit' | 'identity' | 'account'
    time_us: int            # jetstream cursor (microseconds)
    created_ts: int | None  # authoring-clock microseconds since epoch; pyarrow casts
                            # this int to BRONZE_SCHEMA's timestamp[us, tz=UTC] field.
                            # None when the event has no record-level event time
                            # (deletes, updates, records without createdAt, parse
                            # failures).
    dedup_hash: str         # 32-char hex xxh128


def extract_row(line: bytes) -> ExtractedRow | None:
    """Parse one jetstream JSON line into an ExtractedRow. Returns None on invalid JSON."""
    try:
        event = orjson.loads(line)
    except orjson.JSONDecodeError:
        return None

    kind = event.get("kind", "")
    time_us = int(event.get("time_us", 0))
    created_ts = _extract_created_ts(event, kind)
    h = dedup_hash(event)
    return ExtractedRow(
        data=line.decode("utf-8"),
        kind=kind,
        time_us=time_us,
        created_ts=created_ts,
        dedup_hash=h,
    )


def _extract_created_ts(event: dict[str, Any], kind: str) -> int | None:
    """Extract the authoring-clock timestamp as microseconds, or None if absent.

    For commit events, only `op="create"` carries a meaningful event time:

      - delete: the record is gone — there is no record.createdAt to read
      - update: record.createdAt reflects the record's *birth time*, not the
        update event time (e.g. a profile update preserves the profile's
        original creation timestamp). Treating it as event time silently
        misroutes events into stale partitions

    For both, we return None and let downstream consumers fall back to
    time_us when an authoring timestamp is needed.
    """
    raw: str | None = None
    if kind == "commit":
        commit = event.get("commit", {})
        if commit.get("operation") != "create":
            return None
        raw = commit.get("record", {}).get("createdAt")
    elif kind == "identity":
        raw = event.get("identity", {}).get("time")
    elif kind == "account":
        raw = event.get("account", {}).get("time")

    if not raw:
        return None
    try:
        cleaned = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000)
    except (ValueError, TypeError):
        return None
