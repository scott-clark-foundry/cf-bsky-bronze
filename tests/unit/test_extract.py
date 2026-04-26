"""extract_row: raw JSON line -> materialized row dict (or None on invalid JSON)."""
from __future__ import annotations

import orjson

from bluesky_ingest.extract import ExtractedRow, extract_row


def test_extract_commit_event() -> None:
    event = {
        "did": "did:plc:alice",
        "kind": "commit",
        "time_us": 1_700_000_000_000_000,
        "commit": {
            "rev": "3l6hj5xdvn22v",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "abc123",
            "cid": "bafyZ",
            "record": {"text": "hi", "createdAt": "2025-01-01T00:00:00.000Z"},
        },
    }
    line = orjson.dumps(event)
    row = extract_row(line)
    assert isinstance(row, ExtractedRow)
    assert row.kind == "commit"
    assert row.time_us == 1_700_000_000_000_000
    assert row.created_ts == 1_735_689_600_000_000  # 2025-01-01 UTC
    assert row.data == line.decode("utf-8")
    assert len(row.dedup_hash) == 32


def test_extract_identity_event() -> None:
    event = {
        "did": "did:plc:alice",
        "kind": "identity",
        "time_us": 1_700_000_000_000_000,
        "identity": {
            "did": "did:plc:alice",
            "handle": "alice.bsky.social",
            "seq": 42,
            "time": "2025-01-01T00:00:00.000Z",
        },
    }
    row = extract_row(orjson.dumps(event))
    assert row is not None
    assert row.kind == "identity"
    assert row.created_ts == 1_735_689_600_000_000


def test_extract_account_event() -> None:
    event = {
        "did": "did:plc:bob",
        "kind": "account",
        "time_us": 1_700_000_000_000_000,
        "account": {
            "did": "did:plc:bob",
            "active": True,
            "status": "activated",
            "seq": 7,
            "time": "2025-01-01T00:00:00.000Z",
        },
    }
    row = extract_row(orjson.dumps(event))
    assert row is not None
    assert row.kind == "account"


def test_extract_invalid_json_returns_none() -> None:
    assert extract_row(b"{not json") is None


def test_extract_missing_created_ts_returns_none() -> None:
    event = {
        "did": "did:plc:alice",
        "kind": "commit",
        "time_us": 1_700_000_000_000_000,
        "commit": {
            "rev": "3l6hj5xdvn22v",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "abc123",
            "record": {"text": "hi"},
        },
    }
    row = extract_row(orjson.dumps(event))
    assert row is not None
    assert row.created_ts is None


def test_extract_delete_commit_returns_none_created_ts() -> None:
    event = {
        "did": "did:plc:alice",
        "kind": "commit",
        "time_us": 1_700_000_000_000_000,
        "commit": {
            "rev": "3l6hj5xdvn22v",
            "operation": "delete",
            "collection": "app.bsky.feed.post",
            "rkey": "abc123",
        },
    }
    row = extract_row(orjson.dumps(event))
    assert row is not None
    assert row.created_ts is None


def test_extract_update_commit_returns_none_created_ts() -> None:
    event = {
        "did": "did:plc:alice",
        "kind": "commit",
        "time_us": 1_700_000_000_000_000,
        "commit": {
            "rev": "3l6hj5xdvn22v",
            "operation": "update",
            "collection": "app.bsky.actor.profile",
            "rkey": "self",
            "cid": "bafyZ",
            "record": {"displayName": "Alice", "createdAt": "2024-02-07T22:41:23.278Z"},
        },
    }
    row = extract_row(orjson.dumps(event))
    assert row is not None
    assert row.created_ts is None


def test_extract_unparseable_created_ts_returns_none() -> None:
    event = {
        "did": "did:plc:alice",
        "kind": "commit",
        "time_us": 1_700_000_000_000_000,
        "commit": {
            "rev": "3l6hj5xdvn22v",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "abc123",
            "record": {"text": "hi", "createdAt": "not-a-timestamp"},
        },
    }
    row = extract_row(orjson.dumps(event))
    assert row is not None
    assert row.created_ts is None
