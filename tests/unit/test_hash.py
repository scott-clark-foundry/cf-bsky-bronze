"""dedup_hash is xxh128 over kind-specific field sets."""
from __future__ import annotations

import pytest

from bluesky_ingest.hash import dedup_hash


COMMIT = {
    "did": "did:plc:alice",
    "kind": "commit",
    "time_us": 1_700_000_000_000_000,
    "commit": {
        "rev": "3l6hj5xdvn22v",
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": "abc123",
        "cid": "bafyreib...",
        "record": {"text": "hi", "createdAt": "2025-01-01T00:00:00Z"},
    },
}


def test_commit_hash_is_32_hex_chars() -> None:
    h = dedup_hash(COMMIT)
    assert len(h) == 32
    assert all(c in "0123456789abcdef" for c in h)


def test_commit_hash_ignores_time_us() -> None:
    other = {**COMMIT, "time_us": 2_000_000_000_000_000}
    assert dedup_hash(COMMIT) == dedup_hash(other)


def test_commit_hash_ignores_record_and_cid() -> None:
    other = {
        **COMMIT,
        "commit": {**COMMIT["commit"], "cid": "bafyZZZ", "record": {"text": "different"}},
    }
    assert dedup_hash(COMMIT) == dedup_hash(other)


def test_commit_hash_differs_on_rev() -> None:
    other = {**COMMIT, "commit": {**COMMIT["commit"], "rev": "3l6hj5xdvn22w"}}
    assert dedup_hash(COMMIT) != dedup_hash(other)


def test_commit_hash_differs_on_rkey() -> None:
    other = {**COMMIT, "commit": {**COMMIT["commit"], "rkey": "def456"}}
    assert dedup_hash(COMMIT) != dedup_hash(other)


def test_identity_hash_uses_seq() -> None:
    event = {
        "did": "did:plc:alice",
        "kind": "identity",
        "time_us": 1_700_000_000_000_000,
        "identity": {"seq": 42, "handle": "alice.bsky.social", "time": "..."},
    }
    h = dedup_hash(event)
    assert len(h) == 32
    other_time = {**event, "time_us": 9}
    assert dedup_hash(other_time) == h
    other_seq = {**event, "identity": {**event["identity"], "seq": 43}}
    assert dedup_hash(other_seq) != h


def test_account_hash_uses_seq() -> None:
    event = {
        "did": "did:plc:alice",
        "kind": "account",
        "time_us": 1_700_000_000_000_000,
        "account": {"seq": 7, "active": True, "time": "..."},
    }
    h = dedup_hash(event)
    assert len(h) == 32
    other_seq = {**event, "account": {**event["account"], "seq": 8}}
    assert dedup_hash(other_seq) != h


def test_unknown_kind_raises() -> None:
    with pytest.raises(ValueError, match="unknown kind"):
        dedup_hash({"kind": "mystery", "did": "x"})
