"""dedup_hash — xxh128 over kind-specific identifying fields.

The hash uniquely identifies an atproto event for deduplication across
re-deliveries. Field sets per kind are documented in `dedup_hash` below.
"""
from __future__ import annotations

from typing import Any

import xxhash


def dedup_hash(event: dict[str, Any]) -> str:
    """Return a 32-character hex xxh128 hash over the event's identifying fields.

    Raises ValueError for unknown kinds.
    """
    kind = event.get("kind")
    did = event.get("did", "")
    h = xxhash.xxh128()

    if kind == "commit":
        commit = event["commit"]
        for part in (
            did,
            kind,
            commit.get("rev", ""),
            commit.get("operation", ""),
            commit.get("collection", ""),
            commit.get("rkey", ""),
        ):
            h.update(str(part).encode("utf-8"))
            h.update(b"\x00")
    elif kind == "identity":
        identity = event["identity"]
        for part in (did, kind, str(identity.get("seq", ""))):
            h.update(part.encode("utf-8"))
            h.update(b"\x00")
    elif kind == "account":
        account = event["account"]
        for part in (did, kind, str(account.get("seq", ""))):
            h.update(part.encode("utf-8"))
            h.update(b"\x00")
    else:
        raise ValueError(f"unknown kind: {kind!r}")

    return h.hexdigest()
