"""boto3 S3 client factory."""
from __future__ import annotations

import boto3
from botocore.client import BaseClient
from botocore.config import Config as BotoConfig

from bluesky_ingest.config import Config


def make_s3_client(cfg: Config) -> BaseClient:
    """Construct an S3-compatible client (Hetzner / MinIO) with path-style addressing."""
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )
