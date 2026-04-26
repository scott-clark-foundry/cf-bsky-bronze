"""structlog configuration for bluesky-ingest.

Logs JSON lines to stdout. Captured by `docker logs` when run in a
container, or by journald (`StandardOutput=journal`) when run under
systemd directly. No on-disk log files — operators inspect via the
runtime's log channel of choice.
"""
from __future__ import annotations

import logging

import structlog


def setup_logging(component: str) -> structlog.stdlib.BoundLogger:
    """Configure structlog for the given component. Returns a bound logger."""
    root_logger = logging.getLogger()
    root_logger.handlers = [logging.StreamHandler()]
    root_logger.setLevel(logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(component=component)
