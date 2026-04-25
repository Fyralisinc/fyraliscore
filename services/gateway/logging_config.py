"""services/gateway/logging_config.py — structlog bootstrap.

BUILD-PLAN §3 Prompt 2.A:
    "Structured logging with request_id, tenant_id, actor_id."

structlog's ContextVar-based binder means each request's logger is
isolated automatically. The Gateway middleware binds `request_id`,
`tenant_id`, `actor_id` on request enter and logs method/path/status/
duration on exit.
"""
from __future__ import annotations

import logging
import sys

import structlog


def configure_structlog(level: str = "INFO") -> None:
    """Install a JSON-rendering structlog processor chain.

    Idempotent — calling twice is harmless.
    """
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None):
    """Return a bound structlog logger. `name` is optional module hint."""
    return structlog.get_logger(name)


__all__ = ["configure_structlog", "get_logger"]
