"""Shared structlog setup for the Fyralis BYOC control plane.

A single ``configure_logging()`` entry point so every CP service (auth proxy,
agent, console, onboarding, licensing, metering...) emits structured logs in a
consistent shape. Defaults to JSON (machine-ingestible into Loki) with an
opt-in human-readable console renderer for local dev.

Usage::

    from control_plane.lib.logging import configure_logging, get_logger

    configure_logging()                 # once, at process start
    log = get_logger("auth-proxy")
    log.info("tenant_authenticated", tenant_id="acme", fingerprint=fp)

Configuration is read from the environment via ``lib.config`` when available,
but ``configure_logging`` also accepts explicit overrides so it has no hard
dependency cycle with config.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Literal

import structlog

__all__ = [
    "configure_logging",
    "get_logger",
    "add_service_context",
]

_CONFIGURED = False

LogFormat = Literal["json", "console"]


def add_service_context(service: str | None) -> structlog.types.Processor:
    """Build a processor that stamps a static ``service`` field on every event."""

    def _processor(
        _logger: Any, _method: str, event_dict: structlog.types.EventDict
    ) -> structlog.types.EventDict:
        if service and "service" not in event_dict:
            event_dict["service"] = service
        return event_dict

    return _processor


def _resolve_level(level: str | int | None) -> int:
    if level is None:
        level = os.environ.get("CP_LOG_LEVEL", "INFO")
    if isinstance(level, int):
        return level
    resolved = logging.getLevelName(str(level).upper())
    # logging.getLevelName returns an int for known names, else a str.
    return resolved if isinstance(resolved, int) else logging.INFO


def _resolve_format(fmt: LogFormat | None) -> LogFormat:
    if fmt is not None:
        return fmt
    env = os.environ.get("CP_LOG_FORMAT", "json").strip().lower()
    return "console" if env == "console" else "json"


def configure_logging(
    *,
    service: str | None = None,
    level: str | int | None = None,
    fmt: LogFormat | None = None,
    force: bool = False,
) -> None:
    """Configure structlog + the stdlib root logger for the whole process.

    Idempotent: repeated calls are no-ops unless ``force=True``. Honors the
    ``CP_LOG_LEVEL`` and ``CP_LOG_FORMAT`` env vars when args are omitted.
    """
    global _CONFIGURED
    if _CONFIGURED and not force:
        return

    resolved_level = _resolve_level(level)
    resolved_format = _resolve_format(fmt)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        add_service_context(service),
    ]

    if resolved_format == "console":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer(
            colors=sys.stderr.isatty()
        )
    else:
        # Render exceptions as a structured field for JSON / Loki ingestion.
        shared_processors.append(structlog.processors.format_exc_info)
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(resolved_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # Route stdlib logging (uvicorn, httpx, cryptography warnings) to stderr at
    # the same level so nothing logs to a different sink.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=resolved_level,
        force=True,
    )

    _CONFIGURED = True


def get_logger(name: str | None = None, **initial_values: Any) -> Any:
    """Return a bound structlog logger, configuring with defaults if needed."""
    if not _CONFIGURED:
        configure_logging()
    logger = structlog.get_logger(name) if name else structlog.get_logger()
    if initial_values:
        logger = logger.bind(**initial_values)
    return logger
