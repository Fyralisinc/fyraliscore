"""lib/shared/events.py — process-local domain event bus.

A tiny publish/subscribe seam so core domain code can announce that something
happened ("a recommendation was created") without importing whoever cares
(today: the demo SSE fan-out). Core *publishes*; overlays *subscribe*.

Subscribers are discovered lazily, once per process, from installed packages
that declare the ``company_os.event_subscribers`` entry-point group. Each such
entry point is a callable ``register(subscribe)`` that wires its handlers. This
keeps the old semantics exactly:

* runs in whatever process calls :func:`publish` (gateway *or* a worker), so a
  recommendation created inside ``think_worker`` still reaches its subscribers;
* a no-op when no overlay is installed (the entry-point group is empty);
* a misbehaving subscriber can never break the publisher — handler exceptions
  are logged and swallowed.

This module lives in ``lib`` and therefore must not import ``services`` (the
import-linter contract enforces this); discovery uses only the stdlib.
"""
from __future__ import annotations

import importlib.metadata as importlib_metadata
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

log = structlog.get_logger("events")

Handler = Callable[..., Awaitable[None]]

_SUBSCRIBER_ENTRY_POINT_GROUP = "company_os.event_subscribers"

_subscribers: dict[str, list[Handler]] = {}
_discovered = False


def subscribe(topic: str, handler: Handler) -> None:
    """Register ``handler`` to be awaited whenever ``topic`` is published."""
    _subscribers.setdefault(topic, []).append(handler)


def unsubscribe(topic: str, handler: Handler) -> None:
    """Remove a previously registered handler if it is still present."""
    handlers = _subscribers.get(topic)
    if not handlers:
        return
    with contextlib.suppress(ValueError):
        handlers.remove(handler)
    if not handlers:
        _subscribers.pop(topic, None)


def _discover_subscribers() -> None:
    """Load overlay-provided subscribers once per process (idempotent)."""
    global _discovered
    if _discovered:
        return
    _discovered = True
    try:
        entry_points = importlib_metadata.entry_points(
            group=_SUBSCRIBER_ENTRY_POINT_GROUP
        )
    except Exception:  # noqa: BLE001 - discovery must never break a publisher
        log.warning("event_subscriber_discovery_failed", exc_info=True)
        return
    for ep in entry_points:
        try:
            register = ep.load()
            register(subscribe)
            log.info("event_subscribers_registered", source=ep.name)
        except Exception:  # noqa: BLE001 - one bad overlay must not poison others
            log.warning(
                "event_subscriber_register_failed", source=ep.name, exc_info=True
            )


async def publish(topic: str, /, **payload: Any) -> None:
    """Await every subscriber for ``topic``. No-op when none are registered."""
    _discover_subscribers()
    handlers = _subscribers.get(topic)
    if not handlers:
        return
    for handler in handlers:
        try:
            await handler(**payload)
        except Exception:  # noqa: BLE001 - a subscriber must never break publish
            log.warning("event_handler_failed", topic=topic, exc_info=True)


def reset_for_tests() -> None:
    """Clear all subscribers and force re-discovery (test isolation only)."""
    global _discovered
    _subscribers.clear()
    _discovered = False
