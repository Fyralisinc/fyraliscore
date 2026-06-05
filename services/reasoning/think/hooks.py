"""services/reasoning/think/hooks.py — reasoning context-augmentor seam.

The think pipeline assembles a context ``bundle`` from strict retrieval. Overlays
may extend that bundle before the LLM call — e.g. the demo overlay attaches the
full active commitment/decision ledger so a curated snapshot reads completely,
tolerating non-canonical states that strict retrieval rejects.

Core ships **no** augmentor: the default path is strict retrieval only. Overlays
register augmentors via the ``company_os.reasoning_augmentors`` entry-point group
(discovered lazily, once per process, so it works inside ``think_worker``):

    [project.entry-points."company_os.reasoning_augmentors"]
    demo = "fyralis_demo.reasoning:augment_full_ledger"

An augmentor is an async callable ``(*, conn, trigger, bundle, allowed_region)``
that may mutate ``bundle`` in place and returns the (possibly extended)
``allowed_region``. Augmentors are chained; exceptions propagate to the caller so
the think pipeline keeps its existing poisoned-transaction handling.

This module lives in ``services.reasoning`` and imports nothing from the app,
product, or ingest layers (only the stdlib) — consistent with the import-linter
contract.
"""
from __future__ import annotations

import importlib.metadata as importlib_metadata
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

log = structlog.get_logger("think.hooks")

_ENTRY_POINT_GROUP = "company_os.reasoning_augmentors"

# (*, conn, trigger, bundle, allowed_region) -> new allowed_region
Augmentor = Callable[..., Awaitable[list[Any]]]

_augmentors: list[Augmentor] | None = None


def _discover() -> list[Augmentor]:
    global _augmentors
    if _augmentors is not None:
        return _augmentors
    found: list[Augmentor] = []
    try:
        entry_points = importlib_metadata.entry_points(group=_ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - discovery must never break think
        log.warning("reasoning_augmentor_discovery_failed", exc_info=True)
        _augmentors = found
        return found
    for ep in entry_points:
        try:
            found.append(ep.load())
            log.info("reasoning_augmentor_registered", source=ep.name)
        except Exception:  # noqa: BLE001 - one bad overlay must not break others
            log.warning(
                "reasoning_augmentor_load_failed", source=ep.name, exc_info=True
            )
    _augmentors = found
    return found


async def augment_context(*, conn: Any, trigger: Any, bundle: Any, allowed_region: list[Any]) -> list[Any]:
    """Run registered augmentors in order, returning the final allowed_region.

    No-op (returns ``allowed_region`` unchanged) when no overlay is installed.
    """
    for augmentor in _discover():
        allowed_region = await augmentor(
            conn=conn, trigger=trigger, bundle=bundle, allowed_region=allowed_region
        )
    return allowed_region


def reset_for_tests() -> None:
    global _augmentors
    _augmentors = None
