"""services/ingest/ingestion/enrichers.py — draft-enricher registry (the E2 seam).

Generalizes the former hardcoded github inline hook (the deleted
``if channel == "github:webhook": maybe_enrich_github_draft(...)`` in
``core.ingest_from_draft``) into a channel-keyed registry. An enricher mutates
``draft.content`` in place *before persistence* so the same observation row
carries the derived signal.

Two registration paths:
  - **in-repo first-party** — ``@register_enricher(channel)`` at import time
    (mirrors the handler registry);
  - **installed extensions** — a ``DraftEnricher`` (or list, or zero-arg callable
    returning either) contributed through the ``company_os.draft_enrichers``
    entry-point group, discovered once per process and failure-isolated. **Core
    never imports the extension** — it only discovers what is installed (the same
    inversion as the gateway / reasoning entry-point seams).

``run_enrichers`` runs every enricher for a channel in registration order, each
wrapped **raw-on-failure**: an enricher that raises or times out is swallowed so
the raw draft still persists. No-op when none are registered.
"""
from __future__ import annotations

import importlib.metadata as importlib_metadata
import logging
from typing import Any
from uuid import UUID

from lib.extensions.host_api.v1 import DraftEnricher, EnricherFn

log = logging.getLogger("ingestion.enrichers")

_ENTRY_POINT_GROUP = "company_os.draft_enrichers"

# Decorator-registered (in-repo) enrichers, channel -> ordered list.
_REGISTERED: dict[str, list[DraftEnricher]] = {}
# Entry-point-discovered enrichers, resolved once per process (None = not yet).
_discovered: dict[str, list[DraftEnricher]] | None = None


def register_enricher(channel: str, *, name: str = "enricher"):
    """Decorator: register ``fn`` as an enricher for ``channel`` (in-repo path).

    Unlike the handler registry, multiple enrichers may register for the same
    channel — they run in registration order. ``fn`` must match the
    :data:`~lib.extensions.host_api.v1.EnricherFn` shape
    ``async def fn(draft, *, pool, tenant_id) -> None``.
    """

    def _decorator(fn: EnricherFn) -> EnricherFn:
        _REGISTERED.setdefault(channel, []).append(
            DraftEnricher(channel=channel, fn=fn, name=name)
        )
        return fn

    return _decorator


def _discover() -> dict[str, list[DraftEnricher]]:
    global _discovered
    if _discovered is not None:
        return _discovered
    found: dict[str, list[DraftEnricher]] = {}
    try:
        entry_points = importlib_metadata.entry_points(group=_ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - discovery must never break ingest
        log.warning("draft_enricher_discovery_failed", exc_info=True)
        _discovered = found
        return found
    for ep in entry_points:
        try:
            obj = ep.load()
            resolved = obj() if callable(obj) and not isinstance(obj, DraftEnricher) else obj
            items = resolved if isinstance(resolved, (list, tuple)) else [resolved]
            for it in items:
                if not isinstance(it, DraftEnricher):
                    log.error("draft_enricher_bad_type source=%s", ep.name)
                    continue
                found.setdefault(it.channel, []).append(it)
                log.info(
                    "draft_enricher_discovered source=%s channel=%s name=%s",
                    ep.name, it.channel, it.name,
                )
        except Exception:  # noqa: BLE001 - one bad extension must not break others
            log.error("draft_enricher_load_failed source=%s", ep.name, exc_info=True)
    _discovered = found
    return found


def _enrichers_for(channel: str) -> list[DraftEnricher]:
    return list(_REGISTERED.get(channel, [])) + list(_discover().get(channel, []))


async def run_enrichers(channel: str, draft: Any, *, pool: Any, tenant_id: UUID) -> None:
    """Run every enricher registered for ``channel``, in order, raw-on-failure.

    No-op when none are registered. Each enricher is wrapped so a raise/timeout
    is swallowed and the raw draft still persists — the raw-on-failure guarantee
    that used to live inline in ``core.ingest_from_draft``.
    """
    for enr in _enrichers_for(channel):
        try:
            await enr.fn(draft, pool=pool, tenant_id=tenant_id)
        except Exception:  # noqa: BLE001 - enrichment must never break ingest
            log.warning(
                "draft_enricher_failed channel=%s name=%s", channel, enr.name,
                exc_info=True,
            )


def registered_channels() -> list[str]:
    """Channels that have at least one enricher (in-repo or discovered)."""
    return sorted(set(_REGISTERED) | set(_discover()))


def enricher_names(channel: str) -> list[str]:
    """Names of the enrichers that would run for ``channel``, in order."""
    return [e.name for e in _enrichers_for(channel)]


def reset_for_tests() -> None:
    """Drop in-repo registrations and force entry-point re-discovery."""
    global _discovered
    _REGISTERED.clear()
    _discovered = None


__all__ = [
    "register_enricher",
    "run_enrichers",
    "registered_channels",
    "enricher_names",
    "reset_for_tests",
]
