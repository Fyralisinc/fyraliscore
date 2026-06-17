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
from lib.extensions.registry import active_manifests

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
                if it.manifest_id is None:
                    # A discovered (installed-package) enricher MUST name its
                    # owning manifest so the host can govern it. The ungated
                    # (manifest_id=None) path is reserved for in-repo
                    # @register_enricher only — never an external entry point.
                    log.error(
                        "draft_enricher_missing_manifest_id source=%s name=%s",
                        ep.name, it.name,
                    )
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


def _manifests_by_id() -> dict[str, Any]:
    """Host-API-ACTIVE interface manifests keyed by id (cached underneath).

    Uses ``active_manifests`` (not the raw discovered set) so an enricher whose
    owning manifest is host-API-incompatible falls into the ``man is None`` →
    skip branch in ``_gate_allows`` — the same SemVer-pin gating the worker
    supervisor applies (ADR-0004 §A.4)."""
    return {m.id: m for m in active_manifests()}


async def _gate_allows(enr: DraftEnricher, *, pool: Any, tenant_id: UUID) -> bool:
    """Per-tenant capability gate for a contributed enricher.

    An enricher with ``manifest_id`` only runs when the host says the owning
    extension is enabled + granted for this tenant
    (``access.enricher_allowed``) — the runtime enforcement of the
    manifest/grant/feature-flag the catalog records. An in-repo enricher
    (``manifest_id is None``) is ungated (back-compat). Skips (returns False) if
    the owning manifest isn't discoverable — governance can't be verified.
    """
    if enr.manifest_id is None:
        return True
    man = _manifests_by_id().get(enr.manifest_id)
    if man is None:
        log.warning(
            "draft_enricher_no_manifest channel=%s name=%s manifest_id=%s",
            enr.channel, enr.name, enr.manifest_id,
        )
        return False
    # Imported lazily: the platform access layer pulls feature-flag + grants
    # repos that the ingest module shouldn't load at import time.
    from services.platform.extensions.access import enricher_allowed

    return await enricher_allowed(pool, tenant_id=tenant_id, manifest=man)


async def run_enrichers(channel: str, draft: Any, *, pool: Any, tenant_id: UUID) -> None:
    """Run every enricher registered for ``channel``, in order, raw-on-failure.

    No-op when none are registered. Each enricher is first checked against the
    per-tenant capability gate (skipped if its owning extension isn't
    enabled/granted for this tenant), then wrapped so a raise/timeout is
    swallowed and the raw draft still persists — the raw-on-failure guarantee
    that used to live inline in ``core.ingest_from_draft``.
    """
    for enr in _enrichers_for(channel):
        try:
            allowed = await _gate_allows(enr, pool=pool, tenant_id=tenant_id)
        except Exception:  # noqa: BLE001 - a gate failure must not break ingest
            log.warning(
                "draft_enricher_gate_failed channel=%s name=%s", channel, enr.name,
                exc_info=True,
            )
            continue
        if not allowed:
            continue
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
