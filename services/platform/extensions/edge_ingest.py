"""services/platform/extensions/edge_ingest.py — third-party edge-ingest (E3.2 / INV-6).

Developer-hosted extensions *derive* signals and write them back ONLY here, at the
ingestion edge, at a constrained trust tier. This module is the host-side logic the
``POST /ext/v1/ingest`` endpoint calls:

  * **Trust ceiling (INV-6).** Edge observations default to ``inferential_external``
    and may rise only to the per-grant ``trust_ceiling`` (max ``attested_agent``).
    ``authoritative`` / ``authoritative_external`` are **unreachable** for any
    extension. A POST asserting a tier above the ceiling (or an unreachable tier)
    is **rejected, not silently downgraded**.
  * **Namespacing.** The stored ``source_channel`` is host-prefixed
    ``ext:<extension_id>:<sub>`` so an extension can never write into a core channel
    (e.g. ``github:webhook``) or impersonate another extension.
  * **Same pipeline.** Persists via ``ingest_from_draft`` — the identical dedup +
    enrich + embed + think-trigger path the first-party handlers use — tagged with
    the extension as the source actor.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from lib.shared.ids import uuid7
from lib.shared.trust import TrustTier, min_tier

EXT_CHANNEL_PREFIX = "ext"
_DEFAULT_TIER = TrustTier.inferential_external
# Never reachable by a non-first-party extension, regardless of grant ceiling.
_UNREACHABLE = frozenset({TrustTier.authoritative, TrustTier.authoritative_external})
_SUBCHANNEL_RE = re.compile(r"^[a-z0-9][a-z0-9_.\-]{0,63}$")


class EdgeIngestError(Exception):
    """Carries an OAuth-style error code + HTTP status for the endpoint."""

    def __init__(self, code: str, status: int = 400) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


def resolve_trust_tier(requested: str | None, ceiling: str) -> TrustTier:
    """The effective tier for an edge write, or raise EdgeIngestError.

    No request → the default, capped to the ceiling (never above it). An explicit
    request above the ceiling or for an unreachable tier is rejected."""
    ceiling_t = TrustTier(ceiling) if isinstance(ceiling, str) else ceiling
    if requested is None:
        return min_tier(_DEFAULT_TIER, ceiling_t)  # least-trustworthy of the two
    try:
        req_t = TrustTier(requested)
    except ValueError as exc:
        raise EdgeIngestError("invalid_trust_tier", 400) from exc
    if req_t in _UNREACHABLE:
        raise EdgeIngestError("trust_tier_unreachable", 403)
    if req_t.rank < ceiling_t.rank:  # lower rank == MORE trustworthy than allowed
        raise EdgeIngestError("trust_tier_over_ceiling", 403)
    return req_t


def namespaced_channel(extension_id: str, sub_channel: str) -> str:
    """``ext:<extension_id>:<sub>`` after validating the sub-channel shape."""
    sub = (sub_channel or "").strip()
    if not _SUBCHANNEL_RE.match(sub):
        raise EdgeIngestError("invalid_channel", 400)
    return f"{EXT_CHANNEL_PREFIX}:{extension_id}:{sub}"


async def edge_ingest(
    pool: Any,
    *,
    extension_id: str,
    tenant_id: UUID,
    trust_ceiling: str,
    can_write: bool,
    sub_channel: str,
    content: dict[str, Any],
    content_text: str,
    external_id: str | None = None,
    requested_trust_tier: str | None = None,
    occurred_at: str | None = None,
    deps: Any = None,
) -> dict[str, Any]:
    """Validate + persist one edge observation. Returns the ack dict."""
    if not can_write:
        raise EdgeIngestError("write_observations_not_granted", 403)
    if not isinstance(content, dict) or not isinstance(content_text, str):
        raise EdgeIngestError("invalid_body", 400)

    tier = resolve_trust_tier(requested_trust_tier, trust_ceiling)
    channel = namespaced_channel(extension_id, sub_channel)

    when = _parse_ts(occurred_at)
    from services.ingest.ingestion.core import ingest_from_draft
    from services.ingest.ingestion.handlers import ObservationDraft

    draft = ObservationDraft(
        source_channel=channel,
        content_text=content_text,
        content=dict(content),
        occurred_at=when,
        trust_tier=tier.value,
        kind="signal",
        source_actor_ref=f"extension:{extension_id}",
        external_id=external_id or f"{extension_id}:{uuid7()}",
    )
    result = await ingest_from_draft(
        channel=channel, draft=draft, pool=pool, tenant_id=tenant_id,
        actor_repo=getattr(deps, "actor_repo", None),
        alias_repo=getattr(deps, "alias_repo", None),
        embedder=getattr(deps, "embedder", None),
    )
    obs = result.observation
    return {
        "accepted": True,
        "id": str(getattr(obs, "id", None) or obs["id"]),
        "trust_tier": tier.value,
        "source_channel": channel,
        "external_id": draft.external_id,
        "deduped": bool(result.deduped),
    }


def _parse_ts(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(raw)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise EdgeIngestError("invalid_occurred_at", 400) from exc


__all__ = ["edge_ingest", "resolve_trust_tier", "namespaced_channel", "EdgeIngestError"]
