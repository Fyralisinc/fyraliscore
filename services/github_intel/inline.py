"""services/github_intel/inline.py — inline enrichment hook (raw-on-failure).

Called from `services.ingestion.core.ingest_from_draft` for every
`github:webhook` draft. It augments `draft.content["intelligence"]` in place so
the SAME observation row carries the reasoning (the layer's primary output).

Hard guarantees:
  - Bounded by GITHUB_INTEL_INLINE_TIMEOUT_MS.
  - Flag-gated per tenant (github_intel.enabled, default off).
  - Read-only (never writes state on the parallel normalize stage).
  - ANY exception/timeout/disabled => draft returned unchanged => the RAW
    GitHub signal is what gets ingested.
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

from services.github_intel.config import (
    GITHUB_INTEL_ENABLED, GITHUB_INTEL_LLM_ENABLED, INLINE_TIMEOUT_MS, MAX_BLAST_HOPS,
)


async def maybe_enrich_github_draft(draft: Any, *, pool: Any, tenant_id: UUID) -> None:
    """Best-effort inline enrichment. Mutates draft.content in place; never raises."""
    try:
        from services.ingestion.feature_flags.client import TenantFlags
        flags = TenantFlags(pool)
        if not await flags.get_bool(tenant_id, GITHUB_INTEL_ENABLED, default=False):
            return
        llm_enabled = await flags.get_bool(tenant_id, GITHUB_INTEL_LLM_ENABLED, default=False)
        await asyncio.wait_for(
            _enrich(draft, pool=pool, tenant_id=tenant_id, llm_enabled=llm_enabled),
            timeout=INLINE_TIMEOUT_MS / 1000.0,
        )
    except Exception:  # noqa: BLE001 — raw-on-failure: enrichment must never break ingest
        return


async def _enrich(draft: Any, *, pool: Any, tenant_id: UUID, llm_enabled: bool) -> None:
    from lib.shared.tenant_context import tenant_transaction
    from services.github_intel.enrichment import build_inline_intelligence

    async with tenant_transaction(tenant_id, pool=pool) as ctx:
        intel = await build_inline_intelligence(
            ctx,
            tenant_id=tenant_id,
            content=draft.content,
            raw_payload=draft.raw_payload,
            occurred_at=draft.occurred_at,
            llm_enabled=llm_enabled,
            max_hops=MAX_BLAST_HOPS,
        )
    # Attach into the SAME content body and enrich the embedding/think seed.
    draft.content["intelligence"] = intel
    effect = intel.get("effect")
    if effect:
        draft.content_text = f"{draft.content_text} — [intel] {effect}"
