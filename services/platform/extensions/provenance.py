"""services/platform/extensions/provenance.py — model source provenance (E2.8 foundation).

Records the set of source identities that materially drove each synthesized Model,
so a Model driven by a third-party extension can be surfaced as **contestable**
(INV-1 keeps third parties out of the synthesis write path; this makes their
*influence* — via trust-weighted edge observations — visible).

This is the foundation: a table + a recorder + the "is this third-party-driven?"
query. Source identity for an edge observation is derived from its host-namespaced
channel ``ext:<id>:...`` → ``extension:<id>`` (is_third_party=True). The deep wiring
that calls ``record`` from the synthesis scorer
(``services/reasoning/retrieval/scoring.py``) is the reasoning-team integration
point — this module is what it will call.
"""
from __future__ import annotations

from typing import Any, Iterable
from uuid import UUID

EXT_CHANNEL_PREFIX = "ext:"


def source_identity_for_channel(channel: str) -> tuple[str, bool]:
    """Map a source channel to a (provenance identity, is_third_party) pair.

    ``ext:<id>:...`` → ``("extension:<id>", True)``; anything else is a
    first-party/core channel → ``("channel:<c>", False)``."""
    if channel.startswith(EXT_CHANNEL_PREFIX):
        ext_id = channel[len(EXT_CHANNEL_PREFIX):].split(":", 1)[0]
        return f"extension:{ext_id}", True
    return f"channel:{channel}", False


class ModelProvenanceRepo:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def record(
        self, *, model_id: UUID, tenant_id: UUID | None, source_channels: Iterable[str],
        weights: dict[str, float] | None = None,
    ) -> None:
        """Record the distinct source identities behind a model (idempotent)."""
        seen: dict[str, tuple[str, bool, float]] = {}
        for ch in source_channels:
            ident, third = source_identity_for_channel(ch)
            seen[ident] = (ident, third, (weights or {}).get(ch, 1.0))
        if not seen:
            return
        rows = [(model_id, tenant_id, ident, third, w) for ident, third, w in seen.values()]
        async with self.pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO model_provenance "
                "(model_id, tenant_id, source_identity, is_third_party, weight) "
                "VALUES ($1,$2,$3,$4,$5) ON CONFLICT (model_id, source_identity) DO UPDATE "
                "SET weight=EXCLUDED.weight, is_third_party=EXCLUDED.is_third_party",
                rows,
            )

    async def is_third_party_driven(self, model_id: UUID) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM model_provenance "
                "WHERE model_id=$1 AND is_third_party)",
                model_id,
            )

    async def sources_for(self, model_id: UUID) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT source_identity, is_third_party, weight FROM model_provenance "
                "WHERE model_id=$1 ORDER BY weight DESC, source_identity",
                model_id,
            )
        return [dict(r) for r in rows]


__all__ = ["ModelProvenanceRepo", "source_identity_for_channel"]
