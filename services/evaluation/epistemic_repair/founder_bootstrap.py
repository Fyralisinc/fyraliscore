"""Gold-blind founder identity bootstrap seam for production-shaped replays.

The replay caller supplies a public identity manifest explicitly.  This module
only adapts that manifest to the pre-enqueue P6 batch hook; it neither derives
entries from a sealed population nor seeds behavioral Models.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.company_identity_bootstrap import (
    FounderIdentityBootstrapEntry,
    FounderIdentityBootstrapResult,
    apply_founder_identity_bootstrap,
)


_SEMANTIC_COUNT_QUERY = """
SELECT
  (SELECT count(*) FROM models WHERE tenant_id=$1) AS models,
  (
    SELECT count(*)
    FROM relation_instances
    WHERE tenant_id=$1 AND status IN ('active', 'accepted')
  ) AS accepted_relation_instances,
  (
    SELECT count(*)
    FROM model_edges
    WHERE tenant_id=$1 AND status='active'
  ) AS active_model_edges,
  (SELECT count(*) FROM observations WHERE tenant_id=$1) AS observations,
  (SELECT count(*) FROM resources WHERE tenant_id=$1) AS resources
"""

_SEMANTIC_COUNT_KEYS = (
    "models",
    "accepted_relation_instances",
    "active_model_edges",
    "observations",
    "resources",
)


async def _semantic_counts(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> dict[str, int]:
    """Read the tenant-scoped truth surfaces the identity hook must not alter."""

    row = await conn.fetchrow(_SEMANTIC_COUNT_QUERY, tenant_id)
    return {key: int(row[key]) for key in _SEMANTIC_COUNT_KEYS}


@dataclass(slots=True)
class FounderBootstrapBatchPreparer:
    """Apply one explicit founder manifest before the first batch is enqueued.

    ``P6ThinkExecutionDependencies.prepare_persisted_batch`` calls its hook once
    per batch.  The lock and cached result make one preparer safe for a
    multi-batch execution while the domain service supplies durable idempotency
    across process retries.
    """

    manifest_ref: str
    authority_ref: str
    asserted_by_ref: str
    provenance_refs: tuple[str, ...]
    entries: tuple[FounderIdentityBootstrapEntry, ...]
    effective_at: datetime
    _tenant_id: UUID | None = field(default=None, init=False, repr=False)
    _result: FounderIdentityBootstrapResult | None = field(
        default=None, init=False, repr=False,
    )
    _receipt: dict[str, Any] | None = field(default=None, init=False, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    @property
    def result(self) -> FounderIdentityBootstrapResult | None:
        """Return bootstrap evidence after the first successful preparation."""

        return self._result

    @property
    def receipt(self) -> dict[str, Any] | None:
        """Return JSONable proof that bootstrap changed identity, not truth.

        A defensive copy prevents report serialization or caller decoration from
        mutating the cached evidence used by later batches.
        """

        return deepcopy(self._receipt)

    async def __call__(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        _batch: Any,
        _observation_ids: dict[str, UUID],
    ) -> None:
        """Materialize identity aliases once, before Think enqueue authority."""

        async with self._lock:
            if self._tenant_id is not None and self._tenant_id != tenant_id:
                raise ValueError(
                    "a founder bootstrap preparer cannot be shared across tenants"
                )
            if self._result is not None:
                return
            counts_before = await _semantic_counts(conn, tenant_id=tenant_id)
            result = await apply_founder_identity_bootstrap(
                conn,
                tenant_id=tenant_id,
                manifest_ref=self.manifest_ref,
                authority_ref=self.authority_ref,
                asserted_by_ref=self.asserted_by_ref,
                provenance_refs=self.provenance_refs,
                entries=self.entries,
                effective_at=self.effective_at,
            )
            counts_after = await _semantic_counts(conn, tenant_id=tenant_id)
            semantic_deltas = {
                key: counts_after[key] - counts_before[key]
                for key in _SEMANTIC_COUNT_KEYS
            }
            self._tenant_id = tenant_id
            self._result = result
            self._receipt = {
                "manifest_ref": result.manifest_ref,
                "alias_count": result.alias_count,
                "applied_before_enqueue": True,
                "semantic_truth_unchanged": all(
                    delta == 0 for delta in semantic_deltas.values()
                ),
                "counts_before": counts_before,
                "counts_after": counts_after,
                "semantic_deltas": semantic_deltas,
            }


def build_founder_bootstrap_batch_preparer(
    *,
    manifest_ref: str,
    authority_ref: str,
    asserted_by_ref: str,
    provenance_refs: tuple[str, ...],
    entries: tuple[FounderIdentityBootstrapEntry, ...],
    effective_at: datetime,
) -> FounderBootstrapBatchPreparer:
    """Build a reusable P6-compatible hook from caller-owned public entries."""

    return FounderBootstrapBatchPreparer(
        manifest_ref=manifest_ref,
        authority_ref=authority_ref,
        asserted_by_ref=asserted_by_ref,
        provenance_refs=provenance_refs,
        entries=entries,
        effective_at=effective_at,
    )


__all__ = [
    "FounderBootstrapBatchPreparer",
    "build_founder_bootstrap_batch_preparer",
]
