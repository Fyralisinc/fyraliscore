"""Gold-blind founder identity bootstrap seam for production-shaped replays.

The replay caller supplies a public identity manifest explicitly.  This module
only adapts that manifest to the pre-enqueue P6 batch hook; it neither derives
entries from a sealed population nor seeds behavioral Models.
"""

from __future__ import annotations

import asyncio
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
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    @property
    def result(self) -> FounderIdentityBootstrapResult | None:
        """Return bootstrap evidence after the first successful preparation."""

        return self._result

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
            self._tenant_id = tenant_id
            self._result = result


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
