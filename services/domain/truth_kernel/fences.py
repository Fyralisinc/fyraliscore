"""Synchronous dependent-truth fencing for Model lifecycle transitions."""

from __future__ import annotations

from typing import Any

from .repository import AsyncpgTruthKernelStorage
from .service import FenceContext, TruthKernelService


class AsyncpgDependentTruthFence:
    """Create durable repair obligations before an invalidating head commits.

    Accepted-current views perform the immediate read fence. These obligations
    preserve the exact affected version/projection set for subsequent repair;
    their unique constraint makes command replay epistemically idempotent.
    """

    name = "dependent_truth_repair_obligations"

    async def apply(self, *, tx: Any, context: FenceContext) -> None:
        if not context.next_version.lifecycle.terminal:
            return
        await tx.execute(
            _DEPENDENT_MODEL_OBLIGATIONS,
            context.tenant_id,
            context.prior_head.version_id,
            context.next_version.created_at,
        )
        await tx.execute(
            _RELATION_EVIDENCE_OBLIGATIONS,
            context.tenant_id,
            context.prior_head.version_id,
            context.next_version.created_at,
        )
        await tx.execute(
            _PROJECTION_OBLIGATIONS,
            context.tenant_id,
            context.prior_head.version_id,
            context.next_version.created_at,
        )


def build_default_truth_kernel() -> TruthKernelService:
    """Construct the production Model truth authority with mandatory fencing."""

    return TruthKernelService(
        storage=AsyncpgTruthKernelStorage(),
        fences=(AsyncpgDependentTruthFence(),),
    )


_DEPENDENT_MODEL_OBLIGATIONS = """
INSERT INTO truth_repair_obligations (
  obligation_id, tenant_id, invalidated_model_version_id, affected_kind,
  affected_id, cause_code, status, created_at
)
SELECT gen_random_uuid(), evidence.tenant_id, $2::uuid, 'model_version',
       evidence.model_version_id, 'support_version_invalidated', 'pending', $3
FROM model_truth_evidence_references evidence
JOIN model_truth_heads dependent_head
  ON dependent_head.tenant_id = evidence.tenant_id
 AND dependent_head.version_id = evidence.model_version_id
 AND dependent_head.lifecycle = 'active'
WHERE evidence.tenant_id = $1
  AND evidence.evidence_kind = 'model_version'
  AND evidence.evidence_id = $2::text
ON CONFLICT (
  tenant_id, invalidated_model_version_id, affected_kind, affected_id, cause_code
) DO NOTHING
"""


_RELATION_EVIDENCE_OBLIGATIONS = """
INSERT INTO truth_repair_obligations (
  obligation_id, tenant_id, invalidated_model_version_id, affected_kind,
  affected_id, cause_code, status, created_at
)
SELECT gen_random_uuid(), evidence.tenant_id, $2::uuid, 'relation_version',
       evidence.relation_version_id, 'relation_evidence_invalidated', 'pending', $3
FROM relation_truth_evidence evidence
JOIN relation_truth_heads relation_head
  ON relation_head.tenant_id = evidence.tenant_id
 AND relation_head.relation_version_id = evidence.relation_version_id
 AND relation_head.lifecycle = 'active'
WHERE evidence.tenant_id = $1
  AND evidence.model_version_id = $2::uuid
ON CONFLICT (
  tenant_id, invalidated_model_version_id, affected_kind, affected_id, cause_code
) DO NOTHING
"""


_PROJECTION_OBLIGATIONS = """
INSERT INTO truth_repair_obligations (
  obligation_id, tenant_id, invalidated_model_version_id, affected_kind,
  affected_id, cause_code, status, created_at
)
SELECT gen_random_uuid(), evidence.tenant_id, $2::uuid, 'projection', projection.id,
       'relation_projection_invalidated', 'pending', $3
FROM relation_truth_evidence evidence
JOIN relation_truth_versions relation_version
  ON relation_version.tenant_id = evidence.tenant_id
 AND relation_version.relation_version_id = evidence.relation_version_id
JOIN relation_edge_projections projection
  ON projection.tenant_id = relation_version.tenant_id
 AND projection.relation_id = relation_version.relation_id
 AND projection.status = 'active'
WHERE evidence.tenant_id = $1
  AND evidence.model_version_id = $2::uuid
ON CONFLICT (
  tenant_id, invalidated_model_version_id, affected_kind, affected_id, cause_code
) DO NOTHING
"""


__all__ = ["AsyncpgDependentTruthFence", "build_default_truth_kernel"]
