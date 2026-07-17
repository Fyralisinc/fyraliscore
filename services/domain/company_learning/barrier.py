"""Truth-critical batch barriers and exact context/outcome attribution.

The service owns no background work.  A completed receipt means the caller's
accepted truth is visible, stale versions are excluded, and the caller has no
truth-critical pending work.  Optional projections and policy learning may
continue against the declared barrier version.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import json
from typing import Any, Literal
from uuid import UUID

from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import InvariantViolation


ContextItemKind = Literal[
    "current_episode", "historical_observation", "accepted_model",
    "accepted_relation", "residual", "candidate",
]
DecisionFate = Literal["mutation", "justified_noop", "validator_drop", "unused"]
OutcomeKind = Literal[
    "confirmation", "revision", "falsification", "correction",
    "human_adjudication",
]


class HistoricalReopenReason(StrEnum):
    COLD_START = "cold_start"
    SPARSE_COVERAGE = "sparse_coverage"
    CONTRADICTION = "contradiction"
    PROVENANCE = "provenance"
    NOVELTY = "novelty"
    CORRECTION = "correction"
    UNRESOLVED_QUESTION = "unresolved_question"


@dataclass(frozen=True, slots=True)
class ContextDecision:
    decision_id: UUID
    tenant_id: UUID
    batch_id: str
    route_id: str
    context_item_kind: ContextItemKind
    context_item_id: str
    context_item_version: str
    retrieved: bool
    selected: bool
    included: bool
    referenced: bool
    counterevidence_retained: bool
    confidence_affecting: bool
    necessary_background: bool
    historical_reopen_reason: HistoricalReopenReason | None
    decision_fate: DecisionFate
    result_object_kind: str | None
    result_object_id: UUID | None
    evidence_lineage: tuple[dict[str, Any], ...]
    decided_at: datetime

    def validate(self) -> None:
        if self.referenced and not self.included:
            raise ValueError("referenced context must be included")
        if self.included and not self.selected:
            raise ValueError("included context must be selected")
        if self.selected and not self.retrieved:
            raise ValueError("selected context must be retrieved")
        if (
            self.context_item_kind == "historical_observation"
            and self.selected
            and self.historical_reopen_reason is None
        ):
            raise ValueError("selected historical observations require a reopen reason")


@dataclass(frozen=True, slots=True)
class OutcomeLink:
    outcome_link_id: UUID
    tenant_id: UUID
    decision_id: UUID
    outcome_kind: OutcomeKind
    outcome_object_kind: str
    outcome_object_id: UUID
    attribution_basis: Literal["direct", "associative"]
    evidence_lineage: tuple[dict[str, Any], ...]
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class BarrierReceipt:
    barrier_id: UUID
    tenant_id: UUID
    batch_id: str
    barrier_version: int
    prior_barrier_id: UUID | None
    expected_model_version_ids: tuple[UUID, ...]
    expected_relation_version_ids: tuple[UUID, ...]
    invalidated_model_version_ids: tuple[UUID, ...]
    truth_critical_pending_count: int
    completed_at: datetime
    receipt_digest: str


class CompanyLearningBarrierService:
    """Compile context credit and a versioned visibility barrier in one DB tx."""

    async def record_context_decision(self, *, tx: Any, item: ContextDecision) -> None:
        item.validate()
        await tx.execute(
            """
            INSERT INTO company_learning_context_decisions (
              decision_id, tenant_id, batch_id, route_id, context_item_kind,
              context_item_id, context_item_version, retrieved, selected,
              included, referenced, counterevidence_retained,
              confidence_affecting, necessary_background,
              historical_reopen_reason, decision_fate, result_object_kind,
              result_object_id, evidence_lineage, decided_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb,$20)
            ON CONFLICT (decision_id) DO NOTHING
            """,
            item.decision_id, item.tenant_id, item.batch_id, item.route_id,
            item.context_item_kind, item.context_item_id,
            item.context_item_version, item.retrieved, item.selected,
            item.included, item.referenced, item.counterevidence_retained,
            item.confidence_affecting, item.necessary_background,
            item.historical_reopen_reason.value if item.historical_reopen_reason else None,
            item.decision_fate, item.result_object_kind, item.result_object_id,
            json.dumps(item.evidence_lineage), item.decided_at,
        )

    async def record_outcome(self, *, tx: Any, item: OutcomeLink) -> None:
        owner = await tx.fetchval(
            "SELECT tenant_id FROM company_learning_context_decisions WHERE decision_id=$1",
            item.decision_id,
        )
        if owner != item.tenant_id:
            raise InvariantViolation(
                "OUTCOME_DECISION_TENANT",
                "outcome must bind a decision in the same tenant",
            )
        await tx.execute(
            """
            INSERT INTO company_learning_outcome_links (
              outcome_link_id, tenant_id, decision_id, outcome_kind,
              outcome_object_kind, outcome_object_id, attribution_basis,
              evidence_lineage, observed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
            ON CONFLICT (tenant_id, decision_id, outcome_kind, outcome_object_id)
            DO NOTHING
            """,
            item.outcome_link_id, item.tenant_id, item.decision_id,
            item.outcome_kind, item.outcome_object_kind, item.outcome_object_id,
            item.attribution_basis, json.dumps(item.evidence_lineage),
            item.observed_at,
        )

    async def complete(
        self,
        *,
        tx: Any,
        barrier_id: UUID,
        tenant_id: UUID,
        batch_id: str,
        expected_model_version_ids: tuple[UUID, ...] = (),
        expected_relation_version_ids: tuple[UUID, ...] = (),
        invalidated_model_version_ids: tuple[UUID, ...] = (),
        truth_critical_pending_count: int,
        completed_at: datetime,
    ) -> BarrierReceipt:
        if truth_critical_pending_count != 0:
            raise InvariantViolation(
                "BARRIER_PENDING_TRUTH_WORK",
                "cannot complete while truth-critical work remains",
                pending=truth_critical_pending_count,
            )
        completed_at = completed_at.astimezone(timezone.utc)
        if not expected_relation_version_ids and not invalidated_model_version_ids:
            return await self._complete_common_atomic(
                tx=tx, barrier_id=barrier_id, tenant_id=tenant_id,
                batch_id=batch_id, model_versions=expected_model_version_ids,
                completed_at=completed_at,
            )
        else:
            head = await self._lock_head(tx=tx, tenant_id=tenant_id)
            replay = await self._find(tx=tx, tenant_id=tenant_id, batch_id=batch_id)
            if replay is not None:
                return replay
            await self._assert_visibility(
                tx=tx,
                tenant_id=tenant_id,
                model_versions=expected_model_version_ids,
                relation_versions=expected_relation_version_ids,
                invalidated_versions=invalidated_model_version_ids,
            )
            prior = head
        version = int(prior["barrier_version"]) + 1 if prior else 1
        prior_id = prior["barrier_id"] if prior else None
        payload = {
            "barrier_id": str(barrier_id), "tenant_id": str(tenant_id),
            "batch_id": batch_id, "barrier_version": version,
            "prior_barrier_id": str(prior_id) if prior_id else None,
            "expected_model_version_ids": sorted(map(str, expected_model_version_ids)),
            "expected_relation_version_ids": sorted(map(str, expected_relation_version_ids)),
            "invalidated_model_version_ids": sorted(map(str, invalidated_model_version_ids)),
            "truth_critical_pending_count": 0,
            "completed_at": completed_at.isoformat(),
        }
        digest = canonical_sha256(payload)
        await tx.execute(
            """
            INSERT INTO company_learning_barriers (
              barrier_id, tenant_id, batch_id, barrier_version,
              prior_barrier_id, expected_model_version_ids,
              expected_relation_version_ids, invalidated_model_version_ids,
              truth_critical_pending_count, status, receipt_digest, completed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,0,'complete',$9,$10)
            """,
            barrier_id, tenant_id, batch_id, version, prior_id,
            list(expected_model_version_ids), list(expected_relation_version_ids),
            list(invalidated_model_version_ids), digest, completed_at,
        )
        await tx.execute(
            """UPDATE company_learning_barrier_heads
               SET barrier_id=$2,barrier_version=$3 WHERE tenant_id=$1""",
            tenant_id, barrier_id, version,
        )
        return BarrierReceipt(
            barrier_id, tenant_id, batch_id, version, prior_id,
            expected_model_version_ids, expected_relation_version_ids,
            invalidated_model_version_ids, 0, completed_at, digest,
        )

    @staticmethod
    async def _complete_common_atomic(
        *, tx: Any, barrier_id: UUID, tenant_id: UUID, batch_id: str,
        model_versions: tuple[UUID, ...], completed_at: datetime,
    ) -> BarrierReceipt:
        row = await tx.fetchrow(
            """SELECT * FROM complete_company_learning_barrier_common(
                 $1,$2,$3,$4,$5
               )""",
            tenant_id, batch_id, barrier_id, list(model_versions),
            completed_at,
        )
        if row is None:
            raise InvariantViolation(
                "BARRIER_MODEL_VISIBILITY", "expected Models are not current",
            )
        receipt = BarrierReceipt(
            row["barrier_id"], row["tenant_id"], row["batch_id"],
            int(row["barrier_version"]), row["prior_barrier_id"],
            tuple(row["expected_model_version_ids"]),
            tuple(row["expected_relation_version_ids"]),
            tuple(row["invalidated_model_version_ids"]),
            int(row["truth_critical_pending_count"]), row["completed_at"],
            row["receipt_digest"],
        )
        payload = {
            "barrier_id": str(receipt.barrier_id), "tenant_id": str(receipt.tenant_id),
            "batch_id": receipt.batch_id, "barrier_version": receipt.barrier_version,
            "prior_barrier_id": str(receipt.prior_barrier_id) if receipt.prior_barrier_id else None,
            "expected_model_version_ids": sorted(map(str, receipt.expected_model_version_ids)),
            "expected_relation_version_ids": [], "invalidated_model_version_ids": [],
            "truth_critical_pending_count": 0,
            "completed_at": receipt.completed_at.isoformat(),
        }
        expected_digest = canonical_sha256(payload)
        if receipt.receipt_digest != expected_digest:
            raise InvariantViolation(
                "BARRIER_RECEIPT_DIGEST", "atomic receipt digest mismatch",
                actual=receipt.receipt_digest, expected=expected_digest,
            )
        return receipt

    @staticmethod
    async def _lock_head(*, tx: Any, tenant_id: UUID) -> Any:
        await tx.execute(
            """INSERT INTO company_learning_barrier_heads(tenant_id)
               VALUES($1) ON CONFLICT (tenant_id) DO NOTHING""",
            tenant_id,
        )
        return await tx.fetchrow(
            """SELECT barrier_id,barrier_version
               FROM company_learning_barrier_heads
               WHERE tenant_id=$1 FOR UPDATE""",
            tenant_id,
        )

    async def _assert_visibility(
        self, *, tx: Any, tenant_id: UUID,
        model_versions: tuple[UUID, ...], relation_versions: tuple[UUID, ...],
        invalidated_versions: tuple[UUID, ...],
    ) -> None:
        visible_models = await tx.fetch(
            "SELECT truth_version_id FROM accepted_current_models WHERE tenant_id=$1 AND truth_version_id=ANY($2::uuid[])",
            tenant_id, list(model_versions),
        )
        visible_model_ids = {row["truth_version_id"] for row in visible_models}
        if visible_model_ids != set(model_versions):
            raise InvariantViolation("BARRIER_MODEL_VISIBILITY", "expected Models are not current")
        if relation_versions:
            visible_relations = await tx.fetch(
                "SELECT truth_relation_version_id FROM accepted_current_relations WHERE tenant_id=$1 AND truth_relation_version_id=ANY($2::uuid[])",
                tenant_id, list(relation_versions),
            )
            if {row["truth_relation_version_id"] for row in visible_relations} != set(relation_versions):
                raise InvariantViolation("BARRIER_RELATION_VISIBILITY", "expected relations are not current")
        if invalidated_versions:
            stale = await tx.fetchval(
                "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 AND truth_version_id=ANY($2::uuid[])",
                tenant_id, list(invalidated_versions),
            )
            if stale:
                raise InvariantViolation("BARRIER_STALE_VISIBILITY", "invalidated Model remains current")

    @staticmethod
    async def _assert_models_and_select_prior(
        *, tx: Any, tenant_id: UUID, model_versions: tuple[UUID, ...],
    ) -> Any:
        """Validate common-case model visibility and select prior in one call."""
        row = await tx.fetchrow(
            """WITH visible AS (
                 SELECT array_agg(truth_version_id)::uuid[] AS ids
                 FROM accepted_current_models
                 WHERE tenant_id=$1 AND truth_version_id=ANY($2::uuid[])
               ), prior AS (
                 SELECT barrier_id, barrier_version
                 FROM company_learning_barriers
                 WHERE tenant_id=$1 ORDER BY barrier_version DESC
                 LIMIT 1 FOR UPDATE
               )
               SELECT COALESCE(visible.ids, '{}'::uuid[]) AS visible_ids,
                      prior.barrier_id, prior.barrier_version
               FROM visible LEFT JOIN prior ON true""",
            tenant_id, list(model_versions),
        )
        if set(row["visible_ids"]) != set(model_versions):
            raise InvariantViolation(
                "BARRIER_MODEL_VISIBILITY", "expected Models are not current",
            )
        if row["barrier_id"] is None:
            return None
        return row


    @staticmethod
    async def _find(*, tx: Any, tenant_id: UUID, batch_id: str) -> BarrierReceipt | None:
        row = await tx.fetchrow(
            "SELECT * FROM company_learning_barriers WHERE tenant_id=$1 AND batch_id=$2",
            tenant_id, batch_id,
        )
        if row is None:
            return None
        return BarrierReceipt(
            row["barrier_id"], row["tenant_id"], row["batch_id"],
            int(row["barrier_version"]), row["prior_barrier_id"],
            tuple(row["expected_model_version_ids"]),
            tuple(row["expected_relation_version_ids"]),
            tuple(row["invalidated_model_version_ids"]),
            int(row["truth_critical_pending_count"]), row["completed_at"],
            row["receipt_digest"],
        )
