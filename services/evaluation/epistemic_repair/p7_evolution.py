"""Production-backed P7 arm evolution and canonical lifecycle bridging.

The evaluator does not decide that a Model is wrong.  It may only forward a
successful production Think run's already-validated lifecycle operation to the
canonical truth kernel, with exact same-tenant observations as counterevidence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid5

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from lib.contracts.kernel import canonical_sha256
from lib.contracts.truth_admission import (
    AdvanceModelHeadCommand,
    ModelHeadExpectation,
    ModelTruthTransition,
    ModelVersion,
)
from lib.contracts.truth_evidence import (
    EvidenceAuthority,
    TruthEvidenceCoordinate,
    TruthEvidenceKind,
    TruthEvidenceReference,
    TruthEvidenceRole,
)
from services.evaluation.epistemic_repair.p5_runner import _load_model_version
from lib.shared.errors import InvariantViolation
from services.domain.truth_kernel import build_default_truth_kernel
from services.domain.truth_kernel.relations.repository import (
    AsyncpgRelationKernelStorage,
)
from services.domain.truth_kernel.relations.service import RelationTruthKernel


P7EvolutionArm = Literal[
    "adaptive", "frozen", "observation_only", "memory_hidden", "corrupted"
]

_MUTABLE_ARMS = frozenset({"adaptive", "memory_hidden", "corrupted"})
_TERMINAL_ACTIONS: dict[str, ModelTruthTransition] = {
    "falsify": ModelTruthTransition.FALSIFY,
    "archive": ModelTruthTransition.ARCHIVE,
    "supersede": ModelTruthTransition.SUPERSEDE,
}


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class P7LifecycleBridgeReceipt(_Frozen):
    tenant_id: str
    arm_id: P7EvolutionArm
    batch_number: int = Field(ge=1, le=12)
    think_run_id: str
    model_id: str
    prior_version_id: str
    next_version_id: str
    action: Literal["falsify", "archive", "supersede"]
    contradictory_observation_ids: tuple[str, ...] = Field(min_length=1)
    resulting_lifecycle: str
    within_two_batch_recovery_bound: bool | None
    command_receipt_digest: str


def arm_allows_reasoning(arm: P7EvolutionArm, batch_number: int) -> bool:
    if arm == "frozen":
        return batch_number <= 3
    return True


def arm_allows_canonical_mutation(arm: P7EvolutionArm, batch_number: int) -> bool:
    if arm not in _MUTABLE_ARMS:
        return batch_number <= 3 and arm == "frozen"
    return True


def arm_memory_visible(arm: P7EvolutionArm) -> bool:
    return arm in {"adaptive", "frozen", "corrupted"}


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        import json

        value = json.loads(value)
    return value if isinstance(value, dict) else {}


def _lifecycle_summaries(ops_applied: Any) -> tuple[dict[str, Any], ...]:
    raw = _as_dict(ops_applied).get("memory_lifecycle_ops") or ()
    return tuple(item for item in raw if isinstance(item, dict))


async def _counterevidence(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    think_run_id: UUID,
    observation_ids: tuple[UUID, ...],
    decided_at: datetime,
) -> tuple[TruthEvidenceReference, ...]:
    rows = await conn.fetch(
        "SELECT id,occurred_at,source_channel,content_text FROM observations "
        "WHERE tenant_id=$1 AND id=ANY($2::uuid[]) ORDER BY occurred_at,id",
        tenant_id,
        list(observation_ids),
    )
    if len(rows) != len(set(observation_ids)):
        raise InvariantViolation(
            "P7_LIFECYCLE_COUNTEREVIDENCE_MISSING",
            "every lifecycle transition requires exact same-tenant observations",
            expected=len(set(observation_ids)),
            found=len(rows),
        )
    result = []
    for row in rows:
        text = str(row["content_text"] or "")
        if not text.strip():
            raise InvariantViolation(
                "P7_LIFECYCLE_COUNTEREVIDENCE_EMPTY",
                "empty observations cannot govern a lifecycle transition",
                observation_id=str(row["id"]),
            )
        result.append(TruthEvidenceReference(
            reference_id=uuid5(
                model_id,
                f"p7-think-counterevidence:{think_run_id}:{row['id']}",
            ),
            tenant_id=tenant_id,
            kind=TruthEvidenceKind.OBSERVATION,
            evidence_id=str(row["id"]),
            evidence_version=1,
            evidence_digest=canonical_sha256(text),
            role=TruthEvidenceRole.COUNTEREVIDENCE,
            coordinate=TruthEvidenceCoordinate(
                source_system=str(row["source_channel"] or "normalized-signal"),
                source_object_id=str(row["id"]),
                source_revision="1",
                field_path="content_text",
                span_start=0,
                span_end=len(text),
            ),
            authority=EvidenceAuthority(
                authority_ref=f"validated-think-lifecycle:{think_run_id}",
                policy_version="p7-production-lifecycle-bridge-v1",
                authority_epoch=1,
                decided_at=decided_at,
            ),
            occurred_at=row["occurred_at"],
            recorded_at=decided_at,
            cutoff_at=decided_at,
        ))
    return tuple(result)


async def bridge_validated_think_lifecycle(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    arm: P7EvolutionArm,
    batch_number: int,
    think_run_id: UUID,
    corruption_model_ids: frozenset[UUID] = frozenset(),
    corruption_injected_batch: int = 4,
) -> tuple[P7LifecycleBridgeReceipt, ...]:
    """Advance canonical heads only from successful production Think output."""

    run = await conn.fetchrow(
        "SELECT status,ops_applied,ended_at FROM think_runs "
        "WHERE tenant_id=$1 AND id=$2",
        tenant_id,
        think_run_id,
    )
    if run is None or run["status"] != "success":
        raise InvariantViolation(
            "P7_LIFECYCLE_THINK_RUN_NOT_SUCCESSFUL",
            "canonical lifecycle bridging requires a successful durable Think run",
            think_run_id=str(think_run_id),
        )
    summaries = _lifecycle_summaries(run["ops_applied"])
    consequential = tuple(
        item for item in summaries if item.get("action") in _TERMINAL_ACTIONS
    )
    if consequential and not arm_allows_canonical_mutation(arm, batch_number):
        raise InvariantViolation(
            "P7_ARM_FORBIDDEN_CANONICAL_MUTATION",
            "frozen and observation-only arms cannot mutate canonical memory",
            arm=arm,
            batch_number=batch_number,
        )
    receipts: list[P7LifecycleBridgeReceipt] = []
    decided_at = run["ended_at"] or datetime.now(timezone.utc)
    for item in consequential:
        try:
            model_id = UUID(str(item["model_id"]))
            evidence_ids = tuple(
                dict.fromkeys(UUID(str(value)) for value in item.get("evidence_event_ids") or ())
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvariantViolation(
                "P7_LIFECYCLE_RECEIPT_MALFORMED",
                "validated lifecycle summary lacks typed target/evidence coordinates",
                think_run_id=str(think_run_id),
            ) from exc
        if not evidence_ids:
            raise InvariantViolation(
                "P7_LIFECYCLE_COUNTEREVIDENCE_MISSING",
                "terminal lifecycle decisions require explicit observation evidence",
                model_id=str(model_id),
            )
        head = await conn.fetchrow(
            "SELECT version_id,version,semantic_digest,lifecycle "
            "FROM model_truth_heads WHERE tenant_id=$1 AND model_id=$2",
            tenant_id,
            model_id,
        )
        if head is None:
            raise InvariantViolation(
                "P7_LIFECYCLE_TARGET_NOT_CANONICAL",
                "Think cannot transition a noncanonical or cross-tenant Model",
                model_id=str(model_id),
            )
        prior = await _load_model_version(
            conn, tenant_id=tenant_id, version_id=head["version_id"]
        )
        counterevidence = await _counterevidence(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            think_run_id=think_run_id,
            observation_ids=evidence_ids,
            decided_at=decided_at,
        )
        transition = _TERMINAL_ACTIONS[str(item["action"])]
        next_evidence = (*prior.evidence, *counterevidence)
        next_version = prior.model_copy(update={
            "version_id": uuid5(
                model_id,
                f"p7-lifecycle:{think_run_id}:{item['action']}:{prior.version + 1}",
            ),
            "version": prior.version + 1,
            "evidence": next_evidence,
            "lifecycle": transition.resulting_lifecycle,
            "created_at": decided_at,
            "semantic_digest": ModelVersion.compute_semantic_digest(
                proposition=prior.proposition,
                natural=prior.natural,
                evidence=next_evidence,
                scope=prior.scope,
            ),
        })
        command = AdvanceModelHeadCommand(
            command_id=uuid5(
                model_id, f"p7-lifecycle-command:{think_run_id}:{item['action']}"
            ),
            idempotency_key=(
                f"p7-production-lifecycle:{tenant_id}:{think_run_id}:"
                f"{model_id}:{item['action']}"
            ),
            tenant_id=tenant_id,
            expectation=ModelHeadExpectation(
                tenant_id=tenant_id,
                model_id=model_id,
                expected_version_id=prior.version_id,
                expected_version=prior.version,
                expected_semantic_digest=prior.semantic_digest,
                expected_lifecycle=prior.lifecycle,
            ),
            next_version=next_version,
            transition=transition,
            reason_codes=(
                "validated_production_think_lifecycle",
                "exact_observation_counterevidence",
            ),
            issued_at=decided_at,
        )
        truth_receipt = await build_default_truth_kernel().advance(
            tx=conn, command=command
        )
        await RelationTruthKernel(AsyncpgRelationKernelStorage()).invalidate_evidence(
            tx=conn,
            tenant_id=tenant_id,
            invalidated_model_version_id=prior.version_id,
            cause_code=f"MODEL_{transition.resulting_lifecycle.value.upper()}",
            occurred_at=decided_at,
        )
        within_bound = (
            batch_number <= corruption_injected_batch + 2
            if model_id in corruption_model_ids
            else None
        )
        receipts.append(P7LifecycleBridgeReceipt(
            tenant_id=str(tenant_id),
            arm_id=arm,
            batch_number=batch_number,
            think_run_id=str(think_run_id),
            model_id=str(model_id),
            prior_version_id=str(prior.version_id),
            next_version_id=str(truth_receipt.version_id),
            action=item["action"],
            contradictory_observation_ids=tuple(map(str, evidence_ids)),
            resulting_lifecycle=truth_receipt.lifecycle.value,
            within_two_batch_recovery_bound=within_bound,
            command_receipt_digest=truth_receipt.result_digest,
        ))
    return tuple(receipts)


__all__ = [
    "P7LifecycleBridgeReceipt",
    "arm_allows_canonical_mutation",
    "arm_allows_reasoning",
    "arm_memory_visible",
    "bridge_validated_think_lifecycle",
]
