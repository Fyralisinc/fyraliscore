"""Non-canonical latent-gap hypotheses backed by measured residual debt."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.reasoning.sage.model_residuals import ModelResidualEvidence
from services.reasoning.sage.model_residuals import ModelResidualEvidenceRepo


LATENT_GAP_STATUSES = {"candidate", "confirmed", "rejected", "expired", "superseded"}
LATENT_GAP_RESOLUTION_OBJECT_KINDS = {
    "model",
    "model_signal_reading",
    "model_edge",
    "relation_claim",
    "relation_instance",
    "model_open_question",
    "projection_snapshot",
    "inquiry_outcome_event",
    "clarification_request",
    "human_review",
}

_COLS = (
    "id",
    "tenant_id",
    "gap_kind",
    "status",
    "residual_cluster_hash",
    "supporting_residual_ids",
    "supporting_observation_ids",
    "missing_evidence_statement",
    "falsifier",
    "next_evidence_needed",
    "confidence",
    "hypothesis_text",
    "metadata",
    "resolution_object_kind",
    "resolution_object_id",
    "resolution_reason",
    "created_at",
    "updated_at",
    "resolved_at",
)
_COLS_SQL = ", ".join(_COLS)


@dataclass(frozen=True)
class SageLatentGapHypothesis:
    tenant_id: UUID
    gap_kind: str
    residual_cluster_hash: str
    supporting_residual_ids: tuple[UUID, ...]
    missing_evidence_statement: str
    falsifier: str
    next_evidence_needed: str
    hypothesis_text: str
    confidence: float
    id: UUID | None = None
    status: str = "candidate"
    supporting_observation_ids: tuple[UUID, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    resolution_object_kind: str | None = None
    resolution_object_id: UUID | None = None
    resolution_reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    resolved_at: datetime | None = None


class SageLatentGapHypothesisRepo:
    """Tenant-bound repository for non-canonical latent-gap candidates."""

    def __init__(
        self,
        pool: asyncpg.Pool | None = None,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id

    async def insert_candidate(
        self,
        hypothesis: SageLatentGapHypothesis,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> SageLatentGapHypothesis:
        self._validate(hypothesis, require_candidate=True)
        return await self._with_conn(conn, lambda c: self._insert_candidate(c, hypothesis))

    async def list_candidates(
        self,
        *,
        limit: int = 100,
        conn: asyncpg.Connection | None = None,
    ) -> list[SageLatentGapHypothesis]:
        if limit < 1:
            raise ValueError("limit must be positive")
        return await self._with_conn(conn, lambda c: self._list_candidates(c, limit))

    async def resolve(
        self,
        hypothesis_id: UUID,
        *,
        status: str,
        reason: str,
        object_kind: str | None = None,
        object_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> SageLatentGapHypothesis | None:
        if status not in LATENT_GAP_STATUSES or status == "candidate":
            raise ValueError(f"invalid latent-gap terminal status: {status}")
        if object_kind is not None and object_kind not in LATENT_GAP_RESOLUTION_OBJECT_KINDS:
            raise ValueError(f"invalid resolution object kind: {object_kind}")
        return await self._with_conn(
            conn,
            lambda c: self._resolve_with_conn(
                c,
                hypothesis_id,
                status=status,
                reason=reason,
                object_kind=object_kind,
                object_id=object_id,
                metadata=metadata or {},
            ),
        )

    async def _insert_candidate(
        self,
        conn: asyncpg.Connection,
        hypothesis: SageLatentGapHypothesis,
    ) -> SageLatentGapHypothesis:
        row_id = hypothesis.id or uuid7()
        row = await conn.fetchrow(
            f"""
            INSERT INTO sage_latent_gap_hypotheses (
                id, tenant_id, gap_kind, status, residual_cluster_hash,
                supporting_residual_ids, supporting_observation_ids,
                missing_evidence_statement, falsifier, next_evidence_needed,
                confidence, hypothesis_text, metadata
            ) VALUES (
                $1, $2, $3, 'candidate', $4,
                $5::uuid[], $6::uuid[],
                $7, $8, $9,
                $10, $11, $12::jsonb
            )
            ON CONFLICT DO NOTHING
            RETURNING {_COLS_SQL}
            """,
            row_id,
            self._tenant_id,
            hypothesis.gap_kind,
            hypothesis.residual_cluster_hash,
            list(hypothesis.supporting_residual_ids),
            list(hypothesis.supporting_observation_ids),
            hypothesis.missing_evidence_statement,
            hypothesis.falsifier,
            hypothesis.next_evidence_needed,
            float(hypothesis.confidence),
            hypothesis.hypothesis_text,
            _jsonb(hypothesis.metadata),
        )
        if row is not None:
            return _hydrate(row)
        existing = await conn.fetchrow(
            f"""
            SELECT {_COLS_SQL}
            FROM sage_latent_gap_hypotheses
            WHERE tenant_id = $1
              AND residual_cluster_hash = $2
              AND gap_kind = $3
              AND status = 'candidate'
            ORDER BY created_at ASC
            LIMIT 1
            """,
            self._tenant_id,
            hypothesis.residual_cluster_hash,
            hypothesis.gap_kind,
        )
        if existing is None:
            raise RuntimeError("latent-gap insert conflicted but no row was found")
        return _hydrate(existing)

    async def _list_candidates(
        self,
        conn: asyncpg.Connection,
        limit: int,
    ) -> list[SageLatentGapHypothesis]:
        rows = await conn.fetch(
            f"""
            SELECT {_COLS_SQL}
            FROM sage_latent_gap_hypotheses
            WHERE tenant_id = $1 AND status = 'candidate'
            ORDER BY confidence DESC, created_at ASC
            LIMIT $2
            """,
            self._tenant_id,
            limit,
        )
        return [_hydrate(row) for row in rows]

    async def _resolve_with_conn(
        self,
        conn: asyncpg.Connection,
        hypothesis_id: UUID,
        *,
        status: str,
        reason: str,
        object_kind: str | None,
        object_id: UUID | None,
        metadata: dict[str, Any],
    ) -> SageLatentGapHypothesis | None:
        row = await conn.fetchrow(
            f"""
            UPDATE sage_latent_gap_hypotheses
            SET status = $3,
                resolution_object_kind = $4,
                resolution_object_id = $5,
                resolution_reason = $6,
                metadata = metadata || $7::jsonb,
                updated_at = now(),
                resolved_at = now()
            WHERE tenant_id = $1
              AND id = $2
              AND status = 'candidate'
            RETURNING {_COLS_SQL}
            """,
            self._tenant_id,
            hypothesis_id,
            status,
            object_kind,
            object_id,
            reason,
            _jsonb(metadata),
        )
        return _hydrate(row) if row is not None else None

    async def _with_conn(self, conn: Any, operation: Any) -> Any:
        if conn is not None:
            return await operation(conn)
        if self._pool is None:
            raise RuntimeError(
                "SageLatentGapHypothesisRepo was constructed without a pool; "
                "pass conn= or construct it with a pool"
            )
        async with self._pool.acquire() as acquired:
            return await operation(acquired)

    def _validate(
        self,
        hypothesis: SageLatentGapHypothesis,
        *,
        require_candidate: bool = False,
    ) -> None:
        if hypothesis.tenant_id != self._tenant_id:
            raise ValueError("SageLatentGapHypothesis.tenant_id does not match repo")
        if hypothesis.status not in LATENT_GAP_STATUSES:
            raise ValueError(f"invalid latent-gap status: {hypothesis.status}")
        if require_candidate and hypothesis.status != "candidate":
            raise ValueError("insert_candidate requires status='candidate'")
        if not hypothesis.gap_kind.strip():
            raise ValueError("gap_kind is required")
        if not hypothesis.residual_cluster_hash.strip():
            raise ValueError("residual_cluster_hash is required")
        if not hypothesis.supporting_residual_ids:
            raise ValueError("supporting_residual_ids is required")
        if not hypothesis.missing_evidence_statement.strip():
            raise ValueError("missing_evidence_statement is required")
        if not hypothesis.falsifier.strip():
            raise ValueError("falsifier is required")
        if not hypothesis.next_evidence_needed.strip():
            raise ValueError("next_evidence_needed is required")
        if not hypothesis.hypothesis_text.strip():
            raise ValueError("hypothesis_text is required")
        if not 0.0 <= float(hypothesis.confidence) <= 1.0:
            raise ValueError("confidence must be in [0, 1]")


async def create_latent_gap_hypotheses_for_sources(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source_observation_ids: tuple[UUID, ...],
    min_support: int = 1,
    max_candidates: int = 3,
) -> list[SageLatentGapHypothesis]:
    source_ids = _dedupe_uuids(source_observation_ids)
    if not source_ids or max_candidates <= 0:
        return []
    residual_repo = ModelResidualEvidenceRepo(tenant_id=tenant_id)
    hypothesis_repo = SageLatentGapHypothesisRepo(tenant_id=tenant_id)
    async with pool.acquire() as conn:
        residuals = await residual_repo.list_for_observations(
            list(source_ids),
            conn=conn,
        )
        candidates = build_latent_gap_hypotheses_from_residuals(
            residuals,
            tenant_id=tenant_id,
            min_support=min_support,
        )[:max_candidates]
        rows: list[SageLatentGapHypothesis] = []
        for candidate in candidates:
            rows.append(await hypothesis_repo.insert_candidate(candidate, conn=conn))
        return rows


def build_latent_gap_hypotheses_from_residuals(
    residuals: list[ModelResidualEvidence],
    *,
    tenant_id: UUID,
    min_support: int = 1,
) -> list[SageLatentGapHypothesis]:
    groups: dict[str, list[ModelResidualEvidence]] = {}
    for residual in residuals:
        if residual.tenant_id != tenant_id:
            continue
        if residual.status != "open" or residual.id is None:
            continue
        if _is_repair_validator_artifact(residual):
            continue
        groups.setdefault(residual.residual_kind, []).append(residual)

    hypotheses: list[SageLatentGapHypothesis] = []
    for gap_kind, grouped in sorted(groups.items()):
        deduped = _dedupe_residuals(grouped)
        if len(deduped) < max(1, int(min_support)):
            continue
        residual_ids = tuple(residual.id for residual in deduped if residual.id is not None)
        observation_ids = tuple(
            dict.fromkeys(
                residual.source_observation_id
                for residual in deduped
                if residual.source_observation_id is not None
            )
        )
        statement = _missing_evidence_statement(gap_kind, len(deduped))
        falsifier = _latent_gap_falsifier(gap_kind)
        next_evidence = _next_evidence_needed(gap_kind)
        hypotheses.append(
            SageLatentGapHypothesis(
                tenant_id=tenant_id,
                gap_kind=gap_kind,
                residual_cluster_hash=_cluster_hash(gap_kind, residual_ids),
                supporting_residual_ids=residual_ids,
                supporting_observation_ids=observation_ids,
                missing_evidence_statement=statement,
                falsifier=falsifier,
                next_evidence_needed=next_evidence,
                hypothesis_text=_hypothesis_text(
                    gap_kind=gap_kind,
                    statement=statement,
                    next_evidence=next_evidence,
                ),
                confidence=min(0.85, 0.35 + (0.10 * len(deduped))),
                metadata={
                    "source": "residual_cluster",
                    "residual_count": len(deduped),
                    "residual_kinds": [gap_kind],
                    "canonical_write": False,
                    "authority_effect": "none",
                },
            )
        )
    return hypotheses


def _is_repair_validator_artifact(residual: ModelResidualEvidence) -> bool:
    """Keep T4 repair-loop validator debris out of company gap hypotheses."""

    if residual.residual_kind != "validation_dropped_value":
        return False
    metadata = residual.metadata or {}
    trigger_kind = str(metadata.get("trigger_kind") or "")
    trigger_subkind = str(metadata.get("trigger_subkind") or "")
    repair_intent = str(metadata.get("repair_intent") or "")
    repair_source = str(metadata.get("repair_source") or "")
    if trigger_kind.startswith("T4") and trigger_subkind == "representation_repair":
        return True
    if repair_intent == "repair_validation_dropped_value":
        return True
    if trigger_kind.startswith("T4") and repair_source == "model_residual_evidence":
        return True
    return False


def _dedupe_uuids(values: tuple[UUID, ...]) -> tuple[UUID, ...]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


def _dedupe_residuals(
    residuals: list[ModelResidualEvidence],
) -> list[ModelResidualEvidence]:
    out: list[ModelResidualEvidence] = []
    seen: set[UUID] = set()
    for residual in sorted(residuals, key=lambda item: str(item.id)):
        if residual.id is None or residual.id in seen:
            continue
        seen.add(residual.id)
        out.append(residual)
    return out


def _cluster_hash(gap_kind: str, residual_ids: tuple[UUID, ...]) -> str:
    payload = {
        "gap_kind": gap_kind,
        "supporting_residual_ids": [str(residual_id) for residual_id in residual_ids],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]


def _missing_evidence_statement(gap_kind: str, count: int) -> str:
    if gap_kind == "counterevidence_unattached":
        return (
            f"{count} counterevidence residual(s) lack a target belief; "
            "the missing structure is the contested model or falsifier edge."
        )
    if gap_kind == "valuable_unmodeled":
        return (
            f"{count} valuable residual signal(s) did not become durable memory; "
            "the missing structure is the model, edge, relation, or projection they update."
        )
    if gap_kind == "compression_uncertain":
        return (
            f"{count} successful Think no-op residual(s) lack proof of compression; "
            "the missing structure is a compact model delta or justified ignore."
        )
    if gap_kind == "validation_dropped_value":
        return (
            f"{count} validation/apply drop residual(s) may contain useful value; "
            "the missing structure is the valid mutation shape."
        )
    if gap_kind == "relation_unanchored":
        return (
            f"{count} relation residual(s) could not bind to canonical endpoints; "
            "the missing structure is the relation claim/frame anchor."
        )
    if gap_kind == "open_question_needed":
        return (
            f"{count} residual(s) require explicit follow-up; "
            "the missing structure is the open question or clarification route."
        )
    return f"{count} residual signal(s) share unresolved missingness kind {gap_kind}."


def _latent_gap_falsifier(gap_kind: str) -> str:
    return {
        "counterevidence_unattached": (
            "A later trace attaches the counterevidence to a specific model as "
            "contestation or falsification."
        ),
        "valuable_unmodeled": (
            "A later trace absorbs every supporting residual into a model, edge, "
            "relation, projection, or justified ignore."
        ),
        "compression_uncertain": (
            "A later trace proves the no-op was justified or applies the missing "
            "compact model delta."
        ),
        "validation_dropped_value": (
            "A later validation trace proves the dropped operation was invalid noise "
            "rather than useful value."
        ),
        "relation_unanchored": (
            "A later trace binds the relation evidence to canonical endpoints or "
            "rejects it as invalid."
        ),
        "open_question_needed": (
            "A later trace creates, resolves, or rejects the required open question."
        ),
    }.get(gap_kind, "A later trace resolves the residual cluster without this hypothesis.")


def _next_evidence_needed(gap_kind: str) -> str:
    return {
        "counterevidence_unattached": (
            "Retrieve likely target models and attach confirm/contest/falsify evidence."
        ),
        "valuable_unmodeled": (
            "Rerun metabolism for the source observations with model-spine context."
        ),
        "compression_uncertain": (
            "Inspect Think context_use and applied ops to prove justified no-op or missed delta."
        ),
        "validation_dropped_value": (
            "Inspect validation/apply errors and convert valid dropped value into a mutation."
        ),
        "relation_unanchored": (
            "Retrieve endpoint models and promote the relation evidence into a claim/frame."
        ),
        "open_question_needed": (
            "Create a targeted open question or route the uncertainty to a clarifying human."
        ),
    }.get(gap_kind, "Collect targeted follow-up evidence or human clarification.")


def _hypothesis_text(
    *,
    gap_kind: str,
    statement: str,
    next_evidence: str,
) -> str:
    return (
        f"Latent company-understanding gap ({gap_kind}): {statement} "
        f"Next evidence needed: {next_evidence}"
    )


def _hydrate(row: Any) -> SageLatentGapHypothesis:
    data = dict(row)
    metadata = data.get("metadata") or {}
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    return SageLatentGapHypothesis(
        id=data.get("id"),
        tenant_id=data["tenant_id"],
        gap_kind=data["gap_kind"],
        status=data["status"],
        residual_cluster_hash=data["residual_cluster_hash"],
        supporting_residual_ids=tuple(data.get("supporting_residual_ids") or ()),
        supporting_observation_ids=tuple(data.get("supporting_observation_ids") or ()),
        missing_evidence_statement=data["missing_evidence_statement"],
        falsifier=data["falsifier"],
        next_evidence_needed=data["next_evidence_needed"],
        confidence=float(data["confidence"]),
        hypothesis_text=data["hypothesis_text"],
        metadata=dict(metadata) if isinstance(metadata, dict) else {},
        resolution_object_kind=data.get("resolution_object_kind"),
        resolution_object_id=data.get("resolution_object_id"),
        resolution_reason=data.get("resolution_reason"),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        resolved_at=data.get("resolved_at"),
    )


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, default=str)


__all__ = [
    "LATENT_GAP_RESOLUTION_OBJECT_KINDS",
    "LATENT_GAP_STATUSES",
    "SageLatentGapHypothesis",
    "SageLatentGapHypothesisRepo",
    "build_latent_gap_hypotheses_from_residuals",
    "create_latent_gap_hypotheses_for_sources",
]
