"""SAGE feedback for reviewed latent pattern candidates."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.reasoning.sage.discovery.negative_memory_repo import NegativeMemoryRepo
from services.reasoning.sage.discovery.shortcuts_repo import DiscoveryShortcutsRepo
from services.reasoning.sage.discovery.types import NegativeMemory


PatternReviewOutcome = Literal["accepted", "rejected"]


@dataclass(frozen=True, slots=True)
class PatternReviewFeedbackReport:
    candidate_id: UUID
    tenant_id: UUID | None
    outcome: PatternReviewOutcome
    candidate_found: bool
    shortcut_written: bool = False
    negative_memory_written: bool = False
    skipped_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def record_pattern_review_feedback(
    conn: asyncpg.Connection,
    *,
    candidate_id: UUID,
    outcome: PatternReviewOutcome,
    promoted_pattern_model_id: UUID | None = None,
    rejection_reason: str | None = None,
) -> PatternReviewFeedbackReport:
    """Metabolize a pattern-review outcome into SAGE utility memory.

    The feedback is deliberately non-canonical:
      * accepted candidates create/bump a discovery shortcut to the promoted
        Pattern Model;
      * rejected candidates create expiring negative memory for the candidate
        shape so future retrieval/scouting avoids the same dead end.
    """

    candidate = await _load_pattern_candidate(conn, candidate_id)
    if candidate is None:
        return PatternReviewFeedbackReport(
            candidate_id=candidate_id,
            tenant_id=None,
            outcome=outcome,
            candidate_found=False,
            skipped_reason="pattern_candidate_not_found",
        )
    tenant_id = candidate["tenant_id"]
    signature = _feedback_signature(candidate_id, candidate)
    if outcome == "accepted":
        if promoted_pattern_model_id is None:
            return PatternReviewFeedbackReport(
                candidate_id=candidate_id,
                tenant_id=tenant_id,
                outcome=outcome,
                candidate_found=True,
                skipped_reason="missing_promoted_pattern_model_id",
            )
        if not await _table_exists(conn, "discovery_shortcuts"):
            return PatternReviewFeedbackReport(
                candidate_id=candidate_id,
                tenant_id=tenant_id,
                outcome=outcome,
                candidate_found=True,
                skipped_reason="discovery_shortcuts_missing",
            )
        repo = DiscoveryShortcutsRepo(None, tenant_id=tenant_id)
        await repo.upsert_from_outcome(
            signature,
            to_model_id=promoted_pattern_model_id,
            delta_utility=_accepted_delta(candidate),
            conn=conn,
        )
        return PatternReviewFeedbackReport(
            candidate_id=candidate_id,
            tenant_id=tenant_id,
            outcome=outcome,
            candidate_found=True,
            shortcut_written=True,
        )

    if not await _table_exists(conn, "negative_memory"):
        return PatternReviewFeedbackReport(
            candidate_id=candidate_id,
            tenant_id=tenant_id,
            outcome=outcome,
            candidate_found=True,
            skipped_reason="negative_memory_missing",
        )
    memory = NegativeMemory(
        id=uuid7(),
        tenant_id=tenant_id,
        memory_type="rejected_hypothesis",
        signature=signature,
        rejected_claim=_candidate_claim(candidate),
        rejected_path={
            "source": "pattern_review",
            "pattern_candidate_id": str(candidate_id),
            "constituent_model_ids": [
                str(model_id) for model_id in candidate["constituent_model_ids"]
            ],
            "proposed_signature": _json_obj(candidate["proposed_signature"]),
        },
        reason=rejection_reason or "pattern review rejected weak candidate",
        evidence_snapshot_hash=_evidence_snapshot_hash(candidate),
        confidence=_rejection_confidence(candidate),
        expires_at=datetime.now(timezone.utc) + timedelta(days=60),
    )
    repo = NegativeMemoryRepo(None, tenant_id=tenant_id)
    await repo.insert(memory, conn=conn)
    return PatternReviewFeedbackReport(
        candidate_id=candidate_id,
        tenant_id=tenant_id,
        outcome=outcome,
        candidate_found=True,
        negative_memory_written=True,
    )


async def _load_pattern_candidate(
    conn: asyncpg.Connection,
    candidate_id: UUID,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT tenant_id, proposed_signature, observed_tendency,
               constituent_model_ids, cluster_size, density,
               promoted_pattern_model_id, rejected_at, rejection_reason
        FROM pattern_candidates
        WHERE id = $1
        """,
        candidate_id,
    )
    return dict(row) if row is not None else None


def _feedback_signature(candidate_id: UUID, candidate: dict[str, Any]) -> dict[str, Any]:
    proposed = _json_obj(candidate["proposed_signature"])
    tendency = _json_obj(candidate["observed_tendency"])
    entities = [
        "pattern_review",
        f"pattern_candidate:{candidate_id}",
        f"cluster_size:{int(candidate['cluster_size'])}",
    ]
    kind = proposed.get("kind")
    if kind:
        entities.append(f"signature_kind:{kind}")
    cluster_size = tendency.get("cluster_size")
    if cluster_size:
        entities.append(f"observed_cluster_size:{cluster_size}")
    return {
        "signal_type": "pattern_review",
        "question_primitive": "RECURRENCE",
        "entities": entities[:12],
    }


def _candidate_claim(candidate: dict[str, Any]) -> str:
    proposed = _json_obj(candidate["proposed_signature"])
    tendency = _json_obj(candidate["observed_tendency"])
    exemplars = tendency.get("exemplars")
    exemplar = ""
    if isinstance(exemplars, list) and exemplars:
        exemplar = str(exemplars[0])
    signature_kind = str(proposed.get("kind") or "cluster_signature")
    return (
        f"Rejected pattern candidate {signature_kind}; "
        f"cluster_size={candidate['cluster_size']}; "
        f"density={float(candidate['density']):.4f}; exemplar={exemplar[:220]}"
    )


def _accepted_delta(candidate: dict[str, Any]) -> float:
    density = max(0.0, min(1.0, float(candidate["density"] or 0.0)))
    support = min(1.0, int(candidate["cluster_size"] or 0) / 6.0)
    return round(0.22 + 0.18 * density + 0.10 * support, 4)


def _rejection_confidence(candidate: dict[str, Any]) -> float:
    density = max(0.0, min(1.0, float(candidate["density"] or 0.0)))
    support = min(1.0, int(candidate["cluster_size"] or 0) / 6.0)
    return round(max(0.35, min(0.85, 0.42 + 0.18 * density + 0.10 * support)), 4)


def _evidence_snapshot_hash(candidate: dict[str, Any]) -> str:
    payload = {
        "constituent_model_ids": [
            str(model_id) for model_id in candidate["constituent_model_ids"]
        ],
        "proposed_signature": _json_obj(candidate["proposed_signature"]),
        "observed_tendency": _json_obj(candidate["observed_tendency"]),
        "cluster_size": int(candidate["cluster_size"]),
        "density": float(candidate["density"]),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]


def _json_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1)", f"public.{table_name}"))


__all__ = [
    "PatternReviewFeedbackReport",
    "PatternReviewOutcome",
    "record_pattern_review_feedback",
]
