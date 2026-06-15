"""Deterministic edge-kind refinement for Think edge ops.

LLMs often choose ``supports`` as a safe fallback even when the prose names a
sharper registered relation. Keep that correction in one post-parse boundary so
prompt wording is not the only defense against generic graph writes.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from .diff_schema import EdgeOp


_REFINABLE_GENERIC_KINDS = {
    "supports",
    "co_occurs_with",
}
_PRECISE_DURABLE_KINDS = {
    "blocks",
    "contributes_to_resolution",
    "contradicts",
    "early_warning_for",
    "explains",
    "weakens",
}
_AUTO_ACCEPT_MIN_CONFIDENCE = 0.68

_BLOCKING_RE = re.compile(
    r"\b("
    r"block(?:s|ed|ing)?|blocked\s+(?:by|on|until)|waiting\s+(?:on|for)|"
    r"cannot\s+(?:proceed|launch|ship|approve|close)|prevents?|"
    r"prerequisite|depends?\s+on|required\s+before|requires?\s+.+\s+before"
    r")\b",
    re.I,
)
_NEGATED_BLOCKING_RE = re.compile(
    r"\b("
    r"does\s+not\s+block|doesn't\s+block|not\s+(?:a\s+)?blocker|"
    r"not\s+(?:clearly\s+)?(?:a\s+)?blocking\s+dependency|"
    r"not\s+blocked\s+by|no\s+blocking\s+dependency|"
    r"does\s+not\s+prevent|doesn't\s+prevent"
    r")\b",
    re.I,
)
_RESOLUTION_RE = re.compile(
    r"\b("
    r"resolv(?:e|es|ed|ing)|unblock(?:s|ed|ing)?|closed?|completed?|"
    r"delivered?|attached|accepted|approved|remediated|fixed|settle[ds]?"
    r")\b",
    re.I,
)
_WEAKEN_RE = re.compile(
    r"\b("
    r"weakens?|counter-?evidence|reduces?\s+confidence|undermines?|"
    r"not\s+enough\s+on\s+its\s+own|contrary\s+evidence"
    r")\b",
    re.I,
)
_CONTRADICT_RE = re.compile(r"\b(contradicts?|mutually exclusive|cannot both be true)\b", re.I)
_WARNING_RE = re.compile(
    r"\b(early warning|leading indicator|warning sign|risk signal|"
    r"churn risk|renewal risk|warns? about)\b",
    re.I,
)
_EXPLAINS_RE = re.compile(
    r"\b(explains?|because|due to|root cause|mechanism|reason for|"
    r"helps explain)\b",
    re.I,
)


@dataclass(frozen=True)
class EdgeEndpointText:
    source: str = ""
    target: str = ""


async def canonicalize_edge_semantics(
    op: EdgeOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_model_event_ids: set[UUID] | None = None,
) -> EdgeOp:
    """Return ``op`` with an unambiguous generic edge refined.

    The function is intentionally conservative: it only upgrades generic kinds
    to registered kinds when the edge explanation / metadata / endpoint text
    carries explicit relation language. It never invents endpoints.
    """

    if op.op != "add" or op.edge_kind not in _REFINABLE_GENERIC_KINDS:
        return op

    endpoint_text = await _load_endpoint_text(
        conn,
        tenant_id=tenant_id,
        source_model_id=op.source_model_id,
        target_model_id=op.target_model_id,
        pending_model_event_ids=pending_model_event_ids or set(),
    )
    explicit_text = _explicit_semantic_text(op)
    semantic_text = explicit_text or _endpoint_semantic_text(endpoint_text)
    if not semantic_text.strip():
        return op

    target_kind: str | None = None
    weight = op.weight
    reason: str | None = None

    has_blocking = (
        _BLOCKING_RE.search(semantic_text) is not None
        and _NEGATED_BLOCKING_RE.search(semantic_text) is None
    )

    if _CONTRADICT_RE.search(semantic_text):
        target_kind = "contradicts"
        weight = 0.65 if weight is None else weight
        reason = "explicit_contradiction_language"
    elif _WEAKEN_RE.search(semantic_text):
        target_kind = "weakens"
        weight = 0.55 if weight is None else weight
        reason = "explicit_counterevidence_language"
    elif _RESOLUTION_RE.search(semantic_text) and not has_blocking:
        target_kind = "contributes_to_resolution"
        weight = None
        reason = "explicit_resolution_language"
    elif has_blocking:
        target_kind = "blocks"
        weight = 0.75 if weight is None else weight
        reason = "explicit_blocking_language"
    elif _WARNING_RE.search(semantic_text):
        target_kind = "early_warning_for"
        weight = 0.55 if weight is None else weight
        reason = "explicit_warning_language"
    elif op.edge_kind == "supports" and _EXPLAINS_RE.search(semantic_text):
        target_kind = "explains"
        weight = 0.65 if weight is None else weight
        reason = "explicit_explanation_language"

    if target_kind is None or target_kind == op.edge_kind:
        return op

    metadata = dict(op.metadata or {})
    metadata.setdefault("canonicalized_from_edge_kind", op.edge_kind)
    metadata.setdefault("canonicalization_reason", reason)
    metadata.setdefault("canonicalized_by", "edge_semantic_refiner")
    explanation = op.explanation
    if not (explanation or "").strip():
        explanation = f"Refined from {op.edge_kind}: {reason}."
    return op.model_copy(
        update={
            "edge_kind": target_kind,
            "weight": weight,
            "metadata": metadata,
            "explanation": explanation,
        }
    )


def normalize_edge_review_status(op: EdgeOp) -> EdgeOp:
    """Promote explicit, evidence-backed precise edges to durable memory.

    The LLM often marks useful precise edges as ``candidate`` out of caution.
    Candidate is still valid, but production memory only compounds when
    high-confidence relation facts become accepted edges. Keep the promotion
    rule deterministic and auditable.
    """

    if op.op != "add":
        return op
    if op.review_status not in {"candidate", "needs_review"}:
        return op
    if op.edge_kind not in _PRECISE_DURABLE_KINDS:
        return op
    if float(op.confidence or 0.0) < _AUTO_ACCEPT_MIN_CONFIDENCE:
        return op
    if not (op.explanation or "").strip():
        return op
    if not (op.evidence_event_ids or op.evidence_model_ids):
        return op

    metadata = dict(op.metadata or {})
    metadata.setdefault("review_status_promoted_by", "edge_semantic_refiner")
    metadata.setdefault("review_status_promoted_reason", "explicit_evidence_backed_precise_edge")
    return op.model_copy(
        update={
            "review_status": "accepted",
            "metadata": metadata,
        }
    )


async def _load_endpoint_text(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    source_model_id: UUID,
    target_model_id: UUID,
    pending_model_event_ids: set[UUID],
) -> EdgeEndpointText:
    ids = [
        mid
        for mid in (source_model_id, target_model_id)
        if mid not in pending_model_event_ids
    ]
    if not ids:
        return EdgeEndpointText()
    rows = await conn.fetch(
        """
        SELECT id, "natural", proposition
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        ids,
    )
    by_id: dict[UUID, str] = {}
    for row in rows:
        by_id[row["id"]] = _model_text(row)
    return EdgeEndpointText(
        source=by_id.get(source_model_id, ""),
        target=by_id.get(target_model_id, ""),
    )


def _model_text(row: asyncpg.Record) -> str:
    parts: list[str] = []
    natural = row["natural"]
    if natural:
        parts.append(str(natural))
    prop = row["proposition"]
    if isinstance(prop, str):
        parts.append(prop)
    elif isinstance(prop, dict):
        parts.extend(str(v) for v in prop.values() if isinstance(v, str))
    return " ".join(parts)


def _explicit_semantic_text(op: EdgeOp) -> str:
    metadata_text = ""
    if op.metadata:
        try:
            metadata_text = json.dumps(op.metadata, sort_keys=True, default=str)
        except TypeError:
            metadata_text = str(op.metadata)
    return " ".join(
        part
        for part in (
            op.explanation or "",
            metadata_text,
        )
        if part
    )


def _endpoint_semantic_text(endpoint_text: EdgeEndpointText) -> str:
    return " ".join(
        part for part in (endpoint_text.source, endpoint_text.target) if part
    )


__all__ = ["canonicalize_edge_semantics", "normalize_edge_review_status"]
