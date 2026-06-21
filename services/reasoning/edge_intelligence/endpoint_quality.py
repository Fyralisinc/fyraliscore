"""Endpoint quality gates for edge promotion."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg


_CONCRETE_SCOPE_TYPES = {"customer", "customer_resource", "commitment"}
_PRECISE_EDGE_KINDS = {
    "blocks",
    "enables",
    "supports",
    "weakens",
    "contradicts",
    "causes",
    "explains",
    "predicts",
    "early_warning_for",
    "contributes_to_resolution",
}
_DIAGNOSTIC_TERMS = (
    "unrecorded mutation",
    "state discontinuity",
    "consecutive audit events",
    "mutation gap",
    "missing transition",
)


@dataclass(frozen=True)
class EndpointQualityDecision:
    allowed: bool
    reasons: tuple[str, ...] = field(default_factory=tuple)


async def endpoint_quality_gate(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    source_model_id: UUID,
    target_model_id: UUID,
    edge_kind: str,
) -> EndpointQualityDecision:
    """Reject obviously unsafe edge endpoints when model rows are available."""
    try:
        rows = await conn.fetch(
            """
            SELECT id, status, archived_at, proposition_kind, "natural",
                   proposition, scope_entities
            FROM models
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
            """,
            tenant_id,
            [source_model_id, target_model_id],
        )
    except Exception:  # noqa: BLE001
        return EndpointQualityDecision(True)
    if len(rows) < 2:
        return EndpointQualityDecision(True)
    by_id = {row["id"]: row for row in rows}
    reasons: list[str] = []
    for model_id in (source_model_id, target_model_id):
        row = by_id.get(model_id)
        if row is None:
            continue
        if row["status"] != "active" or row["archived_at"] is not None:
            reasons.append("inactive_endpoint")
        if _is_composite(row):
            reasons.append("composite_endpoint")
        if _is_diagnostic(row):
            reasons.append("diagnostic_endpoint")
    if edge_kind in _PRECISE_EDGE_KINDS:
        source_scope = _scope_set(by_id[source_model_id])
        target_scope = _scope_set(by_id[target_model_id])
        if not _shared_concrete_scope(source_scope, target_scope):
            reasons.append("missing_shared_concrete_scope")
    return EndpointQualityDecision(not reasons, tuple(sorted(set(reasons))))


def _is_composite(row: asyncpg.Record) -> bool:
    if row["proposition_kind"] == "situation":
        return True
    prop = _json_obj(row["proposition"])
    return (
        prop.get("claim_role") == "situation"
        or prop.get("abstraction_level") == "composite"
    )


def _is_diagnostic(row: asyncpg.Record) -> bool:
    text = f"{row['natural'] or ''} {json.dumps(_json_obj(row['proposition']))}".lower()
    return any(term in text for term in _DIAGNOSTIC_TERMS)


def _scope_set(row: asyncpg.Record) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for item in _json_list(row["scope_entities"]):
        if not isinstance(item, dict):
            continue
        scope_type = str(item.get("type") or "").strip()
        scope_id = str(item.get("id") or "").strip()
        if scope_type and scope_id:
            out.add((scope_type, scope_id))
    return out


def _shared_concrete_scope(
    left: set[tuple[str, str]],
    right: set[tuple[str, str]],
) -> bool:
    return any(scope[0] in _CONCRETE_SCOPE_TYPES for scope in left & right)


def _json_obj(value: Any) -> dict[str, Any]:
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


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


__all__ = ["EndpointQualityDecision", "endpoint_quality_gate"]
