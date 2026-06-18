"""Repository helpers for provisional company-substrate candidates.

Substrate candidates are evidence-backed, provisional references discovered
while Think is preparing a reasoning context. They let Models be scoped to
"likely actor/customer/workstream/etc." handles before the system has enough
evidence to safely promote a canonical actor, resource, or act.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7


SUBSTRATE_KINDS: frozenset[str] = frozenset(
    {
        "actor",
        "actor_alias",
        "customer",
        "workstream",
        "system",
        "vendor",
        "commitment",
        "pattern",
    }
)

SUBSTRATE_STATUSES: frozenset[str] = frozenset(
    {
        "proposed",
        "needs_clarification",
        "promoted",
        "rejected",
        "merged",
        "stale",
    }
)


@dataclass(frozen=True, slots=True)
class SubstrateCandidate:
    id: UUID
    tenant_id: UUID
    kind: str
    label: str
    status: str
    confidence: float
    fingerprint: str
    aliases: list[dict[str, Any]] = field(default_factory=list)
    evidence_observation_ids: list[UUID] = field(default_factory=list)
    evidence_model_ids: list[UUID] = field(default_factory=list)
    related_candidate_ids: list[UUID] = field(default_factory=list)
    proposed_canonical_ref: dict[str, Any] | None = None
    promotion_ref: dict[str, Any] | None = None
    merge_target_id: UUID | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_by_run_id: UUID | None = None

    @property
    def scope_ref(self) -> dict[str, str]:
        return {"type": f"candidate_{self.kind}", "id": str(self.id)}

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "tenant_id": str(self.tenant_id),
            "kind": self.kind,
            "label": self.label,
            "status": self.status,
            "confidence": self.confidence,
            "fingerprint": self.fingerprint,
            "aliases": self.aliases,
            "evidence_observation_ids": [
                str(value) for value in self.evidence_observation_ids
            ],
            "evidence_model_ids": [str(value) for value in self.evidence_model_ids],
            "related_candidate_ids": [
                str(value) for value in self.related_candidate_ids
            ],
            "proposed_canonical_ref": self.proposed_canonical_ref,
            "promotion_ref": self.promotion_ref,
            "merge_target_id": (
                str(self.merge_target_id) if self.merge_target_id is not None else None
            ),
            "metadata": self.metadata,
            "created_by_run_id": (
                str(self.created_by_run_id)
                if self.created_by_run_id is not None
                else None
            ),
            "scope_ref": self.scope_ref,
        }


async def upsert_substrate_candidate(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kind: str,
    label: str,
    fingerprint: str,
    confidence: float = 0.5,
    aliases: Iterable[Mapping[str, Any]] | None = None,
    evidence_observation_ids: Iterable[UUID | str] | None = None,
    evidence_model_ids: Iterable[UUID | str] | None = None,
    related_candidate_ids: Iterable[UUID | str] | None = None,
    proposed_canonical_ref: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    status: str = "proposed",
    created_by_run_id: UUID | None = None,
) -> SubstrateCandidate:
    """Insert or merge an evidence-backed provisional candidate."""

    _validate_candidate_inputs(
        kind=kind,
        label=label,
        fingerprint=fingerprint,
        confidence=confidence,
        status=status,
    )
    alias_list = _dedupe_json_objects(aliases or [])
    obs_ids = _uuid_list(evidence_observation_ids or [])
    model_ids = _uuid_list(evidence_model_ids or [])
    related_ids = _uuid_list(related_candidate_ids or [])
    candidate_id = uuid7()

    row = await conn.fetchrow(
        """
        INSERT INTO substrate_candidates (
            id, tenant_id, kind, label, status, confidence, fingerprint,
            aliases, evidence_observation_ids, evidence_model_ids,
            related_candidate_ids, proposed_canonical_ref, metadata,
            created_by_run_id, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8::jsonb, $9::uuid[], $10::uuid[],
            $11::uuid[], $12::jsonb, $13::jsonb,
            $14, now(), now()
        )
        ON CONFLICT (tenant_id, kind, fingerprint) DO UPDATE SET
            label = CASE
              WHEN length(EXCLUDED.label) > length(substrate_candidates.label)
              THEN EXCLUDED.label
              ELSE substrate_candidates.label
            END,
            confidence = greatest(
              substrate_candidates.confidence,
              EXCLUDED.confidence
            ),
            status = CASE
              WHEN substrate_candidates.status = 'promoted'
              THEN substrate_candidates.status
              WHEN EXCLUDED.status <> 'proposed'
              THEN EXCLUDED.status
              ELSE substrate_candidates.status
            END,
            aliases = (
              SELECT coalesce(jsonb_agg(value ORDER BY value::text), '[]'::jsonb)
              FROM (
                SELECT DISTINCT value
                FROM jsonb_array_elements(
                  substrate_candidates.aliases || EXCLUDED.aliases
                ) AS t(value)
              ) AS deduped
            ),
            evidence_observation_ids = ARRAY(
              SELECT DISTINCT value
              FROM unnest(
                substrate_candidates.evidence_observation_ids
                || EXCLUDED.evidence_observation_ids
              ) AS t(value)
              ORDER BY value
            ),
            evidence_model_ids = ARRAY(
              SELECT DISTINCT value
              FROM unnest(
                substrate_candidates.evidence_model_ids
                || EXCLUDED.evidence_model_ids
              ) AS t(value)
              ORDER BY value
            ),
            related_candidate_ids = ARRAY(
              SELECT DISTINCT value
              FROM unnest(
                substrate_candidates.related_candidate_ids
                || EXCLUDED.related_candidate_ids
              ) AS t(value)
              ORDER BY value
            ),
            proposed_canonical_ref = coalesce(
              substrate_candidates.proposed_canonical_ref,
              EXCLUDED.proposed_canonical_ref
            ),
            metadata = substrate_candidates.metadata || EXCLUDED.metadata,
            created_by_run_id = coalesce(
              substrate_candidates.created_by_run_id,
              EXCLUDED.created_by_run_id
            ),
            updated_at = now()
        RETURNING
            id, tenant_id, kind, label, status, confidence, fingerprint,
            aliases, evidence_observation_ids, evidence_model_ids,
            related_candidate_ids, proposed_canonical_ref, promotion_ref,
            merge_target_id, metadata, created_by_run_id
        """,
        candidate_id,
        tenant_id,
        kind,
        label.strip(),
        status,
        float(confidence),
        fingerprint.strip(),
        _jsonb(alias_list),
        obs_ids,
        model_ids,
        related_ids,
        _jsonb_or_none(proposed_canonical_ref),
        _jsonb(dict(metadata or {})),
        created_by_run_id,
    )
    assert row is not None
    return _hydrate(row)


async def list_substrate_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    statuses: Iterable[str] = ("proposed", "needs_clarification"),
    kinds: Iterable[str] | None = None,
    limit: int = 100,
) -> list[SubstrateCandidate]:
    status_values = list(statuses)
    for status in status_values:
        if status not in SUBSTRATE_STATUSES:
            raise ValidationError(
                f"invalid substrate candidate status {status!r}",
                field="status",
                value=status,
            )
    kind_values = list(kinds or [])
    for kind in kind_values:
        if kind not in SUBSTRATE_KINDS:
            raise ValidationError(
                f"invalid substrate candidate kind {kind!r}",
                field="kind",
                value=kind,
            )

    rows = await conn.fetch(
        """
        SELECT
            id, tenant_id, kind, label, status, confidence, fingerprint,
            aliases, evidence_observation_ids, evidence_model_ids,
            related_candidate_ids, proposed_canonical_ref, promotion_ref,
            merge_target_id, metadata, created_by_run_id
        FROM substrate_candidates
        WHERE tenant_id = $1
          AND status = ANY($2::text[])
          AND (cardinality($3::text[]) = 0 OR kind = ANY($3::text[]))
        ORDER BY
          confidence DESC,
          cardinality(evidence_observation_ids) DESC,
          updated_at DESC
        LIMIT $4
        """,
        tenant_id,
        status_values,
        kind_values,
        max(1, int(limit)),
    )
    return [_hydrate(row) for row in rows]


async def get_substrate_candidate(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    candidate_id: UUID,
) -> SubstrateCandidate | None:
    row = await conn.fetchrow(
        """
        SELECT
            id, tenant_id, kind, label, status, confidence, fingerprint,
            aliases, evidence_observation_ids, evidence_model_ids,
            related_candidate_ids, proposed_canonical_ref, promotion_ref,
            merge_target_id, metadata, created_by_run_id
        FROM substrate_candidates
        WHERE tenant_id = $1 AND id = $2
        """,
        tenant_id,
        candidate_id,
    )
    return _hydrate(row) if row is not None else None


async def load_substrate_candidates_for_observations(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observation_ids: Iterable[UUID | str],
    limit: int = 80,
) -> list[SubstrateCandidate]:
    obs_ids = _uuid_list(observation_ids)
    if not obs_ids:
        return []
    rows = await conn.fetch(
        """
        SELECT
            id, tenant_id, kind, label, status, confidence, fingerprint,
            aliases, evidence_observation_ids, evidence_model_ids,
            related_candidate_ids, proposed_canonical_ref, promotion_ref,
            merge_target_id, metadata, created_by_run_id
        FROM substrate_candidates
        WHERE tenant_id = $1
          AND status IN ('proposed', 'needs_clarification', 'promoted')
          AND evidence_observation_ids && $2::uuid[]
        ORDER BY
          confidence DESC,
          cardinality(evidence_observation_ids) DESC,
          updated_at DESC
        LIMIT $3
        """,
        tenant_id,
        obs_ids,
        max(1, int(limit)),
    )
    return [_hydrate(row) for row in rows]


def candidate_scope_ref(candidate: SubstrateCandidate) -> dict[str, str]:
    return candidate.scope_ref


def _validate_candidate_inputs(
    *,
    kind: str,
    label: str,
    fingerprint: str,
    confidence: float,
    status: str,
) -> None:
    if kind not in SUBSTRATE_KINDS:
        raise ValidationError(
            f"invalid substrate candidate kind {kind!r}",
            field="kind",
            value=kind,
        )
    if status not in SUBSTRATE_STATUSES:
        raise ValidationError(
            f"invalid substrate candidate status {status!r}",
            field="status",
            value=status,
        )
    if not label or not label.strip():
        raise ValidationError("label must be non-empty", field="label")
    if not fingerprint or not fingerprint.strip():
        raise ValidationError(
            "fingerprint must be non-empty",
            field="fingerprint",
        )
    if not (0.0 <= float(confidence) <= 1.0):
        raise ValidationError(
            "confidence must be in [0,1]",
            field="confidence",
            value=confidence,
        )


def _hydrate(row: Mapping[str, Any]) -> SubstrateCandidate:
    data = dict(row)
    return SubstrateCandidate(
        id=_as_uuid(data["id"]),
        tenant_id=_as_uuid(data["tenant_id"]),
        kind=str(data["kind"]),
        label=str(data["label"]),
        status=str(data["status"]),
        confidence=float(data["confidence"]),
        fingerprint=str(data["fingerprint"]),
        aliases=_json_list(data.get("aliases")),
        evidence_observation_ids=_uuid_list(data.get("evidence_observation_ids") or []),
        evidence_model_ids=_uuid_list(data.get("evidence_model_ids") or []),
        related_candidate_ids=_uuid_list(data.get("related_candidate_ids") or []),
        proposed_canonical_ref=_json_obj_or_none(data.get("proposed_canonical_ref")),
        promotion_ref=_json_obj_or_none(data.get("promotion_ref")),
        merge_target_id=(
            _as_uuid(data["merge_target_id"])
            if data.get("merge_target_id") is not None
            else None
        ),
        metadata=_json_obj(data.get("metadata")),
        created_by_run_id=(
            _as_uuid(data["created_by_run_id"])
            if data.get("created_by_run_id") is not None
            else None
        ),
    )


def _dedupe_json_objects(values: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        item = dict(value)
        key = json.dumps(item, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _uuid_list(values: Iterable[UUID | str]) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        parsed = _as_uuid(value)
        if parsed in seen:
            continue
        seen.add(parsed)
        out.append(parsed)
    return out


def _as_uuid(value: UUID | str) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _jsonb_or_none(value: Mapping[str, Any] | None) -> str | None:
    if value is None:
        return None
    return _jsonb(dict(value))


def _json_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value)


def _json_obj_or_none(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return _json_obj(value)


def _json_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, str):
        value = json.loads(value)
    return [dict(item) for item in value]


__all__ = [
    "SUBSTRATE_KINDS",
    "SUBSTRATE_STATUSES",
    "SubstrateCandidate",
    "candidate_scope_ref",
    "get_substrate_candidate",
    "list_substrate_candidates",
    "load_substrate_candidates_for_observations",
    "upsert_substrate_candidate",
]
