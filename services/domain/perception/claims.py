"""Evidence-bound semantic claims that precede reasoning models."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EvidenceSpan(_Strict):
    start: int = Field(ge=0)
    end: int = Field(ge=0)
    text_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_field: str = "content_text"

    @model_validator(mode="after")
    def validate_offsets(self) -> "EvidenceSpan":
        if self.end < self.start:
            raise ValueError("evidence span end must not precede start")
        return self


class PerceptionClaimCreate(_Strict):
    tenant_id: UUID
    evidence_id: UUID
    observation_id: UUID
    claimant_ref: dict[str, Any] | None = None
    subject_ref: dict[str, Any]
    predicate: str = Field(min_length=1)
    object_value: Any
    modality: Literal[
        "asserted", "asked", "proposed", "planned", "reported", "denied", "unknown"
    ] = "asserted"
    polarity: Literal["positive", "negative", "unknown"] = "positive"
    confidence: float = Field(ge=0, le=1)
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    evidence_span: EvidenceSpan
    extractor_kind: Literal["deterministic", "model", "human"]
    extractor_name: str = Field(min_length=1)
    extractor_version: str = Field(min_length=1)
    extraction_run_id: UUID | None = None
    supersedes_claim_id: UUID | None = None

    @model_validator(mode="after")
    def validate_claim(self) -> "PerceptionClaimCreate":
        if not self.subject_ref:
            raise ValueError("subject_ref must be non-empty")
        if self.valid_to is not None and self.valid_from is not None:
            if self.valid_to < self.valid_from:
                raise ValueError("valid_to must not precede valid_from")
        return self


class PerceptionClaimRow(PerceptionClaimCreate):
    id: UUID
    claim_key: str
    status: Literal["active", "superseded", "rejected"]
    created_at: datetime


_COLUMNS = (
    "id", "tenant_id", "evidence_id", "observation_id", "claimant_ref",
    "subject_ref", "predicate", "object_value", "modality", "polarity",
    "confidence", "valid_from", "valid_to", "evidence_span",
    "extractor_kind", "extractor_name", "extractor_version",
    "extraction_run_id", "claim_key", "status", "supersedes_claim_id",
    "created_at",
)
_SELECT = ", ".join(_COLUMNS)


def _json(value: Any) -> Any:
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


def _hydrate(row: asyncpg.Record) -> PerceptionClaimRow:
    value = dict(row)
    for key in ("claimant_ref", "subject_ref", "object_value", "evidence_span"):
        value[key] = _json(value.get(key))
    return PerceptionClaimRow.model_validate(value)


def claim_key(value: PerceptionClaimCreate) -> str:
    semantic = {
        "claimant_ref": value.claimant_ref,
        "subject_ref": value.subject_ref,
        "predicate": value.predicate,
        "object_value": value.object_value,
        "modality": value.modality,
        "polarity": value.polarity,
        "valid_from": value.valid_from.isoformat() if value.valid_from else None,
        "valid_to": value.valid_to.isoformat() if value.valid_to else None,
        "evidence_span": value.evidence_span.model_dump(mode="json"),
    }
    encoded = json.dumps(
        semantic, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class PerceptionClaimRepository:
    async def insert(
        self,
        value: PerceptionClaimCreate,
        *,
        conn: asyncpg.Connection,
    ) -> PerceptionClaimRow:
        async with conn.transaction():
            return await self._insert_in_transaction(value, conn=conn)

    async def _insert_in_transaction(
        self,
        value: PerceptionClaimCreate,
        *,
        conn: asyncpg.Connection,
    ) -> PerceptionClaimRow:
        observation_text = await conn.fetchval(
            """
            SELECT content_text FROM observations
             WHERE id = $1 AND tenant_id = $2 AND evidence_id = $3
            """,
            value.observation_id,
            value.tenant_id,
            value.evidence_id,
        )
        if observation_text is None:
            raise ValidationError(
                "claim observation must be linked to the same evidence revision"
            )
        span = value.evidence_span
        if span.end > len(observation_text):
            raise ValidationError("claim evidence span exceeds observation text")
        excerpt = observation_text[span.start : span.end]
        actual_hash = hashlib.sha256(excerpt.encode("utf-8")).hexdigest()
        if actual_hash != span.text_hash:
            raise ValidationError("claim evidence span hash does not match observation")
        key = claim_key(value)
        if value.supersedes_claim_id is not None:
            await conn.execute(
                """
                UPDATE perception_claims SET status = 'superseded'
                 WHERE id = $1 AND tenant_id = $2 AND status = 'active'
                """,
                value.supersedes_claim_id,
                value.tenant_id,
            )
        row = await conn.fetchrow(
            f"""
            INSERT INTO perception_claims (
              id, tenant_id, evidence_id, observation_id, claimant_ref,
              subject_ref, predicate, object_value, modality, polarity,
              confidence, valid_from, valid_to, evidence_span,
              extractor_kind, extractor_name, extractor_version,
              extraction_run_id, claim_key, supersedes_claim_id
            ) VALUES (
              $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8::jsonb,
              $9, $10, $11, $12, $13, $14::jsonb, $15, $16, $17,
              $18, $19, $20
            )
            ON CONFLICT (
              tenant_id, evidence_id, extractor_name, extractor_version, claim_key
            ) DO UPDATE SET confidence = greatest(
              perception_claims.confidence, EXCLUDED.confidence
            )
            RETURNING {_SELECT}
            """,
            uuid7(), value.tenant_id, value.evidence_id, value.observation_id,
            json.dumps(value.claimant_ref) if value.claimant_ref is not None else None,
            json.dumps(value.subject_ref, sort_keys=True),
            value.predicate,
            json.dumps(value.object_value, sort_keys=True, default=str),
            value.modality, value.polarity, value.confidence,
            value.valid_from, value.valid_to,
            json.dumps(value.evidence_span.model_dump(mode="json"), sort_keys=True),
            value.extractor_kind, value.extractor_name, value.extractor_version,
            value.extraction_run_id, key, value.supersedes_claim_id,
        )
        assert row is not None
        return _hydrate(row)

    async def find_contradictions(
        self,
        *,
        tenant_id: UUID,
        subject_ref: dict[str, Any],
        predicate: str,
        conn: asyncpg.Connection,
    ) -> list[tuple[PerceptionClaimRow, PerceptionClaimRow]]:
        rows = await conn.fetch(
            f"""
            SELECT {_SELECT} FROM perception_claims
             WHERE tenant_id = $1 AND subject_ref = $2::jsonb
               AND predicate = $3 AND status = 'active'
             ORDER BY created_at, id
            """,
            tenant_id,
            json.dumps(subject_ref, sort_keys=True),
            predicate,
        )
        claims = [_hydrate(row) for row in rows]
        contradictions: list[tuple[PerceptionClaimRow, PerceptionClaimRow]] = []
        for index, left in enumerate(claims):
            for right in claims[index + 1 :]:
                opposite = {left.polarity, right.polarity} == {"positive", "negative"}
                different_values = left.object_value != right.object_value
                if opposite or different_values:
                    contradictions.append((left, right))
        return contradictions


def span_for_text(text: str, start: int = 0, end: int | None = None) -> EvidenceSpan:
    end = len(text) if end is None else end
    excerpt = text[start:end]
    return EvidenceSpan(
        start=start,
        end=end,
        text_hash=hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
    )


__all__ = [
    "EvidenceSpan", "PerceptionClaimCreate", "PerceptionClaimRepository",
    "PerceptionClaimRow", "claim_key", "span_for_text",
]
