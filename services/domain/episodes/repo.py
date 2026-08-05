"""Persistence for topic routing and append-only episode membership."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7

from .routing import MembershipDecisionValue, RoutingSignal, TopicCandidate, canonical_ref, topic_key


class _Row(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EpisodeTopicRow(_Row):
    id: UUID
    tenant_id: UUID
    topic_key: str
    origin: Literal["automatic", "query_seeded", "human_pinned"]
    label: str
    query_text: str | None
    requester_actor_id: UUID | None
    primary_anchor: dict[str, Any]
    anchor_refs: tuple[dict[str, Any], ...]
    claim_predicates: tuple[str, ...]
    lexical_terms: tuple[str, ...]
    valid_time_start: datetime | None
    valid_time_end: datetime | None
    router_name: str
    router_version: str
    head_version: int
    status: Literal["active", "superseded", "archived"]
    created_at: datetime


class EpisodeRow(_Row):
    id: UUID
    tenant_id: UUID
    topic_id: UUID
    lifecycle_state: Literal["open", "dormant", "settled", "reopened", "superseded"]
    head_version: int
    opened_at: datetime
    last_event_at: datetime
    last_ingested_at: datetime
    created_at: datetime
    updated_at: datetime


class EpisodeRouterRunRow(_Row):
    id: UUID
    tenant_id: UUID
    perception_outbox_id: UUID
    observation_id: UUID
    observation_occurred_at: datetime
    evidence_id: UUID
    identity_snapshot_id: UUID
    input_hash: str
    router_name: str
    router_version: str
    feature_schema_version: int
    status: Literal["running", "completed", "failed"]
    result_hash: str | None
    failure: str | None
    started_at: datetime
    completed_at: datetime | None


class EpisodeMembershipRow(_Row):
    id: UUID
    tenant_id: UUID
    topic_id: UUID
    episode_id: UUID
    router_run_id: UUID
    observation_id: UUID
    observation_occurred_at: datetime
    evidence_id: UUID
    identity_snapshot_id: UUID
    claim_ids: tuple[UUID, ...]
    identity_assertion_ids: tuple[UUID, ...]
    decision: Literal["include", "exclude", "hold"]
    score: float
    reasons: tuple[dict[str, Any], ...]
    feature_snapshot: dict[str, Any]
    router_name: str
    router_version: str
    feature_schema_version: int
    decision_key: str
    status: Literal["proposed", "accepted", "rejected", "superseded"]
    supersedes_assertion_id: UUID | None
    created_at: datetime


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value


def _topic(row: asyncpg.Record) -> EpisodeTopicRow:
    value = dict(row)
    for name in ("primary_anchor", "anchor_refs", "claim_predicates", "lexical_terms"):
        value[name] = _json(value[name])
    value["anchor_refs"] = tuple(value["anchor_refs"])
    value["claim_predicates"] = tuple(value["claim_predicates"])
    value["lexical_terms"] = tuple(value["lexical_terms"])
    return EpisodeTopicRow.model_validate(value)


def _episode(row: asyncpg.Record) -> EpisodeRow:
    return EpisodeRow.model_validate(dict(row))


def _run(row: asyncpg.Record) -> EpisodeRouterRunRow:
    return EpisodeRouterRunRow.model_validate(dict(row))


def _membership(row: asyncpg.Record) -> EpisodeMembershipRow:
    value = dict(row)
    value["claim_ids"] = tuple(value["claim_ids"])
    value["identity_assertion_ids"] = tuple(value["identity_assertion_ids"])
    value["reasons"] = tuple(_json(value["reasons"]))
    value["feature_snapshot"] = _json(value["feature_snapshot"])
    return EpisodeMembershipRow.model_validate(value)


_TOPIC_COLUMNS = (
    "id", "tenant_id", "topic_key", "origin", "label", "query_text",
    "requester_actor_id", "primary_anchor", "anchor_refs", "claim_predicates",
    "lexical_terms", "valid_time_start", "valid_time_end", "router_name",
    "router_version", "head_version", "status", "created_at",
)
_EPISODE_COLUMNS = (
    "id", "tenant_id", "topic_id", "lifecycle_state", "head_version",
    "opened_at", "last_event_at", "last_ingested_at", "created_at", "updated_at",
)
_RUN_COLUMNS = (
    "id", "tenant_id", "perception_outbox_id", "observation_id",
    "observation_occurred_at", "evidence_id", "identity_snapshot_id",
    "input_hash", "router_name", "router_version", "feature_schema_version",
    "status", "result_hash", "failure", "started_at", "completed_at",
)
_MEMBERSHIP_COLUMNS = (
    "id", "tenant_id", "topic_id", "episode_id", "router_run_id",
    "observation_id", "observation_occurred_at", "evidence_id",
    "identity_snapshot_id", "claim_ids", "identity_assertion_ids", "decision",
    "score", "reasons", "feature_snapshot", "router_name", "router_version",
    "feature_schema_version", "decision_key", "status",
    "supersedes_assertion_id", "created_at",
)


class EpisodeRoutingRepository:
    async def start_run(
        self,
        *,
        tenant_id: UUID,
        perception_outbox_id: UUID,
        observation_id: UUID,
        observation_occurred_at: datetime,
        evidence_id: UUID,
        identity_snapshot_id: UUID,
        input_hash: str,
        router_name: str,
        router_version: str,
        feature_schema_version: int,
        conn: asyncpg.Connection,
    ) -> EpisodeRouterRunRow:
        row = await conn.fetchrow(
            f"""
            INSERT INTO episode_router_runs (
              id, tenant_id, perception_outbox_id, observation_id,
              observation_occurred_at, evidence_id, identity_snapshot_id,
              input_hash, router_name, router_version, feature_schema_version
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (tenant_id, input_hash, router_name, router_version)
            DO UPDATE SET input_hash = episode_router_runs.input_hash
            RETURNING {', '.join(_RUN_COLUMNS)}
            """,
            uuid7(), tenant_id, perception_outbox_id, observation_id,
            observation_occurred_at, evidence_id, identity_snapshot_id,
            input_hash, router_name, router_version, feature_schema_version,
        )
        assert row is not None
        return _run(row)

    async def finish_run(
        self,
        run_id: UUID,
        *,
        tenant_id: UUID,
        result_hash: str,
        conn: asyncpg.Connection,
    ) -> EpisodeRouterRunRow:
        row = await conn.fetchrow(
            f"""
            UPDATE episode_router_runs
               SET status='completed', result_hash=$3, completed_at=now()
             WHERE id=$1 AND tenant_id=$2 AND status='running'
            RETURNING {', '.join(_RUN_COLUMNS)}
            """,
            run_id, tenant_id, result_hash,
        )
        if row is None:
            existing = await conn.fetchrow(
                f"SELECT {', '.join(_RUN_COLUMNS)} FROM episode_router_runs "
                "WHERE id=$1 AND tenant_id=$2 AND status='completed' AND result_hash=$3",
                run_id, tenant_id, result_hash,
            )
            if existing is None:
                raise ValidationError("episode router run is not active")
            return _run(existing)
        return _run(row)

    async def create_topic_and_episode(
        self,
        signal: RoutingSignal,
        *,
        origin: Literal["automatic", "query_seeded", "human_pinned"] = "automatic",
        query_text: str | None = None,
        requester_actor_id: UUID | None = None,
        router_name: str,
        router_version: str,
        conn: asyncpg.Connection,
    ) -> tuple[EpisodeTopicRow, EpisodeRow]:
        key = topic_key(
            tenant_id=signal.tenant_id, origin=origin, primary_anchor=signal.primary_anchor
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"episode-topic:{signal.tenant_id}:{key}",
        )
        topic_row = await conn.fetchrow(
            f"""
            INSERT INTO episode_topics (
              id, tenant_id, topic_key, origin, label, query_text,
              requester_actor_id, primary_anchor, anchor_refs, claim_predicates,
              lexical_terms, valid_time_start, router_name, router_version
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,
              $12,$13,$14
            ) ON CONFLICT (tenant_id, topic_key)
              DO UPDATE SET topic_key=episode_topics.topic_key
            RETURNING {', '.join(_TOPIC_COLUMNS)}
            """,
            uuid7(), signal.tenant_id, key, origin, signal.topic_label, query_text,
            requester_actor_id, json.dumps(signal.primary_anchor, sort_keys=True),
            json.dumps(signal.anchor_refs, sort_keys=True),
            json.dumps(signal.claim_predicates), json.dumps(signal.lexical_terms),
            signal.occurred_at, router_name, router_version,
        )
        assert topic_row is not None
        topic = _topic(topic_row)
        episode_row = await conn.fetchrow(
            f"""
            INSERT INTO episodes (
              id, tenant_id, topic_id, opened_at, last_event_at, last_ingested_at
            ) VALUES ($1,$2,$3,$4,$4,$5)
            ON CONFLICT (tenant_id, topic_id)
              DO UPDATE SET updated_at=episodes.updated_at
            RETURNING {', '.join(_EPISODE_COLUMNS)}
            """,
            uuid7(), signal.tenant_id, topic.id, signal.occurred_at,
            signal.ingested_at,
        )
        assert episode_row is not None
        episode = _episode(episode_row)
        manifest = {
            "topic_id": str(topic.id), "version": 1,
            "primary_anchor": signal.primary_anchor,
            "anchor_refs": signal.anchor_refs,
            "claim_predicates": signal.claim_predicates,
            "lexical_terms": signal.lexical_terms,
        }
        manifest_hash = hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        await conn.execute(
            """
            INSERT INTO episode_topic_versions (
              id, tenant_id, topic_id, version, primary_anchor, anchor_refs,
              claim_predicates, lexical_terms, manifest_hash
            ) VALUES ($1,$2,$3,1,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8)
            ON CONFLICT (tenant_id, topic_id, version) DO NOTHING
            """,
            uuid7(), signal.tenant_id, topic.id,
            json.dumps(signal.primary_anchor, sort_keys=True),
            json.dumps(signal.anchor_refs, sort_keys=True),
            json.dumps(signal.claim_predicates), json.dumps(signal.lexical_terms),
            manifest_hash,
        )
        return topic, episode

    async def candidates(
        self, *, tenant_id: UUID, conn: asyncpg.Connection
    ) -> list[TopicCandidate]:
        rows = await conn.fetch(
            """
            SELECT t.id AS topic_id, e.id AS episode_id, t.primary_anchor,
                   t.anchor_refs, t.claim_predicates, t.lexical_terms,
                   e.last_event_at
              FROM episode_topics t JOIN episodes e
                ON e.tenant_id=t.tenant_id AND e.topic_id=t.id
             WHERE t.tenant_id=$1 AND t.status='active'
               AND e.lifecycle_state IN ('open','dormant','settled','reopened')
             ORDER BY e.last_event_at DESC, t.id LIMIT 200
            """,
            tenant_id,
        )
        result: list[TopicCandidate] = []
        for row in rows:
            result.append(
                TopicCandidate(
                    topic_id=row["topic_id"], episode_id=row["episode_id"],
                    primary_anchor=_json(row["primary_anchor"]),
                    anchor_refs=tuple(_json(row["anchor_refs"])),
                    claim_predicates=tuple(_json(row["claim_predicates"])),
                    lexical_terms=tuple(_json(row["lexical_terms"])),
                    last_event_at=row["last_event_at"],
                )
            )
        return result

    async def record_membership(
        self,
        *,
        signal: RoutingSignal,
        run: EpisodeRouterRunRow,
        decision: MembershipDecisionValue,
        router_name: str,
        router_version: str,
        feature_schema_version: int,
        conn: asyncpg.Connection,
    ) -> EpisodeMembershipRow:
        semantic = {
            "tenant_id": str(signal.tenant_id), "topic_id": str(decision.topic_id),
            "episode_id": str(decision.episode_id),
            "observation_id": str(signal.observation_id),
            "identity_snapshot_id": str(signal.identity_snapshot_id),
            "decision": decision.decision, "router_name": router_name,
            "router_version": router_version,
            "feature_schema_version": feature_schema_version,
            "feature_snapshot": decision.feature_snapshot,
        }
        key = hashlib.sha256(
            json.dumps(semantic, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        row = await conn.fetchrow(
            f"""
            INSERT INTO episode_membership_assertions (
              id, tenant_id, topic_id, episode_id, router_run_id,
              observation_id, observation_occurred_at, evidence_id,
              identity_snapshot_id, claim_ids, identity_assertion_ids, decision,
              score, reasons, feature_snapshot, router_name, router_version,
              feature_schema_version, decision_key
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15::jsonb,
              $16,$17,$18,$19
            ) ON CONFLICT (tenant_id, decision_key)
              DO UPDATE SET decision_key=episode_membership_assertions.decision_key
            RETURNING {', '.join(_MEMBERSHIP_COLUMNS)}
            """,
            uuid7(), signal.tenant_id, decision.topic_id, decision.episode_id,
            run.id, signal.observation_id, signal.occurred_at, signal.evidence_id,
            signal.identity_snapshot_id, list(signal.claim_ids),
            list(signal.identity_assertion_ids), decision.decision, decision.score,
            json.dumps([reason.model_dump(mode="json") for reason in decision.reasons], sort_keys=True),
            json.dumps(decision.feature_snapshot, sort_keys=True), router_name,
            router_version, feature_schema_version, key,
        )
        assert row is not None
        membership = _membership(row)
        for assertion_id in signal.identity_assertion_ids:
            await conn.execute(
                """
                INSERT INTO identity_dependents (
                  tenant_id, identity_assertion_id, dependent_kind, dependent_id
                ) VALUES ($1,$2,'episode_membership',$3)
                ON CONFLICT DO NOTHING
                """,
                signal.tenant_id, assertion_id, membership.id,
            )
        if decision.decision == "include":
            await conn.execute(
                """
                UPDATE episodes
                   SET last_event_at=greatest(last_event_at,$3),
                       last_ingested_at=greatest(last_ingested_at,$4),
                       lifecycle_state=CASE WHEN lifecycle_state='settled'
                                            THEN 'reopened' ELSE lifecycle_state END,
                       updated_at=now()
                 WHERE id=$1 AND tenant_id=$2
                """,
                decision.episode_id, signal.tenant_id, signal.occurred_at,
                signal.ingested_at,
            )
        return membership

    async def memberships_for_run(
        self, run_id: UUID, *, tenant_id: UUID, conn: asyncpg.Connection
    ) -> list[EpisodeMembershipRow]:
        rows = await conn.fetch(
            f"SELECT {', '.join(_MEMBERSHIP_COLUMNS)} FROM episode_membership_assertions "
            "WHERE tenant_id=$1 AND router_run_id=$2 ORDER BY topic_id, id",
            tenant_id, run_id,
        )
        return [_membership(row) for row in rows]


__all__ = [
    "EpisodeMembershipRow", "EpisodeRoutingRepository", "EpisodeRouterRunRow",
    "EpisodeRow", "EpisodeTopicRow",
]
