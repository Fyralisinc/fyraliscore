"""Requester-authorized query topics constructed from automatic episodes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.domain.evidence.access import can_actor_read_evidence_set

from .assembler import EpisodeSignalAssembler
from .construction import EpisodeConstructionService
from .contracts import EpisodeSnapshot, MembershipReason
from .intake import PerceptionOutboxRow
from .handoff import EpisodeSnapshotOutboxRepository
from .repo import EpisodeRoutingRepository
from .routing import MembershipDecisionValue, canonical_ref, lexical_terms


@dataclass(frozen=True)
class QueryEpisodeResult:
    topic_id: UUID
    episode_id: UUID
    equivalent_topic_ids: tuple[UUID, ...]
    snapshot: EpisodeSnapshot


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value


class QueryEpisodeService:
    router_name = "fyralis-query-episode-router"
    router_version = "1.0.0"
    feature_schema_version = 1

    def __init__(self) -> None:
        self._repo = EpisodeRoutingRepository()
        self._assembler = EpisodeSignalAssembler()
        self._construction = EpisodeConstructionService()

    async def construct(
        self,
        query: str,
        *,
        tenant_id: UUID,
        requester_actor_id: UUID,
        seed_anchor_refs: tuple[dict[str, Any], ...] = (),
        valid_time_start: datetime | None = None,
        valid_time_end: datetime | None = None,
        conn: asyncpg.Connection,
    ) -> QueryEpisodeResult:
        text = query.strip()
        if not text:
            raise ValueError("query must be non-empty")
        requester_tenant = await conn.fetchval(
            "SELECT tenant_id FROM actors WHERE id=$1", requester_actor_id
        )
        if requester_tenant != tenant_id:
            raise ValueError("query requester is not in the tenant")
        scope_hash = hashlib.sha256(
            json.dumps(
                {
                    "tenant_id": str(tenant_id), "requester": str(requester_actor_id),
                    "query": text, "seed_anchor_refs": seed_anchor_refs,
                    "valid_time_start": valid_time_start, "valid_time_end": valid_time_end,
                },
                sort_keys=True, separators=(",", ":"), default=str,
            ).encode()
        ).hexdigest()
        topic_id, episode_id, existing_snapshot = await self._create_query_topic(
            tenant_id=tenant_id, requester_actor_id=requester_actor_id, query=text,
            scope_hash=scope_hash, seed_anchor_refs=seed_anchor_refs,
            valid_time_start=valid_time_start, valid_time_end=valid_time_end, conn=conn,
        )
        if existing_snapshot is not None:
            await EpisodeSnapshotOutboxRepository().enqueue(existing_snapshot, conn=conn)
            return QueryEpisodeResult(topic_id, episode_id, (), existing_snapshot)

        automatic_topics = await conn.fetch(
            """
            SELECT t.id,t.anchor_refs,t.lexical_terms,e.id AS episode_id
              FROM episode_topics t JOIN episodes e
                ON e.tenant_id=t.tenant_id AND e.topic_id=t.id
             WHERE t.tenant_id=$1 AND t.origin <> 'query_seeded' AND t.status='active'
               AND e.lifecycle_state <> 'superseded'
             ORDER BY e.last_event_at DESC,t.id
            """,
            tenant_id,
        )
        query_refs = {canonical_ref(value) for value in seed_anchor_refs}
        query_terms = set(lexical_terms(text))
        equivalents: list[asyncpg.Record] = []
        for topic in automatic_topics:
            topic_refs = {canonical_ref(value) for value in _json(topic["anchor_refs"])}
            topic_terms = set(_json(topic["lexical_terms"]))
            shared_refs = query_refs.intersection(topic_refs)
            shared_terms = query_terms.intersection(topic_terms)
            lexical_ratio = len(shared_terms) / len(query_terms.union(topic_terms)) if query_terms and topic_terms else 0
            if shared_refs or (len(shared_terms) >= 2 and lexical_ratio >= 0.12):
                await self._record_equivalence(
                    left_topic_id=topic_id, right_topic_id=topic["id"],
                    tenant_id=tenant_id,
                    provenance={
                        "producer": self.router_name, "version": self.router_version,
                        "shared_anchor_refs": sorted(shared_refs),
                        "shared_lexical_terms": sorted(shared_terms),
                        "lexical_ratio": lexical_ratio,
                    },
                    conn=conn,
                )
                equivalents.append(topic)

        for source_topic in equivalents:
            source_memberships = await conn.fetch(
                """
                WITH latest AS (
                  SELECT DISTINCT ON (m.observation_id) m.*
                    FROM episode_membership_assertions m
                   WHERE m.tenant_id=$1 AND m.episode_id=$2 AND m.status='accepted'
                   ORDER BY m.observation_id,m.created_at DESC,m.id DESC
                )
                SELECT l.*,p.id AS outbox_id,p.event_kind,p.aggregate_type,p.aggregate_id,
                       p.identity_snapshot_hash,p.identity_resolution_status,
                       p.knowledge_snapshot_id,p.knowledge_snapshot_hash,p.claim_set_hash,
                       p.contract_version,p.dedupe_key,p.payload,p.status AS outbox_status,
                       p.available_at,p.attempt_count,p.lease_owner,p.lease_expires_at,
                       p.last_error,p.completed_at,p.created_at AS outbox_created_at,
                       p.updated_at AS outbox_updated_at
                  FROM latest l JOIN perception_outbox p
                   ON p.tenant_id=l.tenant_id AND p.observation_id=l.observation_id
                   AND p.identity_snapshot_id=l.identity_snapshot_id
                   AND p.knowledge_snapshot_id=l.knowledge_snapshot_id
                 WHERE l.decision='include'
                 ORDER BY l.observation_occurred_at,l.observation_id
                """,
                tenant_id, source_topic["episode_id"],
            )
            for source in source_memberships:
                access = await can_actor_read_evidence_set(
                    requester_actor_id, tenant_id=tenant_id,
                    evidence_ids=[source["evidence_id"]], conn=conn,
                )
                if not access.allowed:
                    continue
                item = PerceptionOutboxRow.model_validate(
                    {
                        "id": source["outbox_id"], "tenant_id": tenant_id,
                        "event_kind": source["event_kind"],
                        "aggregate_type": source["aggregate_type"],
                        "aggregate_id": source["aggregate_id"],
                        "observation_id": source["observation_id"],
                        "observation_occurred_at": source["observation_occurred_at"],
                        "evidence_id": source["evidence_id"],
                        "identity_snapshot_id": source["identity_snapshot_id"],
                        "identity_snapshot_hash": source["identity_snapshot_hash"],
                        "identity_resolution_status": source["identity_resolution_status"],
                        "knowledge_snapshot_id": source["knowledge_snapshot_id"],
                        "knowledge_snapshot_hash": source["knowledge_snapshot_hash"],
                        "claim_set_hash": source["claim_set_hash"],
                        "contract_version": source["contract_version"],
                        "dedupe_key": source["dedupe_key"],
                        "payload": _json(source["payload"]),
                        "status": source["outbox_status"],
                        "available_at": source["available_at"],
                        "attempt_count": source["attempt_count"],
                        "lease_owner": source["lease_owner"],
                        "lease_expires_at": source["lease_expires_at"],
                        "last_error": source["last_error"],
                        "completed_at": source["completed_at"],
                        "created_at": source["outbox_created_at"],
                        "updated_at": source["outbox_updated_at"],
                    }
                )
                signal = await self._assembler.assemble(item, conn=conn)
                run_hash = hashlib.sha256(
                    f"query:{scope_hash}:{source['id']}:{self.router_version}".encode()
                ).hexdigest()
                run = await self._repo.start_run(
                    tenant_id=tenant_id, perception_outbox_id=item.id,
                    observation_id=item.observation_id,
                    observation_occurred_at=item.observation_occurred_at,
                    evidence_id=item.evidence_id,
                    identity_snapshot_id=item.identity_snapshot_id,
                    knowledge_snapshot_id=item.knowledge_snapshot_id,
                    knowledge_snapshot_hash=item.knowledge_snapshot_hash,
                    claim_set_hash=item.claim_set_hash,
                    input_hash=run_hash, router_name=self.router_name,
                    router_version=self.router_version,
                    feature_schema_version=self.feature_schema_version, conn=conn,
                )
                decision = MembershipDecisionValue(
                    topic_id=topic_id, episode_id=episode_id, decision="include", score=1.0,
                    reasons=(
                        MembershipReason(
                            code="query_match", weight=1.0,
                            detail={"source_topic_id": str(source_topic["id"])},
                        ),
                    ),
                    feature_snapshot={
                        "query_scope_hash": scope_hash,
                        "equivalent_topic_id": str(source_topic["id"]),
                        "access_decision": access.reason,
                    },
                )
                membership = await self._repo.record_membership(
                    signal=signal, run=run, decision=decision,
                    router_name=self.router_name, router_version=self.router_version,
                    feature_schema_version=self.feature_schema_version, conn=conn,
                )
                await self._repo.finish_run(
                    run.id, tenant_id=tenant_id,
                    result_hash=hashlib.sha256(membership.decision_key.encode()).hexdigest(),
                    conn=conn,
                )

        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM episode_membership_assertions "
            "WHERE tenant_id=$1 AND episode_id=$2 AND decision='include' AND status='accepted')",
            tenant_id, episode_id,
        ):
            raise ValueError("no authorized evidence matched the query episode")
        snapshot = await self._construction.settle(
            episode_id, tenant_id=tenant_id, reason="query_scope_satisfied", conn=conn
        )
        return QueryEpisodeResult(
            topic_id, episode_id, tuple(topic["id"] for topic in equivalents), snapshot
        )

    async def _create_query_topic(
        self,
        *,
        tenant_id: UUID,
        requester_actor_id: UUID,
        query: str,
        scope_hash: str,
        seed_anchor_refs: tuple[dict[str, Any], ...],
        valid_time_start: datetime | None,
        valid_time_end: datetime | None,
        conn: asyncpg.Connection,
    ) -> tuple[UUID, UUID, EpisodeSnapshot | None]:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"query-episode:{tenant_id}:{scope_hash}",
        )
        primary = {"type": "query_scope", "id": scope_hash}
        now = datetime.now(UTC)
        topic = await conn.fetchrow(
            """
            INSERT INTO episode_topics (
              id,tenant_id,topic_key,origin,label,query_text,requester_actor_id,
              primary_anchor,anchor_refs,claim_predicates,lexical_terms,
              valid_time_start,valid_time_end,router_name,router_version
            ) VALUES ($1,$2,$3,'query_seeded',$4,$4,$5,$6::jsonb,$7::jsonb,
                      '[]'::jsonb,$8::jsonb,$9,$10,$11,$12)
            ON CONFLICT (tenant_id,topic_key)
              DO UPDATE SET topic_key=episode_topics.topic_key
            RETURNING id,head_version
            """,
            uuid7(), tenant_id, scope_hash, query, requester_actor_id,
            json.dumps(primary), json.dumps(seed_anchor_refs, sort_keys=True),
            json.dumps(lexical_terms(query)), valid_time_start, valid_time_end,
            self.router_name, self.router_version,
        )
        topic_manifest = {
            "topic_id": str(topic["id"]), "version": 1,
            "primary_anchor": primary, "anchor_refs": seed_anchor_refs,
            "claim_predicates": (), "lexical_terms": lexical_terms(query),
        }
        topic_manifest_hash = hashlib.sha256(
            json.dumps(topic_manifest, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        await conn.execute(
            """
            INSERT INTO episode_topic_versions (
              id,tenant_id,topic_id,version,primary_anchor,anchor_refs,
              claim_predicates,lexical_terms,manifest_hash
            ) VALUES ($1,$2,$3,1,$4::jsonb,$5::jsonb,'[]'::jsonb,$6::jsonb,$7)
            ON CONFLICT (tenant_id,topic_id,version) DO NOTHING
            """,
            uuid7(), tenant_id, topic["id"], json.dumps(primary),
            json.dumps(seed_anchor_refs, sort_keys=True), json.dumps(lexical_terms(query)),
            topic_manifest_hash,
        )
        episode = await conn.fetchrow(
            """
            INSERT INTO episodes (id,tenant_id,topic_id,opened_at,last_event_at,last_ingested_at)
            VALUES ($1,$2,$3,$4,$4,$4)
            ON CONFLICT (tenant_id,topic_id) DO UPDATE SET updated_at=episodes.updated_at
            RETURNING id,lifecycle_state,head_version
            """,
            uuid7(), tenant_id, topic["id"], now,
        )
        if int(episode["head_version"]) > 0 and episode["lifecycle_state"] == "settled":
            row = await conn.fetchrow(
                "SELECT manifest,snapshot_hash FROM episode_snapshots "
                "WHERE tenant_id=$1 AND episode_id=$2 ORDER BY version DESC LIMIT 1",
                tenant_id, episode["id"],
            )
            if row is not None:
                return topic["id"], episode["id"], EpisodeSnapshot.model_validate(
                    {**_json(row["manifest"]), "snapshot_hash": row["snapshot_hash"]}
                )
        return topic["id"], episode["id"], None

    async def _record_equivalence(
        self,
        *,
        left_topic_id: UUID,
        right_topic_id: UUID,
        tenant_id: UUID,
        provenance: dict[str, Any],
        conn: asyncpg.Connection,
    ) -> None:
        left, right = sorted((left_topic_id, right_topic_id), key=str)
        await conn.execute(
            """
            INSERT INTO episode_topic_equivalences (
              id,tenant_id,left_topic_id,right_topic_id,decision,authority,provenance
            ) VALUES ($1,$2,$3,$4,'equivalent','system',$5::jsonb)
            ON CONFLICT (tenant_id,left_topic_id,right_topic_id) DO NOTHING
            """,
            uuid7(), tenant_id, left, right, json.dumps(provenance, sort_keys=True),
        )


__all__ = ["QueryEpisodeResult", "QueryEpisodeService"]
