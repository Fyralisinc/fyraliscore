"""Access-aware identity grounding for user queries.

Query resolution creates an auditable snapshot but never creates durable identity
assertions. Only candidates whose supporting evidence the requester can read are
allowed to influence the result.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from services.domain.entity_aliases.repo import normalize_phrase
from services.domain.evidence.access import can_actor_read_evidence_set

from .capabilities import capability_snapshot
from .foundation import EntityMentionCreate, EntityMentionRow, ResolutionRunCreate
from .foundation_repo import EntityMentionRepository, ResolutionRunRepository
from .resolution import CandidateSeed, IdentityResolutionSnapshot
from .resolution_repo import CandidateProvider, IdentityResolutionRepository
from .service import IdentityResolutionService


_QUOTED = re.compile(r"[\"']([^\"']{2,120})[\"']")
_NAMED = re.compile(r"\b(?:[A-Z][\w-]*)(?:\s+[A-Z][\w-]*){0,3}\b")
_QUESTION_WORDS = {"what", "who", "when", "where", "why", "how", "which"}


def extract_query_mentions(query: str) -> tuple[str, ...]:
    """Return conservative identity-shaped phrases; the full query remains a topic seed."""

    values = [match.group(1).strip() for match in _QUOTED.finditer(query)]
    values.extend(match.group(0).strip() for match in _NAMED.finditer(query))
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_phrase(value)
        if normalized in _QUESTION_WORDS:
            continue
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(value)
    return tuple(result[:20])


class AccessAwareQueryCandidateProvider(CandidateProvider):
    """Retrieve candidates only from tenant actors or readable accepted assertions."""

    def __init__(self, requester_actor_id: UUID) -> None:
        self._requester_actor_id = requester_actor_id

    async def candidates_for(
        self,
        mention: EntityMentionRow,
        *,
        tenant_id: UUID,
        conn: asyncpg.Connection,
    ) -> list[CandidateSeed]:
        normalized = normalize_phrase(mention.text)
        seeds: list[CandidateSeed] = []
        actor_rows = await conn.fetch(
            """
            SELECT id, display_name,
                   similarity(display_name, $2) AS similarity
              FROM actors
             WHERE tenant_id = $1 AND status = 'active'
               AND similarity(display_name, $2) >= 0.25
             ORDER BY similarity DESC, id LIMIT 10
            """,
            tenant_id,
            mention.text,
        )
        for row in actor_rows:
            exact = normalize_phrase(str(row["display_name"])) == normalized
            seeds.append(
                CandidateSeed(
                    candidate_ref={"type": "actor", "id": str(row["id"])},
                    retrieval_method="actor_name",
                    expected_type="person",
                    features={
                        "exact_alias": 1.0 if exact else 0.0,
                        "name_similarity": float(row["similarity"]),
                        "type_compatibility": 1.0,
                    },
                )
            )

        assertion_rows = await conn.fetch(
            """
            SELECT candidate_entity_ref, confidence, evidence_id,
                   source_identity_ref->>'text' AS source_text
              FROM identity_assertions
             WHERE tenant_id = $1 AND status = 'accepted'
               AND evidence_id IS NOT NULL
               AND source_identity_ref ? 'text'
               AND (
                 regexp_replace(lower(source_identity_ref->>'text'), '\\s+', ' ', 'g') = $2
                 OR similarity(source_identity_ref->>'text', $3) >= 0.25
               )
             ORDER BY confidence DESC, id LIMIT 30
            """,
            tenant_id,
            normalized,
            mention.text,
        )
        for row in assertion_rows:
            decision = await can_actor_read_evidence_set(
                self._requester_actor_id,
                tenant_id=tenant_id,
                evidence_ids=[row["evidence_id"]],
                conn=conn,
            )
            if not decision.allowed:
                continue
            candidate_ref = row["candidate_entity_ref"]
            if isinstance(candidate_ref, str):
                candidate_ref = json.loads(candidate_ref)
            exact = normalize_phrase(str(row["source_text"])) == normalized
            seeds.append(
                CandidateSeed(
                    candidate_ref=candidate_ref,
                    retrieval_method="exact_alias" if exact else "fuzzy_alias",
                    features={
                        "exact_alias": 1.0 if exact else 0.0,
                        "alias_confidence": float(row["confidence"]),
                        "name_similarity": 1.0 if exact else 0.7,
                        "type_compatibility": 1.0,
                    },
                )
            )
        return seeds


@dataclass(frozen=True)
class QueryResolutionResult:
    snapshot: IdentityResolutionSnapshot
    topic_seed: str
    mention_texts: tuple[str, ...]


class QueryIdentityResolutionService:
    async def resolve_query(
        self,
        query: str,
        *,
        tenant_id: UUID,
        requester_actor_id: UUID,
        conn: asyncpg.Connection,
        mention_texts: tuple[str, ...] | None = None,
    ) -> QueryResolutionResult:
        text = query.strip()
        if not text:
            raise ValueError("query must be non-empty")
        phrases = mention_texts if mention_texts is not None else extract_query_mentions(text)
        input_hash = hashlib.sha256(
            json.dumps(
                {
                    "tenant_id": str(tenant_id),
                    "requester_actor_id": str(requester_actor_id),
                    "query": text,
                    "mentions": phrases,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        runs = ResolutionRunRepository()
        repository = IdentityResolutionRepository()
        run = await runs.start(
            ResolutionRunCreate(
                tenant_id=tenant_id,
                input_kind="query",
                requester_actor_id=requester_actor_id,
                input_hash=input_hash,
                resolver_name=IdentityResolutionService.resolver_name,
                resolver_version=IdentityResolutionService.resolver_version,
                policy_version=IdentityResolutionService.policy_version,
                capability_snapshot=capability_snapshot(),
            ),
            conn=conn,
        )
        if run.status == "completed":
            existing = await repository.snapshot_for_run(
                tenant_id=tenant_id, resolver_run_id=run.id, conn=conn
            )
            if existing is not None:
                return QueryResolutionResult(existing, text, phrases)

        mention_repo = EntityMentionRepository()
        mentions = [
            await mention_repo.register(
                EntityMentionCreate(
                    tenant_id=tenant_id,
                    mention_kind="query",
                    text=phrase,
                    context={"query_input_hash": input_hash},
                ),
                conn=conn,
            )
            for phrase in phrases
        ]
        service = IdentityResolutionService(
            candidates=AccessAwareQueryCandidateProvider(requester_actor_id)
        )
        snapshot = await service.resolve(
            run=run,
            mentions=mentions,
            access_policy_hash=None,
            persist_assertions=False,
            conn=conn,
        )
        await runs.finish(
            run.id,
            tenant_id=tenant_id,
            status="completed",
            result_hash=snapshot.snapshot_hash,
            conn=conn,
        )
        return QueryResolutionResult(snapshot, text, phrases)


__all__ = [
    "AccessAwareQueryCandidateProvider",
    "QueryIdentityResolutionService",
    "QueryResolutionResult",
    "extract_query_mentions",
]
