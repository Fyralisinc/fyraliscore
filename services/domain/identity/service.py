"""Application service that resolves all mentions and seals one snapshot."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7

from .foundation import EntityMentionRow, ResolutionRunRow
from .models import IdentityAssertionCreate
from .repo import IdentityAssertionRepository
from .resolution import (
    IdentityResolutionSnapshot,
    IdentitySnapshotItem,
    decide_resolution,
    rank_candidates,
)
from .resolution_repo import (
    CandidateProvider,
    IdentityResolutionRepository,
    PostgresCandidateProvider,
)


class IdentityResolutionService:
    resolver_name = "fyralis-identity"
    resolver_version = "1.0.0"
    policy_version = "source-grounded-v1"

    def __init__(
        self,
        *,
        candidates: CandidateProvider | None = None,
        repository: IdentityResolutionRepository | None = None,
        assertions: IdentityAssertionRepository | None = None,
    ) -> None:
        self._candidates = candidates or PostgresCandidateProvider()
        self._repository = repository or IdentityResolutionRepository()
        self._assertions = assertions or IdentityAssertionRepository()

    async def resolve(
        self,
        *,
        run: ResolutionRunRow,
        mentions: list[EntityMentionRow],
        access_policy_hash: str | None,
        conn: asyncpg.Connection,
        evaluated_at: datetime | None = None,
        persist_assertions: bool = True,
    ) -> IdentityResolutionSnapshot:
        now = evaluated_at or datetime.now(UTC)
        constraints = await self._repository.active_constraints(
            tenant_id=run.tenant_id, conn=conn
        )
        items: list[IdentitySnapshotItem] = []
        assertion_ids: list[UUID] = []
        for mention in mentions:
            seeds = await self._candidates.candidates_for(
                mention, tenant_id=run.tenant_id, conn=conn
            )
            ranked = rank_candidates(
                mention, seeds, constraints=constraints, evaluated_at=now
            )
            await self._repository.record_candidates(
                tenant_id=run.tenant_id,
                resolver_run_id=run.id,
                mention_id=mention.id,
                candidates=ranked,
                conn=conn,
            )
            decision = decide_resolution(mention, ranked)
            assertion_id: UUID | None = None
            if (
                persist_assertions
                and decision.outcome == "resolved"
                and decision.selected_ref is not None
            ):
                top = next(
                    item for item in ranked if item.candidate_ref == decision.selected_ref
                )
                assertion = await self._assertions.propose(
                    IdentityAssertionCreate(
                        tenant_id=run.tenant_id,
                        source_identity_key=f"mention:{mention.mention_key}",
                        source_identity_ref={
                            "kind": "mention",
                            "id": str(mention.id),
                            "text": mention.text,
                        },
                        candidate_entity_ref=decision.selected_ref,
                        assertion_kind="refers_to",
                        confidence=decision.confidence,
                        evidence_id=mention.evidence_id,
                        mention_id=mention.id,
                        resolver_run_id=run.id,
                        score_components=top.features,
                        scope={
                            "input_kind": run.input_kind,
                            "observation_id": (
                                str(run.observation_id) if run.observation_id else None
                            ),
                        },
                        access_policy_hash=access_policy_hash,
                        decision_provenance={
                            "producer": self.resolver_name,
                            "resolver_version": self.resolver_version,
                            "policy_version": self.policy_version,
                            "outcome": decision.outcome,
                        },
                        valid_from=mention.observation_occurred_at,
                    ),
                    conn=conn,
                )
                if decision.outcome == "resolved" and assertion.status == "proposed":
                    assertion = await self._assertions.decide(
                        assertion.id,
                        tenant_id=run.tenant_id,
                        decision="accepted",
                        provenance={
                            "decider": "system:source-grounded-v1",
                            "reason": "automatic_resolution_policy_passed",
                        },
                        conn=conn,
                    )
                assertion_id = assertion.id
                assertion_ids.append(assertion.id)
                if run.observation_id is not None:
                    await self._assertions.register_dependent(
                        assertion.id,
                        tenant_id=run.tenant_id,
                        dependent_kind="observation",
                        dependent_id=run.observation_id,
                        conn=conn,
                    )
            items.append(
                IdentitySnapshotItem(
                    mention_id=mention.id,
                    outcome=decision.outcome,
                    selected_ref=decision.selected_ref,
                    confidence=decision.confidence,
                    assertion_id=assertion_id,
                    alternatives=decision.alternatives,
                    reasons=decision.reasons,
                )
            )

        status = (
            "complete" if items and all(item.outcome == "resolved" for item in items) else "partial"
        )
        snapshot = IdentityResolutionSnapshot.seal(
            id=uuid7(),
            tenant_id=run.tenant_id,
            resolver_run_id=run.id,
            input_kind=run.input_kind,
            observation_id=run.observation_id,
            observation_occurred_at=run.observation_occurred_at,
            requester_actor_id=run.requester_actor_id,
            resolution_status=status,
            items=tuple(items),
            access_policy_hash=access_policy_hash,
            resolver_name=self.resolver_name,
            resolver_version=self.resolver_version,
            policy_version=self.policy_version,
            created_at=now,
        )
        return await self._repository.persist_snapshot(
            snapshot, assertion_ids=assertion_ids, conn=conn
        )


__all__ = ["IdentityResolutionService"]
