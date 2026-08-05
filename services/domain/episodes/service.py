"""Application service for deterministic topic routing and membership creation."""

from __future__ import annotations

import hashlib
import json

import asyncpg

from .assembler import EpisodeSignalAssembler
from .contracts import MembershipReason
from .intake import PerceptionOutboxRow
from .repo import EpisodeMembershipRow, EpisodeRoutingRepository
from .routing import MembershipDecisionValue, score_membership


class EpisodeRoutingService:
    router_name = "fyralis-episode-router"
    router_version = "1.0.0"
    feature_schema_version = 1

    def __init__(
        self,
        *,
        assembler: EpisodeSignalAssembler | None = None,
        repository: EpisodeRoutingRepository | None = None,
    ) -> None:
        self._assembler = assembler or EpisodeSignalAssembler()
        self._repo = repository or EpisodeRoutingRepository()

    async def route(
        self, item: PerceptionOutboxRow, *, conn: asyncpg.Connection
    ) -> list[EpisodeMembershipRow]:
        input_hash = hashlib.sha256(
            json.dumps(
                {
                    "tenant_id": str(item.tenant_id),
                    "outbox_id": str(item.id),
                    "observation_id": str(item.observation_id),
                    "evidence_id": str(item.evidence_id),
                    "identity_snapshot_hash": item.identity_snapshot_hash,
                    "router_version": self.router_version,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        run = await self._repo.start_run(
            tenant_id=item.tenant_id,
            perception_outbox_id=item.id,
            observation_id=item.observation_id,
            observation_occurred_at=item.observation_occurred_at,
            evidence_id=item.evidence_id,
            identity_snapshot_id=item.identity_snapshot_id,
            input_hash=input_hash,
            router_name=self.router_name,
            router_version=self.router_version,
            feature_schema_version=self.feature_schema_version,
            conn=conn,
        )
        if run.status == "completed":
            return await self._repo.memberships_for_run(
                run.id, tenant_id=item.tenant_id, conn=conn
            )

        signal = await self._assembler.assemble(item, conn=conn)
        candidates = await self._repo.candidates(tenant_id=item.tenant_id, conn=conn)
        decisions = [score_membership(signal, candidate) for candidate in candidates]
        includes = [value for value in decisions if value.decision == "include"]
        selected: list[MembershipDecisionValue]
        if includes:
            selected = includes + [
                value for value in decisions
                if value.decision == "hold"
            ][:5]
            selected += sorted(
                (value for value in decisions if value.decision == "exclude"),
                key=lambda value: -value.score,
            )[:3]
        else:
            topic, episode = await self._repo.create_topic_and_episode(
                signal,
                router_name=self.router_name,
                router_version=self.router_version,
                conn=conn,
            )
            selected = [
                MembershipDecisionValue(
                    topic_id=topic.id,
                    episode_id=episode.id,
                    decision="include",
                    score=1.0,
                    reasons=(
                        MembershipReason(
                            code="entity_overlap",
                            weight=1.0,
                            detail={"reason": "topic_seed", "anchor": signal.primary_anchor},
                        ),
                    ),
                    feature_snapshot={
                        "topic_seed": True,
                        "primary_anchor": signal.primary_anchor,
                        "source": signal.source,
                        "installation_scope": signal.installation_scope,
                    },
                )
            ]
            selected += [value for value in decisions if value.decision == "hold"][:5]
            selected += sorted(
                (value for value in decisions if value.decision == "exclude"),
                key=lambda value: -value.score,
            )[:3]

        memberships = [
            await self._repo.record_membership(
                signal=signal,
                run=run,
                decision=decision,
                router_name=self.router_name,
                router_version=self.router_version,
                feature_schema_version=self.feature_schema_version,
                conn=conn,
            )
            for decision in selected
        ]
        result_hash = hashlib.sha256(
            json.dumps(
                [
                    {
                        "decision_key": value.decision_key,
                        "decision": value.decision,
                        "score": value.score,
                    }
                    for value in memberships
                ],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        await self._repo.finish_run(
            run.id, tenant_id=item.tenant_id, result_hash=result_hash, conn=conn
        )
        return memberships


__all__ = ["EpisodeRoutingService"]
