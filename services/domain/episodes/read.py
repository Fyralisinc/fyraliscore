"""Read, replay, explain, and diff immutable episode history."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from .contracts import EpisodeSnapshot


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value,(str,bytes,bytearray)) else value


@dataclass(frozen=True)
class EpisodeSnapshotDiff:
    from_snapshot_id: UUID
    to_snapshot_id: UUID
    added_observation_ids: tuple[UUID,...]
    removed_observation_ids: tuple[UUID,...]
    added_claim_ids: tuple[UUID,...]
    removed_claim_ids: tuple[UUID,...]
    added_contradiction_ids: tuple[UUID,...]
    removed_contradiction_ids: tuple[UUID,...]


class EpisodeReadService:
    async def snapshot(
        self,snapshot_id: UUID,*,tenant_id: UUID,conn: asyncpg.Connection
    ) -> EpisodeSnapshot | None:
        row=await conn.fetchrow(
            "SELECT manifest,snapshot_hash FROM episode_snapshots "
            "WHERE id=$1 AND tenant_id=$2",snapshot_id,tenant_id,
        )
        if row is None:
            return None
        return EpisodeSnapshot.model_validate(
            {**_json(row["manifest"]),"snapshot_hash":row["snapshot_hash"]}
        )

    async def history(
        self,episode_id: UUID,*,tenant_id: UUID,conn: asyncpg.Connection
    ) -> list[EpisodeSnapshot]:
        rows=await conn.fetch(
            "SELECT manifest,snapshot_hash FROM episode_snapshots "
            "WHERE episode_id=$1 AND tenant_id=$2 ORDER BY version",episode_id,tenant_id,
        )
        return [EpisodeSnapshot.model_validate(
            {**_json(row["manifest"]),"snapshot_hash":row["snapshot_hash"]}
        ) for row in rows]

    async def memberships(
        self,episode_id: UUID,*,tenant_id: UUID,conn: asyncpg.Connection
    ) -> list[dict[str,Any]]:
        rows=await conn.fetch(
            """
            SELECT id,observation_id,evidence_id,decision,score,reasons,
                   feature_snapshot,router_name,router_version,created_at
              FROM episode_membership_assertions
             WHERE episode_id=$1 AND tenant_id=$2
             ORDER BY created_at,id
            """,episode_id,tenant_id,
        )
        result=[]
        for row in rows:
            value=dict(row)
            value["reasons"]=_json(value["reasons"])
            value["feature_snapshot"]=_json(value["feature_snapshot"])
            result.append(value)
        return result

    async def citations(
        self,snapshot_id: UUID,*,tenant_id: UUID,conn: asyncpg.Connection
    ) -> list[dict[str,Any]]:
        rows=await conn.fetch(
            """
            SELECT m.observation_id,m.evidence_id,o.occurred_at,o.source_channel,
                   o.content_text,e.source,e.installation_scope,e.source_object_type,
                   e.source_object_id,e.source_revision_id,e.source_recorded_at
              FROM episode_snapshot_memberships m
              JOIN observations o ON o.id=m.observation_id AND o.tenant_id=m.tenant_id
              JOIN source_evidence e ON e.id=m.evidence_id AND e.tenant_id=m.tenant_id
             WHERE m.tenant_id=$1 AND m.snapshot_id=$2
             ORDER BY o.occurred_at,o.id
            """,tenant_id,snapshot_id,
        )
        return [dict(row) for row in rows]

    async def contradictions(
        self,episode_id: UUID,*,tenant_id: UUID,conn: asyncpg.Connection
    ) -> list[dict[str,Any]]:
        rows=await conn.fetch(
            "SELECT id,left_claim_id,right_claim_id,contradiction_kind,status,"
            "explanation,detector_name,detector_version,created_at "
            "FROM episode_contradictions WHERE tenant_id=$1 AND episode_id=$2 "
            "ORDER BY created_at,id",tenant_id,episode_id,
        )
        return [dict(row) for row in rows]

    async def diff(
        self,from_snapshot_id: UUID,to_snapshot_id: UUID,*,tenant_id: UUID,
        conn: asyncpg.Connection,
    ) -> EpisodeSnapshotDiff:
        left=await self.snapshot(from_snapshot_id,tenant_id=tenant_id,conn=conn)
        right=await self.snapshot(to_snapshot_id,tenant_id=tenant_id,conn=conn)
        if left is None or right is None:
            raise ValueError("episode snapshot not found")
        if left.episode_id != right.episode_id:
            raise ValueError("cannot diff snapshots from different episodes")
        def delta(a,b):
            return tuple(sorted(set(b).difference(a),key=str))
        left_contra=tuple(value.id for value in left.contradictions)
        right_contra=tuple(value.id for value in right.contradictions)
        return EpisodeSnapshotDiff(
            from_snapshot_id=left.id,to_snapshot_id=right.id,
            added_observation_ids=delta(left.observation_ids,right.observation_ids),
            removed_observation_ids=delta(right.observation_ids,left.observation_ids),
            added_claim_ids=delta(left.claim_ids,right.claim_ids),
            removed_claim_ids=delta(right.claim_ids,left.claim_ids),
            added_contradiction_ids=delta(left_contra,right_contra),
            removed_contradiction_ids=delta(right_contra,left_contra),
        )


__all__=["EpisodeReadService","EpisodeSnapshotDiff"]
