"""Structured, manifest-constrained reader for downstream episode reasoning."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.domain.evidence.access import can_actor_read_evidence_set

from .contracts import EpisodeSnapshot, ReasoningEpisodeInput
from .handoff import EpisodeSnapshotOutboxRow


class EpisodeReasoningBatch(BaseModel):
    model_config=ConfigDict(extra="forbid",frozen=True)

    reasoning_input: ReasoningEpisodeInput
    snapshot: EpisodeSnapshot
    observations: tuple[dict[str,Any],...]
    claims: tuple[dict[str,Any],...]
    memberships: tuple[dict[str,Any],...]


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value,(str,bytes,bytearray)) else value


class EpisodeReasoningInputService:
    async def load(
        self,item: EpisodeSnapshotOutboxRow,*,conn: asyncpg.Connection
    ) -> EpisodeReasoningBatch:
        row=await conn.fetchrow(
            "SELECT manifest,snapshot_hash FROM episode_snapshots "
            "WHERE tenant_id=$1 AND id=$2",item.tenant_id,item.episode_snapshot_id,
        )
        if row is None or row["snapshot_hash"] != item.episode_snapshot_hash:
            raise ValueError("reasoning handoff snapshot lineage is stale")
        snapshot=EpisodeSnapshot.model_validate(
            {**_json(row["manifest"]),"snapshot_hash":row["snapshot_hash"]}
        )
        if item.mode == "query_answer":
            assert item.requester_actor_id is not None
            access=await can_actor_read_evidence_set(
                item.requester_actor_id,tenant_id=item.tenant_id,
                evidence_ids=snapshot.evidence_ids,conn=conn,
            )
            if not access.allowed:
                raise ValueError(f"query snapshot access denied: {access.reason}")
        observations=await conn.fetch(
            """
            SELECT o.id AS observation_id,o.occurred_at,o.source_channel,o.actor_id,
                   o.content_text,e.id AS evidence_id,e.source,e.installation_scope,
                   e.source_object_type,e.source_object_id,e.source_revision_id,
                   e.source_recorded_at
              FROM observations o JOIN source_evidence e
                ON e.tenant_id=o.tenant_id AND e.id=o.evidence_id
             WHERE o.tenant_id=$1 AND o.id=ANY($2::uuid[])
             ORDER BY o.occurred_at,o.id
            """,item.tenant_id,list(snapshot.observation_ids),
        )
        claims=[]
        if snapshot.claim_ids:
            claim_rows=await conn.fetch(
                "SELECT id,evidence_id,observation_id,claimant_ref,subject_ref,predicate,"
                "object_value,modality,polarity,confidence,valid_from,valid_to,evidence_span "
                "FROM perception_claims WHERE tenant_id=$1 AND id=ANY($2::uuid[]) "
                "ORDER BY id",item.tenant_id,list(snapshot.claim_ids),
            )
            for claim in claim_rows:
                value=dict(claim)
                for name in ("claimant_ref","subject_ref","object_value","evidence_span"):
                    value[name]=_json(value[name])
                if value["evidence_id"] not in snapshot.evidence_ids:
                    raise ValueError("snapshot claim cites evidence outside its manifest")
                claims.append(value)
        memberships=await conn.fetch(
            "SELECT id,observation_id,evidence_id,claim_ids,identity_assertion_ids,"
            "score,reasons,feature_snapshot FROM episode_membership_assertions "
            "WHERE tenant_id=$1 AND id=ANY($2::uuid[]) ORDER BY created_at,id",
            item.tenant_id,list(snapshot.membership_assertion_ids),
        )
        membership_values=[]
        for membership in memberships:
            value=dict(membership)
            value["claim_ids"]=tuple(value["claim_ids"])
            value["identity_assertion_ids"]=tuple(value["identity_assertion_ids"])
            value["reasons"]=_json(value["reasons"])
            value["feature_snapshot"]=_json(value["feature_snapshot"])
            membership_values.append(value)
        reasoning_input=ReasoningEpisodeInput(
            tenant_id=item.tenant_id,episode_snapshot_id=snapshot.id,
            episode_snapshot_hash=snapshot.snapshot_hash,mode=item.mode,
            requester_actor_id=item.requester_actor_id,query_text=item.query_text,
            authorized_evidence_ids=snapshot.evidence_ids,claim_ids=snapshot.claim_ids,
            contradiction_ids=tuple(value.id for value in snapshot.contradictions),
            created_at=datetime.now(UTC),
        )
        return EpisodeReasoningBatch(
            reasoning_input=reasoning_input,snapshot=snapshot,
            observations=tuple(dict(value) for value in observations),
            claims=tuple(claims),memberships=tuple(membership_values),
        )


__all__=["EpisodeReasoningBatch","EpisodeReasoningInputService"]
