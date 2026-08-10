"""Contradiction-preserving, access-safe immutable episode snapshots."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.domain.evidence.access import compose_access_policies, policy_hash

from .contracts import (
    EpisodeAccessManifest,
    EpisodeContradiction,
    EpisodeCoverage,
    EpisodeSettlement,
    EpisodeSnapshot,
)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value


class EpisodeSnapshotService:
    detector_name = "fyralis-claim-contradictions"
    detector_version = "1.0.0"
    access_composition_version = "intersection-v1"

    async def seal(
        self,
        episode_id: UUID,
        *,
        tenant_id: UUID,
        settlement: EpisodeSettlement | None = None,
        conn: asyncpg.Connection,
        created_at: datetime | None = None,
    ) -> EpisodeSnapshot:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"episode-snapshot:{tenant_id}:{episode_id}",
        )
        episode = await conn.fetchrow(
            "SELECT * FROM episodes WHERE id=$1 AND tenant_id=$2 FOR UPDATE",
            episode_id, tenant_id,
        )
        if episode is None:
            raise ValueError("episode not found")
        membership_rows = await conn.fetch(
            """
            WITH latest AS (
              SELECT DISTINCT ON (observation_id)
                     id, observation_id, observation_occurred_at, evidence_id,
                     claim_ids, created_at, decision
                FROM episode_membership_assertions
               WHERE tenant_id=$1 AND episode_id=$2 AND status='accepted'
               ORDER BY observation_id, created_at DESC, id DESC
            )
            SELECT latest.*,o.ingested_at
              FROM latest JOIN observations o
                ON o.id=latest.observation_id
               AND o.occurred_at=latest.observation_occurred_at
             WHERE decision='include'
             ORDER BY observation_occurred_at, observation_id
            """,
            tenant_id, episode_id,
        )
        if not membership_rows:
            raise ValueError("cannot snapshot an episode without included observations")
        membership_ids = tuple(row["id"] for row in membership_rows)
        observation_ids = tuple(row["observation_id"] for row in membership_rows)
        evidence_ids = tuple(row["evidence_id"] for row in membership_rows)
        claim_ids = tuple(
            sorted({claim_id for row in membership_rows for claim_id in row["claim_ids"]}, key=str)
        )
        policies = await conn.fetch(
            "SELECT id, access_policy, access_policy_hash FROM source_evidence "
            "WHERE tenant_id=$1 AND id=ANY($2::uuid[])",
            tenant_id, list(evidence_ids),
        )
        if len(policies) != len(evidence_ids):
            raise ValueError("snapshot evidence access lineage is incomplete")
        policy_values = [_json(row["access_policy"]) for row in policies]
        composed = compose_access_policies(policy_values)
        input_policy_hashes = tuple(
            sorted(str(row["access_policy_hash"] or policy_hash(_json(row["access_policy"]))) for row in policies)
        )
        access = EpisodeAccessManifest(
            visibility=composed["visibility"],
            audience=tuple(composed.get("audience", [])),
            policy_hash=composed["policy_hash"],
            evidence_policy_hashes=input_policy_hashes,
            composition_version=self.access_composition_version,
            evaluated_at=created_at or datetime.now(UTC),
        )
        contradictions = await self._detect_contradictions(
            episode_id=episode_id, tenant_id=tenant_id, claim_ids=claim_ids, conn=conn
        )
        decision_counts = await conn.fetchrow(
            """
            SELECT count(DISTINCT observation_id) AS eligible,
                   count(DISTINCT observation_id) FILTER (WHERE decision='exclude') AS excluded,
                   count(DISTINCT observation_id) FILTER (WHERE decision='hold') AS held
              FROM episode_membership_assertions
             WHERE tenant_id=$1 AND episode_id=$2 AND status='accepted'
            """,
            tenant_id, episode_id,
        )
        eligible = max(int(decision_counts["eligible"]), len(observation_ids))
        held = int(decision_counts["held"])
        coverage = EpisodeCoverage(
            eligible_observation_count=eligible,
            included_observation_count=len(observation_ids),
            reviewed_exclusion_count=int(decision_counts["excluded"]),
            unresolved_candidate_count=held,
            coverage_recall_proxy=(len(observation_ids) / eligible if eligible else 1.0),
            contamination_precision_proxy=1.0,
            citation_completeness=1.0,
            contradiction_preservation=1.0,
            authorization_violation_count=0,
        )
        state = str(episode["lifecycle_state"])
        if state == "settled" and settlement is None:
            raise ValueError("settled episode snapshot requires settlement provenance")
        event_watermark = max(row["observation_occurred_at"] for row in membership_rows)
        ingestion_watermark = max(row["ingested_at"] for row in membership_rows)
        stable_input = {
            "tenant_id": str(tenant_id), "topic_id": str(episode["topic_id"]),
            "episode_id": str(episode_id), "lifecycle_state": state,
            "membership_ids": [str(value) for value in membership_ids],
            "contradiction_ids": [str(value.id) for value in contradictions],
            "access_policy_hash": access.policy_hash,
            "settlement": settlement.model_dump(mode="json") if settlement else None,
        }
        input_hash = hashlib.sha256(
            json.dumps(stable_input, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        existing = await conn.fetchrow(
            "SELECT manifest,snapshot_hash FROM episode_snapshots "
            "WHERE tenant_id=$1 AND episode_id=$2 AND input_hash=$3",
            tenant_id, episode_id, input_hash,
        )
        if existing is not None:
            return EpisodeSnapshot.model_validate(
                {**_json(existing["manifest"]), "snapshot_hash": existing["snapshot_hash"]}
            )
        prior = await conn.fetchrow(
            "SELECT id,version FROM episode_snapshots WHERE tenant_id=$1 AND episode_id=$2 "
            "ORDER BY version DESC LIMIT 1",
            tenant_id, episode_id,
        )
        version = int(prior["version"]) + 1 if prior else 1
        now = created_at or datetime.now(UTC)
        snapshot = EpisodeSnapshot.seal(
            id=uuid7(), tenant_id=tenant_id, topic_id=episode["topic_id"],
            episode_id=episode_id, version=version, lifecycle_state=state,
            prior_snapshot_id=prior["id"] if prior else None,
            observation_ids=observation_ids, evidence_ids=evidence_ids,
            claim_ids=claim_ids, membership_assertion_ids=membership_ids,
            contradictions=tuple(contradictions), access=access, coverage=coverage,
            settlement=settlement, opened_at=episode["opened_at"],
            cutoff_at=event_watermark, created_at=now,
        )
        manifest = snapshot.model_dump(mode="json", exclude={"snapshot_hash"})
        await conn.execute(
            """
            INSERT INTO episode_snapshots (
              id,tenant_id,topic_id,episode_id,version,lifecycle_state,
              prior_snapshot_id,input_hash,manifest,snapshot_hash,access_manifest,
              observation_count,evidence_count,claim_count,contradiction_count,
              event_time_watermark,ingestion_time_watermark,settlement,created_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11::jsonb,$12,$13,$14,
              $15,$16,$17,$18::jsonb,$19
            )
            """,
            snapshot.id, tenant_id, snapshot.topic_id, episode_id, version, state,
            snapshot.prior_snapshot_id, input_hash,
            json.dumps(manifest, sort_keys=True), snapshot.snapshot_hash,
            json.dumps(access.model_dump(mode="json"), sort_keys=True),
            len(observation_ids), len(evidence_ids), len(claim_ids), len(contradictions),
            event_watermark, ingestion_watermark,
            json.dumps(settlement.model_dump(mode="json"), sort_keys=True) if settlement else None,
            now,
        )
        for row in membership_rows:
            await conn.execute(
                "INSERT INTO episode_snapshot_memberships "
                "(tenant_id,snapshot_id,membership_assertion_id,observation_id,evidence_id) "
                "VALUES ($1,$2,$3,$4,$5)",
                tenant_id, snapshot.id, row["id"], row["observation_id"], row["evidence_id"],
            )
        await conn.execute(
            "UPDATE episodes SET head_version=$3,updated_at=now() WHERE id=$1 AND tenant_id=$2",
            episode_id, tenant_id, version,
        )
        return snapshot

    async def _detect_contradictions(
        self,
        *,
        episode_id: UUID,
        tenant_id: UUID,
        claim_ids: tuple[UUID, ...],
        conn: asyncpg.Connection,
    ) -> list[EpisodeContradiction]:
        if not claim_ids:
            return []
        rows = await conn.fetch(
            "SELECT id,subject_ref,predicate,object_value,polarity FROM perception_claims "
            "WHERE tenant_id=$1 AND id=ANY($2::uuid[]) AND status='active' "
            "ORDER BY id",
            tenant_id, list(claim_ids),
        )
        result: list[EpisodeContradiction] = []
        for index, left in enumerate(rows):
            for right in rows[index + 1:]:
                if _json(left["subject_ref"]) != _json(right["subject_ref"]):
                    continue
                if left["predicate"] != right["predicate"]:
                    continue
                opposite = {left["polarity"], right["polarity"]} == {"positive", "negative"}
                different = _json(left["object_value"]) != _json(right["object_value"])
                if not opposite and not different:
                    continue
                kind = "opposite_polarity" if opposite else "incompatible_values"
                ids = sorted((left["id"], right["id"]), key=str)
                key = hashlib.sha256(
                    f"{tenant_id}:{episode_id}:{ids[0]}:{ids[1]}:{kind}:{self.detector_version}".encode()
                ).hexdigest()
                record = await conn.fetchrow(
                    """
                    INSERT INTO episode_contradictions (
                      id,tenant_id,episode_id,left_claim_id,right_claim_id,
                      contradiction_kind,detector_name,detector_version,detection_key
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (tenant_id,detection_key) DO NOTHING
                    RETURNING id,status,explanation
                    """,
                    uuid7(), tenant_id, episode_id, ids[0], ids[1], kind,
                    self.detector_name, self.detector_version, key,
                )
                if record is None:
                    record = await conn.fetchrow(
                        "SELECT id,status,explanation FROM episode_contradictions "
                        "WHERE tenant_id=$1 AND detection_key=$2",
                        tenant_id, key,
                    )
                assert record is not None
                result.append(
                    EpisodeContradiction(
                        id=record["id"], claim_ids=tuple(ids), kind=kind,
                        status=record["status"], explanation=record["explanation"],
                    )
                )
        return result


__all__ = ["EpisodeSnapshotService"]
