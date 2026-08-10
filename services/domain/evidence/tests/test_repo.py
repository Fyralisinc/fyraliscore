from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import SourceEvidenceCreate
from services.domain.evidence.access import (
    can_actor_read_evidence_set,
    compose_access_policies,
)
from services.domain.evidence.repo import SourceEvidenceRepository


pytestmark = pytest.mark.integration


def _evidence(tenant_id, revision: str, *, operation: str = "update"):
    now = datetime.now(tz=timezone.utc)
    return SourceEvidenceCreate(
        tenant_id=tenant_id,
        source="notion",
        installation_scope="stateless:notion",
        source_channel="notion:object",
        source_object_type="page",
        source_object_id="audit-page",
        source_revision_id=revision,
        operation=operation,
        source_recorded_at=now,
        valid_from=now - timedelta(days=1),
        raw_object_key=f"prod/notion/{revision}.json",
        content_hash=(revision.encode().hex() + "0" * 40)[:40],
        raw_ingested_at=now,
        normalized_at=now,
        ingress_kind="poll",
        connector_version="1.0.0",
        parser_version="notion-v1",
        normalizer_version="normalized-envelope-v1",
    )


async def test_revision_replay_dedups_but_later_revision_persists(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            tenant_id,
            f"evidence-{tenant_id}",
        )
        repo = SourceEvidenceRepository()
        first = await repo.insert(_evidence(tenant_id, "r1"), conn=conn)
        replay = await repo.insert(_evidence(tenant_id, "r1"), conn=conn)
        second = await repo.insert(
            _evidence(tenant_id, "r2").model_copy(
                update={"supersedes_revision_id": "r1"}
            ),
            conn=conn,
        )

    assert replay.deduped
    assert replay.evidence.id == first.evidence.id
    assert second.evidence.id != first.evidence.id
    assert second.evidence.supersedes_evidence_id == first.evidence.id


def test_episode_access_policy_is_the_intersection_of_evidence_policies() -> None:
    actor = str(uuid7())
    common = {"type": "actor", "id": actor}
    policy = compose_access_policies(
        [
            {
                "visibility": "restricted",
                "audience": [common, {"type": "actor", "id": str(uuid7())}],
                "source_acl_version": "slack-v2",
            },
            {
                "visibility": "restricted",
                "audience": [common],
                "source_acl_version": "notion-v4",
            },
            {
                "visibility": "tenant",
                "audience": [],
                "source_acl_version": "internal-v1",
            },
        ]
    )

    assert policy["visibility"] == "restricted"
    assert policy["audience"] == [common]
    assert len(policy["policy_hash"]) == 64


def test_unknown_evidence_policy_taints_episode_policy() -> None:
    policy = compose_access_policies(
        [
            {
                "visibility": "tenant",
                "audience": [],
                "source_acl_version": "internal-v1",
            },
            {
                "visibility": "unknown",
                "audience": [],
                "source_acl_version": "not-captured",
            },
        ]
    )
    assert policy["visibility"] == "unknown"


async def test_evidence_set_requires_every_source_acl(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    allowed_actor, denied_actor = uuid7(), uuid7()
    async with fresh_db.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO actors (
              id, tenant_id, type, display_name, status, metadata, created_at
            ) VALUES ($1, $2, 'human_internal', $3, 'active', '{}'::jsonb, now())
            """,
            [
                (allowed_actor, tenant_id, "Allowed"),
                (denied_actor, tenant_id, "Denied"),
            ],
        )
        tenant_evidence = await SourceEvidenceRepository().insert(
            _evidence(tenant_id, "acl-r1").model_copy(
                update={
                    "access_policy": {
                        "visibility": "tenant",
                        "audience": [],
                        "source_acl_version": "test-v1",
                    }
                }
            ),
            conn=conn,
        )
        restricted_evidence = await SourceEvidenceRepository().insert(
            _evidence(tenant_id, "acl-r2").model_copy(
                update={
                    "access_policy": {
                        "visibility": "restricted",
                        "audience": [
                            {"type": "actor", "id": str(allowed_actor)}
                        ],
                        "source_acl_version": "test-v2",
                    }
                }
            ),
            conn=conn,
        )
        ids = [tenant_evidence.evidence.id, restricted_evidence.evidence.id]
        allowed = await can_actor_read_evidence_set(
            allowed_actor, tenant_id=tenant_id, evidence_ids=ids, conn=conn
        )
        denied = await can_actor_read_evidence_set(
            denied_actor, tenant_id=tenant_id, evidence_ids=ids, conn=conn
        )

    assert allowed.allowed
    assert not denied.allowed
    assert denied.reason == "evidence_acl_not_member"
