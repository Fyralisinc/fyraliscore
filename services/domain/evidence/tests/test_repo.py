from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import SourceEvidenceCreate
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
