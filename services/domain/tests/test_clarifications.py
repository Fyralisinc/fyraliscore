from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.domain.clarifications import open_clarification_request


class FakeConn:
    def __init__(self, *, fetchvals=None):
        self.fetches = []
        self._fetchvals = list(fetchvals or [])

    async def fetchval(self, sql, *args):
        self.fetches.append((sql, args))
        if self._fetchvals:
            return self._fetchvals.pop(0)
        return None


@pytest.mark.asyncio
async def test_open_clarification_returns_inserted_id() -> None:
    inserted_id = uuid7()
    conn = FakeConn(fetchvals=[inserted_id])
    tenant_id = uuid7()

    result = await open_clarification_request(
        conn,
        tenant_id=tenant_id,
        kind="actor_identity",
        question="Who is slack:U123?",
        object_kind="source_actor_ref",
        object_key="slack:U123",
    )

    assert result == inserted_id
    assert len(conn.fetches) == 1
    sql, args = conn.fetches[0]
    assert "INSERT INTO clarification_requests" in sql
    assert args[1] == tenant_id
    assert args[2] == "actor_identity"
    assert args[8] == "slack:U123"


@pytest.mark.asyncio
async def test_open_clarification_returns_existing_open_request_on_conflict() -> None:
    existing_id = uuid7()
    tenant_id = uuid7()
    object_id = uuid7()
    conn = FakeConn(fetchvals=[None, existing_id])

    result = await open_clarification_request(
        conn,
        tenant_id=tenant_id,
        kind="entity_resolution",
        question="What does Alpen refer to?",
        object_kind="entity_review",
        object_id=object_id,
        object_key="customer:alpen",
    )

    assert result == existing_id
    assert len(conn.fetches) == 2
    sql, args = conn.fetches[1]
    assert "COALESCE(object_key, object_id::text)" in sql
    assert args == (
        tenant_id,
        "entity_resolution",
        "entity_review",
        object_id,
        "customer:alpen",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_open_clarification_conflict_lookup_accepts_uuid_object_id(
    fresh_db,
) -> None:
    tenant_id = uuid7()
    object_id = uuid7()
    request_id = uuid7()

    async with fresh_db.acquire() as conn:
        inserted = await open_clarification_request(
            conn,
            tenant_id=tenant_id,
            kind="substrate_candidate_resolution",
            question="Who should this candidate resolve to?",
            object_kind="substrate_candidate",
            object_id=object_id,
            priority="normal",
            request_id=request_id,
        )
        returned = await open_clarification_request(
            conn,
            tenant_id=tenant_id,
            kind="substrate_candidate_resolution",
            question="Who should this candidate resolve to?",
            object_kind="substrate_candidate",
            object_id=object_id,
            priority="normal",
        )

    assert inserted == request_id
    assert returned == request_id


@pytest.mark.asyncio
async def test_open_clarification_rejects_invalid_priority() -> None:
    conn = FakeConn()

    with pytest.raises(ValidationError):
        await open_clarification_request(
            conn,
            tenant_id=uuid7(),
            kind="actor_identity",
            priority="maybe",
            question="Who is github:alice?",
            object_kind="source_actor_ref",
            object_key="github:alice",
        )

    assert conn.fetches == []
