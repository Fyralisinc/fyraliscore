from __future__ import annotations

from uuid import uuid4

import pytest

from lib.shared.errors import ValidationError
from services.domain.substrate_candidates import upsert_substrate_candidate


class _FakeConn:
    def __init__(self) -> None:
        self.query = ""
        self.args = ()

    async def fetchrow(self, query: str, *args):
        self.query = query
        self.args = args
        return {
            "id": args[0],
            "tenant_id": args[1],
            "kind": args[2],
            "label": args[3],
            "status": args[4],
            "confidence": args[5],
            "fingerprint": args[6],
            "aliases": args[7],
            "evidence_observation_ids": args[8],
            "evidence_model_ids": args[9],
            "related_candidate_ids": args[10],
            "proposed_canonical_ref": args[11],
            "promotion_ref": None,
            "merge_target_id": None,
            "metadata": args[12],
            "created_by_run_id": args[13],
        }


@pytest.mark.asyncio
async def test_upsert_substrate_candidate_merges_evidence_shape() -> None:
    conn = _FakeConn()
    tenant_id = uuid4()
    observation_id = uuid4()
    run_id = uuid4()

    candidate = await upsert_substrate_candidate(
        conn,
        tenant_id=tenant_id,
        kind="actor",
        label="Rachel",
        fingerprint="actor:rachel",
        confidence=0.86,
        aliases=[
            {"source_channel": "slack", "source_actor_ref": "rachel"},
            {"source_channel": "slack", "source_actor_ref": "rachel"},
        ],
        evidence_observation_ids=[observation_id, str(observation_id)],
        metadata={"basis": "source_actor_ref"},
        created_by_run_id=run_id,
    )

    assert "ON CONFLICT (tenant_id, kind, fingerprint)" in conn.query
    assert candidate.kind == "actor"
    assert candidate.scope_ref == {"type": "candidate_actor", "id": str(candidate.id)}
    assert candidate.evidence_observation_ids == [observation_id]
    assert candidate.aliases == [
        {"source_actor_ref": "rachel", "source_channel": "slack"}
    ]
    assert candidate.metadata == {"basis": "source_actor_ref"}
    assert candidate.created_by_run_id == run_id


@pytest.mark.asyncio
async def test_upsert_substrate_candidate_rejects_unknown_kind() -> None:
    with pytest.raises(ValidationError):
        await upsert_substrate_candidate(
            _FakeConn(),
            tenant_id=uuid4(),
            kind="mystery",
            label="Mystery",
            fingerprint="mystery:x",
        )
