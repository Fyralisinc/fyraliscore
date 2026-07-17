from __future__ import annotations

import os
from uuid import uuid4

import asyncpg
import pytest

from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.types import ModelCreate
from services.domain.models.repo import ModelsRepo
from services.reasoning.think.applier import _with_claim_evidence_defaults
from services.reasoning.think.diff_schema import ClaimOp
from services.reasoning.think.truth_admission import (
    admit_validated_think_claim,
    advance_validated_think_model,
)
from services.reasoning.think.validator import _validate_claim_op


def _claim(*, supporting_event_ids=(), born_from_event_id=None) -> ClaimOp:
    entry = {
        "tenant_id": str(uuid4()),
        "proposition": {"kind": "state", "subject": "Atlas", "assertion": "blocked"},
        "natural": "Atlas is blocked",
        "supporting_event_ids": list(supporting_event_ids),
    }
    if born_from_event_id is not None:
        entry["born_from_event_id"] = born_from_event_id
    return ClaimOp(op="insert", entry=entry)


def test_multi_signal_trigger_is_never_inherited_as_claim_evidence() -> None:
    trigger_ids = [uuid4(), uuid4()]
    result = _with_claim_evidence_defaults(
        _claim(), trigger_cause_event_id=None,
        trigger_supporting_event_ids=trigger_ids,
    )
    assert result.entry is not None
    assert result.entry["supporting_event_ids"] == []
    assert "born_from_event_id" not in result.entry


def test_explicit_claim_local_evidence_is_not_widened_to_batch() -> None:
    local_id, unrelated_id, synthetic_born_id = uuid4(), uuid4(), uuid4()
    result = _with_claim_evidence_defaults(
        _claim(
            supporting_event_ids=[local_id],
            born_from_event_id=synthetic_born_id,
        ),
        trigger_cause_event_id=None,
        trigger_supporting_event_ids=[local_id, unrelated_id],
    )
    assert result.entry is not None
    assert result.entry["supporting_event_ids"] == [local_id]
    assert result.entry["born_from_event_id"] == synthetic_born_id


def test_single_signal_trigger_may_supply_evidence_fallback() -> None:
    only_id = uuid4()
    result = _with_claim_evidence_defaults(
        _claim(), trigger_cause_event_id=None,
        trigger_supporting_event_ids=[only_id],
    )
    assert result.entry is not None
    assert result.entry["supporting_event_ids"] == [only_id]
    assert result.entry["born_from_event_id"] == only_id


@pytest.mark.asyncio
async def test_governed_admission_persists_exact_claim_local_evidence() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL admission proof")
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        tenant_id, observation_id = uuid4(), uuid4()
        await conn.execute(
            "INSERT INTO tenants (id,name) VALUES ($1,$2)", tenant_id, "think-admission-proof",
        )
        await conn.execute(
            """
            INSERT INTO observations
              (id,tenant_id,occurred_at,kind,source_channel,content,content_text,
               embedding,embedding_pending,trust_tier)
            VALUES ($1,$2,now(),'signal','test','{}'::jsonb,$3,$4,FALSE,'authoritative')
            """,
            observation_id, tenant_id, "Atlas release certificate is blocked",
            "[" + ",".join(["0"] * 768) + "]",
        )
        proposed = ModelCreate(
            tenant_id=tenant_id, born_from_event_id=observation_id,
            proposition={"kind": "state", "subject": "Atlas", "assertion": "blocked"},
            natural="Atlas is blocked on its release certificate",
            embedding=[0.0] * 768, scope_temporal={}, confidence=0.7,
            confidence_at_assertion=0.7, supporting_event_ids=[observation_id],
        )
        synthetic_born_id, missing_id = uuid4(), uuid4()
        valid_op = ClaimOp(op="insert", entry={
            **proposed.model_dump(mode="json"),
            "born_from_event_id": str(synthetic_born_id),
            "supporting_event_ids": [str(observation_id)],
        })
        validated_op = await _validate_claim_op(
            valid_op, None, conn, tenant_id=tenant_id,
        )
        assert validated_op.entry is not None
        assert validated_op.entry["born_from_event_id"] == str(synthetic_born_id)
        invalid_entry = dict(valid_op.entry or {})
        invalid_entry["supporting_event_ids"] = [
            str(observation_id), str(missing_id),
        ]
        with pytest.raises(ValidationError, match=str(missing_id)):
            await _validate_claim_op(
                ClaimOp(op="insert", entry=invalid_entry), None, conn,
                tenant_id=tenant_id,
            )
        row = await admit_validated_think_claim(
            conn, proposed=proposed, evidence_observation_ids=(observation_id,),
            models_repo=ModelsRepo(None, embedder=None),
        )
        assert await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 AND id=$2",
            tenant_id, row.id,
        ) == 1
        first_command = await advance_validated_think_model(
            conn, tenant_id=tenant_id, model_id=row.id, confidence=0.61,
            evidence_observation_ids=(observation_id,), reason_code="focused-proof",
        )
        replay_command = await advance_validated_think_model(
            conn, tenant_id=tenant_id, model_id=row.id, confidence=0.61,
            evidence_observation_ids=(observation_id,), reason_code="focused-proof",
        )
        assert replay_command == first_command
        assert await conn.fetchval(
            "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1 AND model_id=$2",
            tenant_id, row.id,
        ) == 2
        assert await conn.fetchval(
            "SELECT confidence FROM accepted_current_models WHERE tenant_id=$1 AND id=$2",
            tenant_id, row.id,
        ) == pytest.approx(0.61)
        assert await conn.fetchval(
            "SELECT confidence FROM models WHERE tenant_id=$1 AND id=$2",
            tenant_id, row.id,
        ) == pytest.approx(0.61)
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM model_truth_evidence_references evidence
            JOIN model_truth_versions version
              ON version.tenant_id=evidence.tenant_id
             AND version.version_id=evidence.model_version_id
            JOIN model_truth_heads head
              ON head.tenant_id=version.tenant_id
             AND head.version_id=version.version_id
            WHERE evidence.tenant_id=$1 AND version.model_id=$2
              AND evidence.evidence_id=$3
            """,
            tenant_id, row.id, str(observation_id),
        ) == 1
        missing_id = uuid4()
        with pytest.raises(InvariantViolation, match="same tenant") as raised:
            await admit_validated_think_claim(
                conn, proposed=proposed.model_copy(update={"id": uuid4()}),
                evidence_observation_ids=(missing_id,),
                models_repo=ModelsRepo(None, embedder=None),
            )
        assert raised.value.context["missing"] == [str(missing_id)]
        assert raised.value.context["found"] == []
    finally:
        await transaction.rollback()
        await conn.close()
