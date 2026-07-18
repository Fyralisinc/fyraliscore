from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from uuid import uuid4

import asyncpg
import pytest

from lib.contracts.truth_admission import ModelTruthTransition
from lib.shared.errors import ValidationError
from services.domain.truth_kernel import build_default_truth_kernel
from services.evaluation.epistemic_repair.p2_runner import _admission
from services.reasoning.think.diff_schema import MemoryLifecycleOp
from services.reasoning.think.truth_admission import advance_validated_think_model
from services.reasoning.think.validator import _validate_memory_lifecycle_op


class _ValidationConnection:
    def __init__(self, *, model_id, observation_ids=()) -> None:
        self.model_id = model_id
        self.observation_ids = set(observation_ids)

    async def fetch(self, query, _tenant_id, values):
        if "FROM models" in query:
            return [{"id": self.model_id, "status": "active"}]
        return [{"id": value} for value in values if value in self.observation_ids]


@pytest.mark.asyncio
async def test_confirm_requires_nonempty_claim_local_observation_evidence() -> None:
    model_id, evidence_id, tenant_id = uuid4(), uuid4(), uuid4()
    conn = _ValidationConnection(model_id=model_id, observation_ids={evidence_id})

    with pytest.raises(ValidationError, match="requires claim-local"):
        await _validate_memory_lifecycle_op(
            MemoryLifecycleOp(
                model_id=model_id,
                action="confirm",
                evidence_event_ids=[evidence_id],
                claim_local_evidence_event_ids=[],
                rationale="Reviewed but no exact observation supports the claim.",
            ),
            conn,
            tenant_id=tenant_id,
        )


@pytest.mark.asyncio
async def test_claim_local_evidence_must_be_declared_and_same_tenant() -> None:
    model_id, broad_id, undeclared_id, tenant_id = (
        uuid4(), uuid4(), uuid4(), uuid4()
    )
    conn = _ValidationConnection(model_id=model_id, observation_ids={broad_id})

    with pytest.raises(ValidationError, match="must be a subset"):
        await _validate_memory_lifecycle_op(
            MemoryLifecycleOp(
                model_id=model_id,
                action="confirm",
                evidence_event_ids=[broad_id],
                claim_local_evidence_event_ids=[undeclared_id],
                rationale="An undeclared sibling cannot support this claim.",
            ),
            conn,
            tenant_id=tenant_id,
        )

    with pytest.raises(ValidationError, match="missing observation"):
        await _validate_memory_lifecycle_op(
            MemoryLifecycleOp(
                model_id=model_id,
                action="confirm",
                evidence_event_ids=[undeclared_id],
                claim_local_evidence_event_ids=[undeclared_id],
                rationale="A cross-tenant observation must be rejected early.",
            ),
            conn,
            tenant_id=tenant_id,
        )


@pytest.mark.asyncio
async def test_distinct_exact_corroborations_advance_and_exact_replay_is_idempotent():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL required")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        tenant_id = uuid4()
        await conn.execute(
            "INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
            tenant_id,
            f"atomic-corroboration-{tenant_id}",
        )
        admitted = await build_default_truth_kernel().admit(
            tx=conn, command=_admission(tenant_id, 9911)
        )
        first_evidence, second_evidence = uuid4(), uuid4()
        for evidence_id, text in (
            (first_evidence, "The exact blocked state is still current."),
            (second_evidence, "A later independent update confirms the same state."),
        ):
            await conn.execute(
                """
                INSERT INTO observations(
                  id,tenant_id,occurred_at,kind,source_channel,content,
                  content_text,embedding_pending,trust_tier,entities_mentioned
                ) VALUES($1,$2,$3,'signal','test',$4::jsonb,$5,TRUE,'ordinary','[]')
                """,
                evidence_id,
                tenant_id,
                datetime.now(timezone.utc),
                json.dumps({"text": text}),
                text,
            )

        first = await advance_validated_think_model(
            conn, tenant_id=tenant_id, model_id=admitted.model_id,
            confidence=0.7, evidence_observation_ids=(first_evidence,),
            transition=ModelTruthTransition.CONFIRM, reason_code="batch-one",
        )
        second = await advance_validated_think_model(
            conn, tenant_id=tenant_id, model_id=admitted.model_id,
            confidence=0.75, evidence_observation_ids=(second_evidence,),
            transition=ModelTruthTransition.CONFIRM, reason_code="batch-two",
        )
        replay = await advance_validated_think_model(
            conn, tenant_id=tenant_id, model_id=admitted.model_id,
            confidence=0.75, evidence_observation_ids=(second_evidence,),
            transition=ModelTruthTransition.CONFIRM,
            reason_code="different-trigger-same-exact-evidence",
        )

        assert first != second
        assert replay == second
        assert await conn.fetchval(
            "SELECT version FROM model_truth_heads WHERE tenant_id=$1 AND model_id=$2",
            tenant_id,
            admitted.model_id,
        ) == 3
        evidence_ids = set(await conn.fetchval(
            """
            SELECT array_agg(evidence_id)
            FROM model_truth_evidence_references evidence
            JOIN model_truth_heads head
              ON head.tenant_id=evidence.tenant_id
             AND head.version_id=evidence.model_version_id
            WHERE head.tenant_id=$1 AND head.model_id=$2
              AND evidence.evidence_kind='observation'
            """,
            tenant_id,
            admitted.model_id,
        ))
        assert {str(first_evidence), str(second_evidence)} <= evidence_ids
    finally:
        await tx.rollback()
        await conn.close()
