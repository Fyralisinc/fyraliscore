from __future__ import annotations

from datetime import timedelta
import os
from uuid import uuid4

import asyncpg
import pytest

from lib.contracts.truth_admission import (
    AdvanceModelHeadCommand,
    ModelHeadExpectation,
    ModelTruthLifecycle,
    ModelTruthTransition,
    ModelVersion,
)
from lib.shared.errors import InvariantViolation
from services.domain.truth_kernel.repository import AsyncpgTruthKernelStorage
from services.domain.truth_kernel.service import TruthKernelService
from services.domain.truth_kernel.tests.test_service import admission


pytestmark = pytest.mark.asyncio


def _for_tenant(tenant_id):
    base = admission()
    evidence = tuple(
        item.model_copy(update={"tenant_id": tenant_id})
        for item in base.candidate.proposed_evidence
    )
    candidate = base.candidate.model_copy(
        update={"tenant_id": tenant_id, "proposed_evidence": evidence}
    )
    decision = base.decision.model_copy(
        update={
            "tenant_id": tenant_id,
            "candidate_digest": candidate.candidate_digest,
        }
    )
    semantic_digest = ModelVersion.compute_semantic_digest(
        proposition=candidate.proposition,
        natural=candidate.natural,
        evidence=evidence,
        scope=candidate.proposed_scope,
    )
    version = base.version.model_copy(
        update={
            "tenant_id": tenant_id,
            "evidence": evidence,
            "semantic_digest": semantic_digest,
        }
    )
    return base.model_copy(
        update={
            "tenant_id": tenant_id,
            "candidate": candidate,
            "decision": decision,
            "version": version,
        }
    )


def _falsify(receipt, version):
    next_version = version.model_copy(
        update={
            "version_id": uuid4(),
            "version": 2,
            "lifecycle": ModelTruthLifecycle.FALSIFIED,
            "created_at": version.created_at + timedelta(minutes=1),
        }
    )
    return AdvanceModelHeadCommand(
        command_id=uuid4(),
        idempotency_key=f"falsify:{receipt.model_id}",
        tenant_id=receipt.tenant_id,
        expectation=ModelHeadExpectation(
            tenant_id=receipt.tenant_id,
            model_id=receipt.model_id,
            expected_version_id=receipt.version_id,
            expected_version=receipt.version,
            expected_semantic_digest=receipt.semantic_digest,
            expected_lifecycle=receipt.lifecycle,
        ),
        next_version=next_version,
        transition=ModelTruthTransition.FALSIFY,
        reason_codes=("contradicting_authoritative_evidence",),
        issued_at=next_version.created_at + timedelta(seconds=1),
    )


async def test_admission_replay_and_terminal_fence_on_postgres():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL truth proof")

    conn = await asyncpg.connect(dsn)
    outer = conn.transaction()
    await outer.start()
    try:
        tenant_id = uuid4()
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'p2-truth-proof')",
            tenant_id,
        )
        command = _for_tenant(tenant_id)
        service = TruthKernelService(storage=AsyncpgTruthKernelStorage())

        admitted = await service.admit(tx=conn, command=command)
        replay = await service.admit(tx=conn, command=command)
        assert replay == admitted
        assert await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1",
            tenant_id,
        ) == 1
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM accepted_current_models accepted
            JOIN models legacy
              ON legacy.tenant_id=accepted.tenant_id AND legacy.id=accepted.id
            WHERE accepted.tenant_id=$1
            """,
            tenant_id,
        ) == 1

        falsify = _falsify(admitted, command.version)
        terminal = await service.advance(tx=conn, command=falsify)
        assert terminal.lifecycle is ModelTruthLifecycle.FALSIFIED
        assert await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1",
            tenant_id,
        ) == 0
        assert await conn.fetchval(
            "SELECT count(*) FROM model_truth_lifecycle_events WHERE tenant_id=$1",
            tenant_id,
        ) == 1

        competing = falsify.model_copy(
            update={
                "command_id": uuid4(),
                "idempotency_key": f"competing:{admitted.model_id}",
            }
        )
        with pytest.raises(InvariantViolation, match="terminal"):
            await service.advance(tx=conn, command=competing)
    finally:
        await outer.rollback()
        await conn.close()
