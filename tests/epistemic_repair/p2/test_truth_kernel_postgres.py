from __future__ import annotations

from datetime import timedelta
import json
import os
from uuid import uuid4

import asyncpg
import pytest

from lib.contracts.truth_admission import (
    AdmitModelCommand,
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
        with pytest.raises(asyncpg.RaiseError, match="truth-kernel command"):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE models SET confidence=0.9 WHERE tenant_id=$1 AND id=$2",
                    tenant_id,
                    admitted.model_id,
                )

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


async def test_exact_duplicate_absorption_preserves_distinct_evidence_on_postgres():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL truth proof")

    conn = await asyncpg.connect(dsn)
    outer = conn.transaction()
    await outer.start()
    try:
        tenant_id = uuid4()
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'p2-duplicate-proof')",
            tenant_id,
        )
        service = TruthKernelService(storage=AsyncpgTruthKernelStorage())
        original_command = _for_tenant(tenant_id)
        original = await service.admit(tx=conn, command=original_command)

        absorbed = []
        for ordinal in range(10):
            duplicate = original_command.model_copy(
                update={
                    "command_id": uuid4(),
                    "idempotency_key": f"postgres-semantic-duplicate:{ordinal}",
                    "issued_at": original_command.issued_at
                    + timedelta(seconds=ordinal + 1),
                }
            )
            absorbed.append(await service.admit(tx=conn, command=duplicate))

        assert all(item.outcome == "absorbed_duplicate" for item in absorbed)
        assert all(item.version_id == original.version_id for item in absorbed)
        assert await conn.fetchval(
            "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1",
            tenant_id,
        ) == 1
        assert await conn.fetchval(
            """
            SELECT count(*) FROM truth_command_receipts
            WHERE tenant_id=$1 AND outcome='absorbed_duplicate'
            """,
            tenant_id,
        ) == 10
        assert await conn.fetchval(
            "SELECT count(*) FROM truth_semantic_absorptions WHERE tenant_id=$1",
            tenant_id,
        ) == 10
        audit = await conn.fetchrow(
            """
            SELECT request_digest, semantic_digest, attempted_command,
                   absorbed_into_version_id
            FROM truth_semantic_absorptions
            WHERE tenant_id=$1 ORDER BY recorded_at LIMIT 1
            """,
            tenant_id,
        )
        attempted_command = audit["attempted_command"]
        if isinstance(attempted_command, str):
            attempted_command = json.loads(attempted_command)
        assert attempted_command["version"]["semantic_digest"] == original.semantic_digest
        assert AdmitModelCommand.model_validate(
            attempted_command
        ).request_digest == audit["request_digest"]
        assert audit["absorbed_into_version_id"] == original.version_id

        distinct_evidence = _for_tenant(tenant_id)
        assert distinct_evidence.version.natural == original_command.version.natural
        assert (
            distinct_evidence.version.semantic_digest
            != original_command.version.semantic_digest
        )
        distinct = await service.admit(tx=conn, command=distinct_evidence)
        assert distinct.outcome == "applied"
        assert distinct.version_id != original.version_id
        assert await conn.fetchval(
            "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1",
            tenant_id,
        ) == 2
    finally:
        await outer.rollback()
        await conn.close()
