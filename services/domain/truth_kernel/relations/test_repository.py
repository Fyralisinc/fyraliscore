from __future__ import annotations

import os
from datetime import timedelta
from uuid import uuid4

import pytest

from services.domain.truth_kernel.relations.contracts import RelationDisposition, RelationVersion
from services.domain.truth_kernel.relations.repository import AsyncpgRelationKernelStorage
from services.domain.truth_kernel.relations.service import RelationTruthKernel
from services.domain.truth_kernel.relations.test_service import NOW, candidate, command


class RecordingTransaction:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.fetch_rows: list[dict] = []

    async def execute(self, sql: str, *args):
        statement = " ".join(sql.split())
        self.statements.append((statement, args))
        return "UPDATE 1" if statement.startswith("UPDATE relation_truth_heads") else "INSERT 0 1"

    async def executemany(self, sql: str, args):
        statement = " ".join(sql.split())
        for values in args:
            self.statements.append((statement, tuple(values)))

    async def fetchrow(self, sql: str, *args):
        self.statements.append((" ".join(sql.split()), args))
        return None

    async def fetchval(self, sql: str, *args):
        self.statements.append((" ".join(sql.split()), args))
        return True

    async def fetch(self, sql: str, *args):
        self.statements.append((" ".join(sql.split()), args))
        return self.fetch_rows


@pytest.mark.asyncio
async def test_admission_bundle_persists_decision_version_participants_evidence_head_and_receipt() -> None:
    tx = RecordingTransaction()
    item = candidate()
    cmd = command(item)
    storage = AsyncpgRelationKernelStorage()
    # Endpoint validation is separately tested; represent both exact accepted heads.
    tx.fetch_rows = [
        {"id": participant.model_id, "truth_version_id": participant.model_version_id}
        for participant in item.participants
    ]

    receipt = await RelationTruthKernel(storage).admit(tx=tx, command=cmd)

    assert receipt.disposition is RelationDisposition.ACCEPTED
    sql = "\n".join(statement for statement, _ in tx.statements)
    for table in (
        "relation_truth_admission_decisions", "relation_truth_versions",
        "relation_truth_participants", "relation_truth_evidence",
        "relation_truth_heads", "truth_command_receipts",
    ):
        assert f"INSERT INTO {table}" in sql
    participant_rows = [args for statement, args in tx.statements if "INSERT INTO relation_truth_participants" in statement]
    evidence_rows = [args for statement, args in tx.statements if "INSERT INTO relation_truth_evidence" in statement]
    assert len(participant_rows) == len(item.participants)
    assert len(evidence_rows) == len(item.evidence)
    assert {row[4] for row in participant_rows} == {p.model_version_id for p in item.participants}
    assert {row[3] for row in evidence_rows} == {e.evidence_reference_id for e in item.evidence}


@pytest.mark.asyncio
async def test_endpoint_validation_uses_exact_accepted_current_model_versions() -> None:
    tx = RecordingTransaction()
    item = candidate()
    tx.fetch_rows = [{"id": item.participants[0].model_id, "truth_version_id": item.participants[0].model_version_id}]

    errors = await AsyncpgRelationKernelStorage().validate_active_participants(
        tx=tx, tenant_id=item.tenant_id, participants=item.participants
    )

    assert len(errors) == 1
    assert item.participants[1].role in errors[0]
    statement, args = tx.statements[-1]
    assert "FROM accepted_current_models" in statement
    assert args[1] == [p.model_version_id for p in item.participants]


@pytest.mark.asyncio
async def test_compare_and_swap_matches_the_entire_expected_head() -> None:
    tx = RecordingTransaction()
    item = candidate()
    cmd = command(item)
    version = RelationTruthKernel._head(
        # Construct the initial immutable version through the service's admission path shape.
        RelationVersion(
            relation_version_id=cmd.relation_version_id,
            relation_id=item.candidate_relation_id, tenant_id=item.tenant_id, version=1,
            admission_decision_id=cmd.admission_decision_id, kind=item.known_kind,
            participants=item.participants, rationale=item.rationale,
            assertion=item.assertion, evidence=item.evidence, created_at=cmd.issued_at,
            semantic_digest=RelationVersion.compute_semantic_digest(
                kind=item.known_kind, participants=item.participants, rationale=item.rationale,
                assertion=item.assertion, evidence=item.evidence,
            ),
        ), cmd.issued_at,
    )
    successor = type(version)(
        version.tenant_id, version.relation_id, uuid4(),
        2, version.semantic_digest, version.lifecycle, NOW + timedelta(minutes=1),
    )
    assert await AsyncpgRelationKernelStorage().compare_and_swap_head(
        tx=tx, expected=version, successor=successor
    )
    statement, args = tx.statements[-1]
    assert "advanced_at=$12" in statement
    assert args[7:] == (
        version.relation_version_id, version.version, version.semantic_digest,
        version.lifecycle.value, version.advanced_at,
    )


@pytest.mark.asyncio
async def test_invalidation_is_atomic_versioned_and_idempotency_protected() -> None:
    tx = RecordingTransaction()
    affected = uuid4()
    tx.fetch_rows = [{"affected_id": affected}]
    item = candidate()

    result = await AsyncpgRelationKernelStorage().dispute_for_invalidated_evidence(
        tx=tx, tenant_id=item.tenant_id,
        invalidated_model_version_id=item.evidence[0].model_version_id,
        cause_code="MODEL_FALSIFIED", occurred_at=NOW,
    )

    assert result == (affected,)
    statement = tx.statements[-1][0]
    assert "INSERT INTO relation_truth_versions" in statement
    assert "lifecycle, rationale" in statement and "'disputed'" in statement
    assert "UPDATE relation_truth_heads" in statement
    assert "INSERT INTO truth_repair_obligations" in statement
    assert "ON CONFLICT (tenant_id, invalidated_model_version_id, affected_kind, affected_id, cause_code) DO NOTHING" in statement


@pytest.mark.asyncio
async def test_postgres_relation_kernel_schema_is_queryable_when_configured() -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL is not configured")
    asyncpg = pytest.importorskip("asyncpg")
    connection = await asyncpg.connect(database_url)
    try:
        for relation in (
            "relation_truth_admission_decisions", "relation_truth_versions",
            "relation_truth_heads", "relation_truth_participants",
            "relation_truth_evidence", "truth_repair_obligations",
            "accepted_current_models",
        ):
            assert await connection.fetchval("SELECT to_regclass($1)", relation) is not None
    finally:
        await connection.close()
