from __future__ import annotations

import os
from uuid import uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.truth_kernel import build_default_truth_kernel
from services.evaluation.epistemic_repair.p2_runner import _admission
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import RelationClaimOp, ValidatedDiff


pytestmark = pytest.mark.asyncio


async def test_accepted_bound_claim_atomically_and_idempotently_advances_relation_truth():
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
            f"p8-relation-bridge-{tenant_id}",
        )
        kernel = build_default_truth_kernel()
        blocker = await kernel.admit(tx=conn, command=_admission(tenant_id, 8801))
        blocked = await kernel.admit(tx=conn, command=_admission(tenant_id, 8802))
        claim_id = uuid7()

        def diff() -> ValidatedDiff:
            return ValidatedDiff(
                trigger_ref=uuid7(),
                tenant_id=tenant_id,
                relation_claim_ops=[
                    RelationClaimOp(
                        id=claim_id,
                        op="upsert",
                        source_model_id=blocker.model_id,
                        target_model_id=blocked.model_id,
                        subject_ref={"kind": "model", "model_id": str(blocker.model_id)},
                        object_ref={"kind": "model", "model_id": str(blocked.model_id)},
                        predicate="blocks",
                        edge_kind="blocks",
                        endpoint_binding_status="bound",
                        write_policy="accepted_edge",
                        status="accepted",
                        confidence=0.86,
                        binding_confidence=0.92,
                        evidence_model_ids=[blocker.model_id, blocked.model_id],
                        explanation="The first accepted model blocks the second.",
                    )
                ],
            )

        first = await apply_diff(diff(), conn, "T1:batch_memory")
        second = await apply_diff(diff(), conn, "T1:batch_memory")

        assert first["relation_claim_ops"][0]["canonical_relation_version_id"]
        assert (
            second["relation_claim_ops"][0]["canonical_relation_version_id"]
            == first["relation_claim_ops"][0]["canonical_relation_version_id"]
        )
        assert second["edge_ops"][0]["op"] == "reuse"
        relation = await conn.fetchrow(
            """
            SELECT id, truth_relation_version_id, truth_relation_kind
            FROM accepted_current_relations
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            claim_id,
        )
        assert relation is not None
        assert relation["truth_relation_kind"] == "dependency_constraint"
        assert await conn.fetchval(
            "SELECT count(*) FROM relation_truth_versions WHERE tenant_id=$1 AND relation_id=$2",
            tenant_id,
            claim_id,
        ) == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM relation_edge_projections WHERE tenant_id=$1 AND relation_id=$2",
            tenant_id,
            claim_id,
        ) == 1
        participants = await conn.fetch(
            """
            SELECT role, model_id, model_version_id
            FROM relation_truth_participants
            WHERE tenant_id=$1 AND relation_version_id=$2
            ORDER BY ordinal
            """,
            tenant_id,
            relation["truth_relation_version_id"],
        )
        assert [(row["role"], row["model_id"]) for row in participants] == [
            ("dependent", blocked.model_id),
            ("prerequisite", blocker.model_id),
        ]
        assert {row["model_version_id"] for row in participants} == {
            blocker.version_id,
            blocked.version_id,
        }
        assert await conn.fetchval(
            """
            SELECT count(*) FROM relation_truth_evidence
            WHERE tenant_id=$1 AND relation_version_id=$2 AND polarity=1
            """,
            tenant_id,
            relation["truth_relation_version_id"],
        ) == 2
    finally:
        await tx.rollback()
        await conn.close()
