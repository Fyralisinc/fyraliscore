from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pytest

from lib.shared.ids import uuid7
from scripts.run_company_learning_pair_harness import (
    _ingest_slack,
    run_pair_experiment,
)
from services.domain.entity_aliases.repo import EntityAliasRepo


pytestmark = pytest.mark.integration


async def test_pair_harness_proves_exact_alias_corrective_memory_lift(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "paired-company-learning"

    payload = await run_pair_experiment(
        pool=resolver_db,
        output_dir=output_dir,
        run_id="pytest-corrective-memory-pair",
        system_version="pytest-system",
        llm_call_cost_usd=0.001,
    )

    spec = payload["spec"]
    report = payload["report"]
    metrics = report["metrics"]

    assert len(spec["cases"]) == 3
    assert {
        case["kind"] for case in spec["cases"]
    } == {"exact_alias_positive"}
    assert len(report["pairs"]) == 3
    assert metrics["pair_count"] == 3
    assert metrics["adaptive_correctness_rate"] == 1.0
    assert metrics["frozen_correctness_rate"] == 0.0
    assert metrics["adaptive_minus_frozen_correctness"] == 1.0
    assert metrics["llm_calls_avoided"] == 3
    assert metrics["adaptive_only_correct_count"] == 3
    assert report["incidents"] == []
    assert report["status"] == "observed"
    assert len(
        {
            expectation["tenant_id"]
            for case in spec["cases"]
            for expectation in (
                case["adaptive_expectation"],
                case["frozen_expectation"],
            )
        }
    ) == 6
    assert len(
        {
            result["lineage"]["training_observation_id"]
            for pair in report["pairs"]
            for result in (pair["adaptive"], pair["frozen"])
        }
    ) == 6
    assessments = {
        (item["case_id"], item["arm"]): item
        for item in report["assessments"]
    }
    cases_by_id = {case["case_id"]: case for case in spec["cases"]}
    expected_admission = {
        "held-out-renewal": True,
        "held-out-support": False,
        "held-out-risk": False,
    }

    for pair in report["pairs"]:
        assert (
            pair["adaptive"]["consumer_fate"]
            == "resolved_for_consumer"
        )
        assert (
            pair["adaptive"]["decision_source"]
            == "governed_exact_alias_replay"
        )
        assert pair["adaptive"]["llm_call_count"] == 0
        assert len(pair["adaptive"]["lineage"]["model_ids"]) == (
            cases_by_id[pair["case_id"]]["adaptive_expectation"][
                "expected_model_count"
            ]
        )
        assert pair["adaptive"]["source_semantic_admitted"] is (
            expected_admission[pair["case_id"]]
        )
        assert pair["frozen"]["consumer_fate"] in {
            "review",
            "abstained",
        }
        assert pair["frozen"]["decision_source"] == "llm"
        assert pair["frozen"]["llm_call_count"] == 1
        assert pair["frozen"]["lineage"]["model_ids"] == []
        assert pair["adaptive"]["observed_safety_incidents"] == []
        assert pair["frozen"]["observed_safety_incidents"] == []
        assert assessments[(pair["case_id"], "adaptive")]["terminal_fate"] == (
            "correct_resolution"
        )
        assert assessments[(pair["case_id"], "adaptive")]["correct"] is True
        assert assessments[(pair["case_id"], "frozen")]["correct"] is False

    artifact_path = output_dir / "company_learning_scenario_evidence.json"
    assert artifact_path.is_file()
    persisted = json.loads(artifact_path.read_text())
    assert persisted == payload
    assert len(payload["report_digest"]) == 64


async def test_frozen_ingest_view_preserves_ordinary_manual_alias(
    resolver_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    learned_customer_id = uuid7()
    ordinary_resource_id = uuid7()
    async with resolver_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT DO NOTHING",
            tenant_id,
        )
    alias_repo = EntityAliasRepo(resolver_db)
    await alias_repo.insert_alias(
        phrase="NBI",
        resolved_entity_ref={
            "type": "customer",
            "id": str(learned_customer_id),
        },
        source="ingestion",
        confidence=0.99,
        tenant_id=tenant_id,
        extra_metadata={
            "identity_basis_class": "independently_adjudicated",
            "identity_basis_ref": f"clarification-request:{uuid7()}",
        },
    )
    await alias_repo.insert_alias(
        phrase="OMS",
        resolved_entity_ref={
            "type": "resource",
            "id": str(ordinary_resource_id),
        },
        source="ingestion",
        confidence=0.99,
        tenant_id=tenant_id,
    )

    observation_id = await _ingest_slack(
        pool=resolver_db,
        tenant_id=tenant_id,
        alias_repo=alias_repo,
        text="NBI and OMS are referenced",
        channel="C-FROZEN-INGEST-CONTROL",
        occurred_at=datetime.now(timezone.utc),
        corrective_memory_reuse_enabled=False,
    )

    async with resolver_db.acquire() as conn:
        entities = await conn.fetchval(
            """
            SELECT entities_mentioned
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            observation_id,
        )
    assert {
        "type": "resource",
        "id": str(ordinary_resource_id),
    } in entities
    assert {
        "type": "customer",
        "id": str(learned_customer_id),
    } not in entities
