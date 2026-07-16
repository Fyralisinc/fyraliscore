from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from lib.evaluation.company_learning_experiment import (
    RecurrenceCaseKind,
)
from scripts.run_company_learning_negative_controls_db import (
    ARTIFACT_NAME,
    run_negative_control_experiment_db,
)


pytestmark = pytest.mark.integration


async def test_negative_controls_fail_closed_on_real_postgres(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    evidence = await run_negative_control_experiment_db(
        pool=resolver_db,
        output_dir=tmp_path,
        run_id="pytest-company-learning-negative-controls",
        system_version="pytest-system",
        llm_call_cost_usd=0.001,
    )

    assert len(evidence.spec.cases) == 4
    assert len(evidence.pairs) == 4
    assert {case.case_id for case in evidence.spec.cases} == {
        pair.case_id for pair in evidence.pairs
    }
    assert {case.kind for case in evidence.spec.cases} == {
        RecurrenceCaseKind.CONTEXTUAL_PHRASE_NEGATIVE,
        RecurrenceCaseKind.UNRELATED_NEGATIVE_CONTROL,
        RecurrenceCaseKind.HOMONYM_LOCAL_ASSOCIATION,
        RecurrenceCaseKind.CONFLICTING_SOURCE_HINT,
    }
    assert (
        len(
            {
                result.tenant_id
                for pair in evidence.pairs
                for result in (pair.adaptive, pair.frozen)
            }
        )
        == 8
    )
    assert evidence.report.status == "observed"
    assert evidence.report.incidents == ()
    assert evidence.report.metrics.pair_count == 4
    assert evidence.report.metrics.complete_terminal_fate_rate == 1.0
    assert evidence.report.metrics.adaptive_unsafe_count == 0
    assert evidence.report.metrics.frozen_unsafe_count == 0

    cases = {case.case_id: case for case in evidence.spec.cases}
    for pair in evidence.pairs:
        for result in (pair.adaptive, pair.frozen):
            expectation = cases[pair.case_id].expectation_for(result.arm)
            assert result.tenant_id == expectation.tenant_id
            assert len(result.lineage.model_ids) == expectation.expected_model_count
            assert result.observed_safety_incidents == frozenset()
        if pair.case_id == "conflicting-source-hint":
            assert pair.adaptive.resolved_entity_ref is None
            assert pair.adaptive.decision_source != "governed_exact_alias_replay"
            assert pair.frozen.resolved_entity_ref is None
            assert pair.frozen.observed_safety_incidents == frozenset()
        else:
            assert pair.adaptive.resolved_entity_ref is None
            assert pair.frozen.resolved_entity_ref is None

    artifact_path = tmp_path / ARTIFACT_NAME
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["report"]["pair_results_digest"] == (
        evidence.report.pair_results_digest
    )
    assert payload["evidence_digest"] == evidence.digest
