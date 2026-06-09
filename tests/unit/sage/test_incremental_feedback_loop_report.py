"""Tests for incremental feedback-loop report architecture SLOs."""
from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from scripts.run_incremental_feedback_loop_stress import (
    SeededCompany,
    _summarize,
    _variant_text,
)
from services.execution.inquiry import (
    _declares_no_material_update,
    _reader_attribution_nonselected_limit,
    _reader_attribution_nonselected_min_score,
)


def test_summary_flags_architecture_slo_failures_despite_perfect_hits() -> None:
    tenant_id = uuid4()
    rows = []
    for idx in range(10):
        rows.append({
            "repetition": 1,
            "family_index": idx,
            "family": f"{idx:03d}_broad_portfolio",
            "archetype": "broad_portfolio",
            "retrieval_ms": 5000.0 + idx,
            "expected_case": True,
            "expected_final_hits": 1,
            "expected_best_final_rank": 1,
            "expected_best_sage_rank": 3,
            "expected_activated": 40,
            "expected_reasons": {"lexical": 4},
            "selected_count": 56,
            "evidence_count": 25,
            "quality_failure_modes": ["unused_selected_context"],
            "optimizer": {
                "negative_memory_inserts": 2,
                "metrics": {"canonical_validation_enqueued": 1.0},
            },
            "learned_counts": {
                "contextual_affordance_profiles": 10,
                "discovery_shortcuts": 10,
                "negative_memory": 30,
                "reader_decision_attributions": 3000,
            },
        })

    summary = _summarize(
        tenant_id=tenant_id,
        company=SeededCompany(
            tenant_id=tenant_id,
            family_cases=tuple(range(10)),
            total_models=150,
            insert_ms=100.0,
            sidecars={"models": 150},
        ),
        results=rows,
    )

    assert summary["expected_hit_rate"] == 1.0
    assert summary["expected_misses"] == 0
    assert summary["architecture_slos"]["retrieval_p95_ms"]["passed"] is False
    assert summary["architecture_slos"]["expected_evidence_ge_24_ratio"]["passed"] is False
    assert summary["architecture_slos"]["reader_attributions_per_case"]["passed"] is False
    assert summary["structural_findings"]
    assert summary["readiness"]["tier"] == "internal_dogfood"
    assert "retrieval_p95_slo" in summary["readiness"]["blockers"]


def test_summary_flags_late_trace_pressure_after_learning_saturates() -> None:
    tenant_id = uuid4()
    rows = []
    attributions = 0
    shortcuts = 0
    reinforced = 0
    contextual = 0
    for idx in range(1, 101):
        attributions += 250
        if idx <= 25:
            shortcuts += 4
            reinforced += 2
            contextual += 2
        rows.append({
            "case_index": idx,
            "repetition": idx,
            "family_index": idx,
            "family": f"{idx:03d}_compliance_dependency",
            "archetype": "compliance_dependency",
            "retrieval_ms": 1200.0,
            "expected_case": True,
            "expected_final_hits": 1,
            "expected_best_final_rank": 1,
            "expected_best_sage_rank": 1,
            "expected_activated": 4,
            "expected_reasons": {"lexical": 1},
            "passed": True,
            "selected_count": 8,
            "evidence_count": 8,
            "quality_failure_modes": [],
            "optimizer": {
                "negative_memory_inserts": 0,
                "metrics": {"canonical_validation_enqueued": 0.0},
            },
            "learned_counts": {
                "contextual_affordance_profiles": contextual,
                "discovery_shortcuts": shortcuts,
                "negative_memory": 0,
                "reader_decision_attributions": attributions,
                "reinforced_affordance_profiles": reinforced,
            },
        })

    summary = _summarize(
        tenant_id=tenant_id,
        company=SeededCompany(
            tenant_id=tenant_id,
            family_cases=tuple(range(10)),
            total_models=20000,
            insert_ms=100.0,
            sidecars={"models": 20000},
        ),
        results=rows,
    )

    pressure = summary["learning_pressure"]
    assert pressure["late_trace_pressure"] is True
    assert any(row["trace_pressure"] for row in pressure["quarters"][1:])
    assert any(
        "Late-run feedback mostly adds reader attribution trace rows" in finding
        for finding in summary["structural_findings"]
    )
    assert summary["readiness"]["tier"] == "design_partner_controlled"
    assert "late_trace_pressure" in summary["readiness"]["blockers"]
    assert summary["source_realism"]["multi_source_ingestion_validated"] is False


def test_weak_workspace_variants_remain_noop_under_hard_followups() -> None:
    case = SimpleNamespace(
        archetype=SimpleNamespace(key="weak_workspace_noise"),
        marker="RTS-weak",
        trigger=SimpleNamespace(
            seed_natural_text=(
                "Workspace chatter for customer-40: lunch notes, travel plans, "
                "and general team coordination. Marker RTS-weak."
            )
        ),
    )

    for repetition in range(2, 6):
        text = _variant_text(case, repetition)

        assert _declares_no_material_update(text.casefold()) is True
        assert "identify the current blocker" not in text
        assert "unresolved operational gate" not in text
        assert "required action" not in text


def test_reader_attribution_trace_pressure_knobs_are_env_tunable(monkeypatch) -> None:
    monkeypatch.setenv("SAGE_READER_ATTRIBUTION_NONSELECTED_LIMIT", "3")
    monkeypatch.setenv("SAGE_READER_ATTRIBUTION_NONSELECTED_MIN_SCORE", "0.82")

    assert _reader_attribution_nonselected_limit() == 3
    assert _reader_attribution_nonselected_min_score() == 0.82

    monkeypatch.setenv("SAGE_READER_ATTRIBUTION_NONSELECTED_LIMIT", "not-an-int")
    monkeypatch.setenv("SAGE_READER_ATTRIBUTION_NONSELECTED_MIN_SCORE", "nope")

    assert _reader_attribution_nonselected_limit() == 16
    assert _reader_attribution_nonselected_min_score() == 0.55
