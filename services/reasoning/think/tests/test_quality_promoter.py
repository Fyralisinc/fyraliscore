from __future__ import annotations

import json

import pytest

from services.reasoning.think.quality_promoter import (
    evaluate_promoted_case,
    load_promoted_cases,
    promote_quality_cases,
    promoted_case_document,
)


def _case(
    flags=None,
    *,
    ratio=0.0,
    graph_selected=0,
    graph_used=False,
    edge_ops=0,
    non_relation_ops=0,
    no_edge_rationale=False,
):
    flags = list(flags or [])
    return {
        "case_id": "think-quality:abc",
        "flags": flags,
        "run": {
            "run_id": "abc",
            "selected_context_reference_ratio": ratio,
            "graph_selected_model_count": graph_selected,
            "edge_ops_count": edge_ops,
            "graph_non_relation_op_count": non_relation_ops,
            "graph_no_edge_rationale_present": no_edge_rationale,
        },
        "context_use": {
            "selected_context_reference_ratio": ratio,
            "graph_selected_model_count": graph_selected,
            "graph_context_used": graph_used,
            "edge_ops_count": edge_ops,
            "graph_non_relation_op_count": non_relation_ops,
            "graph_relation_contract_satisfied": (
                graph_selected == 0
                or edge_ops > 0
                or non_relation_ops > 0
                or no_edge_rationale
            ),
            "graph_no_edge_rationale_present": no_edge_rationale,
        },
    }


def test_promoted_known_failure_case_evaluates_pass() -> None:
    doc = promoted_case_document(
        _case(["unused_selected_context"]),
        expectation_mode="known_failure",
    )

    result = evaluate_promoted_case(doc)

    assert result["status"] == "pass"


def test_promoted_must_pass_case_enforces_context_contract() -> None:
    doc = promoted_case_document(
        _case([], ratio=0.5, graph_selected=2, graph_used=True, edge_ops=1),
        expectation_mode="must_pass",
    )

    assert evaluate_promoted_case(doc)["status"] == "pass"

    doc["case"]["context_use"]["graph_context_used"] = False
    result = evaluate_promoted_case(doc)

    assert result["status"] == "fail"
    assert "graph_context_not_used" in result["failures"]


def test_promoted_must_pass_accepts_explicit_no_edge_rationale() -> None:
    doc = promoted_case_document(
        _case(
            [],
            ratio=0.5,
            graph_selected=2,
            graph_used=True,
            no_edge_rationale=True,
        ),
        expectation_mode="must_pass",
    )

    assert evaluate_promoted_case(doc)["status"] == "pass"


def test_promoted_must_pass_accepts_graph_backed_model_mutation() -> None:
    doc = promoted_case_document(
        _case(
            [],
            ratio=0.5,
            graph_selected=2,
            graph_used=True,
            non_relation_ops=1,
        ),
        expectation_mode="must_pass",
    )

    assert evaluate_promoted_case(doc)["status"] == "pass"


def test_promote_quality_cases_writes_stable_json(tmp_path) -> None:
    paths = promote_quality_cases(
        [_case(["low_selected_context_use"])],
        output_dir=tmp_path,
        source={"tenant_id": "tenant-a"},
    )

    assert len(paths) == 1
    assert paths[0].name == "abc.json"
    payload = json.loads(paths[0].read_text())
    assert payload["case_id"] == "think-quality:abc"
    assert payload["source"]["tenant_id"] == "tenant-a"
    assert payload["expectation"]["expected_flags"] == ["low_selected_context_use"]
    assert load_promoted_cases(tmp_path)[0]["case_id"] == "think-quality:abc"


def test_promoted_case_requires_valid_mode() -> None:
    with pytest.raises(ValueError):
        promoted_case_document(_case(), expectation_mode="surprise")
