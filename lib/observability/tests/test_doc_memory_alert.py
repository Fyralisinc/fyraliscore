"""The DocMemoryMintFailure alert rule loads and references the right metric.

Phase-2 observability (docs/plans/document-memory-substrate.md §10): the
mint path is failure-isolated (a re-resolution error never fails summarization),
so the only signal of a sustained failure is the alert on
``doc_memory_mint_failure_total``. This test parses the Grafana provisioning
YAML and asserts the rule is present, well-formed, and its PromQL expr targets
the correct (un-renamed) metric — guarding against the Task-#1 rename
accidentally pointing the alert at the wrong family.
"""
from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")


_ALERT_PATH = (
    Path(__file__).resolve().parents[3]
    / "observability"
    / "grafana"
    / "provisioning"
    / "alerting"
    / "alert-rules.yml"
)


def _load_rules() -> list[dict]:
    assert _ALERT_PATH.exists(), f"alert rules not found at {_ALERT_PATH}"
    doc = yaml.safe_load(_ALERT_PATH.read_text())
    rules: list[dict] = []
    for group in doc.get("groups", []):
        rules.extend(group.get("rules", []))
    assert rules, "no alert rules parsed"
    return rules


def test_alert_yaml_parses():
    # Loads without error -> valid YAML.
    _load_rules()


def test_doc_memory_mint_failure_rule_present_and_well_formed():
    rules = _load_rules()
    matches = [r for r in rules if r.get("title") == "DocMemoryMintFailure"]
    assert len(matches) == 1, "expected exactly one DocMemoryMintFailure rule"
    rule = matches[0]
    assert rule.get("uid") == "doc-memory-mint-failure"
    assert rule.get("condition") == "C"
    # A query node carries the PromQL expr.
    exprs = [
        node["model"]["expr"]
        for node in rule.get("data", [])
        if isinstance(node.get("model"), dict) and "expr" in node["model"]
    ]
    assert exprs, "rule has no expr"
    expr = exprs[0]
    # The expr must reference the (un-renamed) mint-failure metric — NOT the
    # renamed dispatch counter, and NOT the true mint counter.
    assert "doc_memory_mint_failure_total" in expr
    assert "doc_memory_models_minted_total" not in expr
    assert "doc_memory_enriched_t1_total" not in expr
