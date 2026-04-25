"""Unit tests for TemplateDiffTranslator."""

from __future__ import annotations

from datetime import datetime, timezone

from lsob_baselines.diff_translator import TemplateDiffTranslator
from lsob_contracts import DiffOp, Trigger


def _trigger(kind: str = "commitment_at_risk") -> Trigger:
    return Trigger(
        trigger_id="trg-test",
        kind=kind,
        payload={"entity_ref": "C-ingest"},
        timestamp=datetime(2026, 1, 17, tzinfo=timezone.utc),
    )


def test_template_translator_returns_valid_diff_op():
    t = TemplateDiffTranslator()
    diff = t.translate(
        trigger=_trigger(),
        retrieved_context="Alice reports slipping on the ingest pipeline",
        evidence_signal_ids=["s5", "s8"],
        entities=["C-ingest"],
    )
    assert isinstance(diff, DiffOp)
    # round-trip through pydantic to confirm schema validity.
    DiffOp.model_validate(diff.model_dump(mode="json"))
    assert diff.trigger_id == "trg-test"
    assert diff.claim_ops, "expected at least one claim op"
    claim = diff.claim_ops[0]
    assert claim.proposition_kind == "at_risk_of_slipping"
    assert "s5" in claim.evidence_signal_ids
    assert claim.entities == ["C-ingest"]
    assert diff.act_ops, "slip-like context should produce an act op"
    assert diff.act_ops[0].entity_ref == "C-ingest"
    assert diff.rationale and "dummy-llm" in diff.rationale


def test_template_translator_customer_path():
    t = TemplateDiffTranslator()
    diff = t.translate(
        trigger=Trigger(
            trigger_id="trg-cust",
            kind="customer_health",
            payload={"customer_ref": "acme"},
            timestamp=datetime(2026, 1, 20, tzinfo=timezone.utc),
        ),
        retrieved_context="Acme escalated — dashboard issue now P1.",
        evidence_signal_ids=["s9"],
        entities=["Acme"],
    )
    assert diff.claim_ops[0].proposition_kind == "customer_at_risk"


def test_template_translator_no_context_still_valid():
    t = TemplateDiffTranslator()
    diff = t.translate(
        trigger=_trigger(kind="unknown"),
        retrieved_context="",
        evidence_signal_ids=[],
        entities=[],
    )
    assert isinstance(diff, DiffOp)
    assert 0.0 <= diff.claim_ops[0].asserted_confidence <= 1.0
