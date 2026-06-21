from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.models.propositions import validate_proposition
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.capability_probes import (
    maybe_inject_capability_probe_ops,
)
from services.reasoning.think.diff_schema import RawDiff
from services.reasoning.think.text_embedding import deterministic_text_embedding


def test_capability_probe_injects_supported_lifecycle_ops() -> None:
    tenant_id = uuid7()
    trigger_ref = uuid7()
    obs_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()
    stale_model_id = uuid7()
    fragment_text = (
        "Capability probe. capability_probe=true "
        "capability_probe_kinds=prediction,resource,ontology_gap,archive,"
        "evidence_attachment,question_policy. resource_ops ontology_gap_ops "
        "evidence attachment question_policy evaluate_at archive lifecycle."
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_occurred_at=datetime(2026, 6, 16, tzinfo=timezone.utc),
        seed_signature={
            "batch_signal_fragments": [
                {"observation_id": str(obs_id), "text": fragment_text}
            ]
        },
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=source_model_id,
                status="active",
                confidence=0.9,
                natural="Enterprise-control launch needs security review.",
                scope_actors=[],
                scope_entities=[{"type": "customer", "id": str(uuid7())}],
            ),
            SimpleNamespace(
                id=target_model_id,
                status="active",
                confidence=0.8,
                natural="Security exception approval is still pending.",
                scope_actors=[],
                scope_entities=[],
            ),
            SimpleNamespace(
                id=stale_model_id,
                status="active",
                confidence=0.4,
                natural="Older launch assumption is stale.",
                scope_actors=[],
                scope_entities=[],
            ),
        ]
    )
    raw = RawDiff(trigger_ref=trigger_ref, tenant_id=tenant_id)

    out = maybe_inject_capability_probe_ops(raw, trigger, bundle)

    inserted_props = [
        op.entry.get("proposition", {}).get("kind")
        for op in out.claim_ops
        if op.op == "insert" and isinstance(op.entry, dict)
    ]
    assert "prediction" in inserted_props
    assert len(out.resource_ops) == 1
    assert out.resource_ops[0].op == "create"
    assert out.resource_ops[0].payload["kind"] == "capacity"
    assert len(out.ontology_gap_ops) == 1
    assert out.ontology_gap_ops[0].source_model_id == source_model_id
    assert out.ontology_gap_ops[0].target_model_id == target_model_id
    assert any(
        op.op == "archive" and op.model_id == stale_model_id for op in out.claim_ops
    )
    assert any(
        op.op == "insert"
        and isinstance(op.entry, dict)
        and "felt rough" in str(op.entry.get("natural", "")).lower()
        for op in out.claim_ops
    )
    assert any(
        op.op == "insert"
        and isinstance(op.entry, dict)
        and "question_policy" in set(op.entry.get("domain_tags") or [])
        for op in out.claim_ops
    )
    for op in out.claim_ops:
        if op.op == "insert" and isinstance(op.entry, dict):
            validate_proposition(op.entry["proposition"])
            entry = {
                **op.entry,
                "tenant_id": tenant_id,
                "embedding": deterministic_text_embedding(op.entry["natural"]),
            }
            ModelCreate.model_validate(entry)
    assert "capability_probe: injected" in (out.reasoning_trace or "")


def test_capability_probe_ignores_ordinary_signals() -> None:
    tenant_id = uuid7()
    raw = RawDiff(trigger_ref=uuid7(), tenant_id=tenant_id)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_signature={
            "batch_signal_fragments": [
                {"observation_id": str(uuid7()), "text": "normal launch update"}
            ]
        },
    )

    out = maybe_inject_capability_probe_ops(raw, trigger, ContextBundle())

    assert out == raw
