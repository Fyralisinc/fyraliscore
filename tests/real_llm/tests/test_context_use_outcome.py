"""Real-LLM context-use outcome evals.

These tests are opt-in through the real_llm harness. They do not touch
the DB; they exercise the actual prompt + provider path and assert that
the model can use selected retrieval context to produce an evidence-
backed edge_op instead of a vague standalone claim.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow, ObservationRow
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.context_use import summarize_context_use
from services.reasoning.think.llm_reason import llm_reason
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test


def _model(
    *,
    tenant_id,
    model_id,
    born_from_event_id,
    natural,
    kind="state",
) -> ModelRow:
    now = datetime.now(timezone.utc)
    return ModelRow(
        id=model_id,
        tenant_id=tenant_id,
        born_from_event_id=born_from_event_id,
        proposition={"kind": kind, "subject": str(model_id), "assertion": natural},
        natural=natural,
        embedding=[0.0] * 768,
        scope_entities=[],
        scope_temporal={"type": "now"},
        confidence=0.72,
        activation=0.8,
        created_at=now,
        confidence_at_assertion=0.72,
        proposition_kind=kind,
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=300)
async def test_real_llm_uses_selected_models_to_emit_edge_op(provider):
    tenant_id = uuid7()
    obs_id = uuid7()
    source_id = uuid7()
    target_id = uuid7()
    now = datetime.now(timezone.utc)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text=(
            "The enterprise renewal is now at risk because the security "
            "review is blocked by missing SOC2 evidence."
        ),
        seed_occurred_at=now,
    )
    bundle = ContextBundle(
        observations=[
            ObservationRow(
                id=obs_id,
                tenant_id=tenant_id,
                occurred_at=now,
                ingested_at=now,
                kind="signal",
                source_channel="eval:context_use",
                content={},
                content_text=(
                    "The enterprise renewal is now at risk because the "
                    "security review is blocked by missing SOC2 evidence."
                ),
                trust_tier="authoritative",
                sequence_num=1,
            )
        ],
        models=[
            _model(
                tenant_id=tenant_id,
                model_id=source_id,
                born_from_event_id=obs_id,
                natural="Security review is blocked by missing SOC2 evidence.",
                kind="concern",
            ),
            _model(
                tenant_id=tenant_id,
                model_id=target_id,
                born_from_event_id=obs_id,
                natural="Enterprise renewal depends on completing security review.",
                kind="prediction",
            ),
        ],
        notes={
            "model_selection": {
                "selected_model_ids": [str(source_id), str(target_id)],
                "pathway_survival": {
                    "G": {
                        "selected_model_ids": [
                            str(source_id),
                            str(target_id),
                        ]
                    }
                },
            }
        },
    )

    diff, _ = await llm_reason(
        trigger,
        bundle,
        provider,
        triggering_content=trigger.seed_natural_text,
        reason_for_trigger=(
            "Outcome eval: connect the selected context Models when their "
            "relationship is the important new memory."
        ),
        max_attempts=1,
    )
    report = summarize_context_use(bundle, diff)

    assert report["edge_ops_touching_graph_models"] >= 1
    assert report["graph_selected_reference_count"] >= 2
    assert any(
        op.edge_kind in {"blocks", "early_warning_for", "weakens", "causes"}
        for op in diff.edge_ops
    )
