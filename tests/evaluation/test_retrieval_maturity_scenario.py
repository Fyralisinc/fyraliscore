"""Bounded batch proof that retrieval matures from evidence to Models."""

from datetime import datetime, timezone
from uuid import uuid4

from lib.evaluation.retrieval_evolution import evaluate_retrieval_evolution
from lib.shared.types import ObservationRow
from services.reasoning.retrieval.assembler import ContextBundle, _select_observations
from services.reasoning.retrieval.config import RetrievalConfig
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.context_use import summarize_context_use
from services.reasoning.think.diff_schema import RawDiff
from services.reasoning.think.prompt import _build_retrieval_guidance_section


def _observation(tenant_id, sequence: int) -> ObservationRow:
    now = datetime.now(timezone.utc)
    return ObservationRow(
        id=uuid4(), tenant_id=tenant_id, occurred_at=now, ingested_at=now,
        kind="signal", source_channel="slack:message", source_actor_ref=None,
        actor_id=None, content={"text": f"batch signal {sequence}"},
        content_text=f"batch signal {sequence}", embedding=None,
        embedding_pending=False, trust_tier="derived", external_id=None,
        cause_id=None, sequence_num=sequence, entities_mentioned=[],
    )


def _run_batch(sequence: int, *, model_count: int, quality_mass: float) -> dict:
    tenant_id = uuid4()
    observations = [_observation(tenant_id, i) for i in range(12)]
    trigger = TriggerContext(
        kind="T1", subkind="event_batch", tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[row.id for row in observations[:10]],
        seed_signature={
            "raw_observation_reopening_reasons": ["contradiction"]
            if model_count >= 8
            else []
        },
    )
    result = RetrievalResult(trigger=trigger, observations=list(observations))
    selected_observations, observation_selection = _select_observations(
        result,
        list(observations),
        cfg=RetrievalConfig(
            observation_context_mode="model_gap",
            assembler_budget_observations=10,
            t1_event_batch_raw_observation_floor=10,
            t1_event_batch_raw_source_floor=10,
            historical_observation_cap=1,
        ),
        budget_observations=10,
        explicit_budget=False,
        selected_model_count=model_count,
        selected_model_quality_mass=quality_mass,
    )
    model_ids = [uuid4() for _ in range(model_count)]
    bundle = ContextBundle(
        observations=selected_observations,
        notes={
            "model_selection": {
                "selected_count": model_count,
                "selected_model_ids": [str(mid) for mid in model_ids],
                "pathway_survival": {},
            },
            "observation_selection": observation_selection,
        },
    )
    # This represents the production output contract: mature batches account
    # for selected semantic memory first; raw evidence is cited only when used.
    references = model_ids[: max(1, min(4, model_count))]
    if not references and selected_observations:
        trace = f"Raw evidence {selected_observations[0].id} establishes cold-start memory."
    else:
        trace = "Selected semantic memory " + " ".join(str(mid) for mid in references)
        if selected_observations:
            trace += f" verified against fresh trigger {selected_observations[0].id}"
    context_use = summarize_context_use(
        bundle,
        RawDiff(trigger_ref=uuid4(), tenant_id=tenant_id, reasoning_trace=trace),
    )
    if model_ids:
        guidance = "\n".join(
            _build_retrieval_guidance_section(
                bundle,
                selected_model_ids={str(mid) for mid in model_ids},
                graph_model_ids=set(),
            )
        )
        assert "models_are_primary=true" in guidance
        assert "raw_reopening_reasons=" in guidance
    return {"sequence": sequence, "context_use": context_use}


def test_nine_batch_cold_to_mature_scenario_meets_retrieval_policy() -> None:
    batches = []
    for sequence in range(1, 10):
        if sequence <= 3:
            batches.append(_run_batch(sequence, model_count=0, quality_mass=0.0))
        elif sequence <= 6:
            batches.append(_run_batch(sequence, model_count=4, quality_mass=2.5))
        else:
            batches.append(_run_batch(sequence, model_count=8, quality_mass=8.0))

    report = evaluate_retrieval_evolution(batches)

    assert report["verdict"] == "meets_preregistered_policy"
    assert report["measurements"]["early_observation_selection_share"] == 1.0
    assert report["measurements"]["late_model_selection_share"] == 8 / 11
    assert report["measurements"]["late_model_reference_share"] == 0.8
    assert report["measurements"]["late_raw_observation_reason_coverage"] == 1.0
    assert all(len(batch["context_use"]["selected_observation_ids"]) > 1 for batch in batches)
    for batch in batches[-3:]:
        assert batch["context_use"]["selected_historical_observation_count"] == 1
        assert "contradiction" in batch["context_use"][
            "raw_observation_reopening_reasons"
        ]
