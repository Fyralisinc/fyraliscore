"""Reproducible bounded production-path proof of post-fix retrieval maturity."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid5

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.retrieval_evolution import evaluate_retrieval_evolution
from lib.shared.types import ObservationRow
from services.reasoning.retrieval.assembler import ContextBundle, _select_observations
from services.reasoning.retrieval.config import RetrievalConfig
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.context_use import summarize_context_use
from services.reasoning.think.diff_schema import RawDiff
from services.reasoning.think.prompt import _build_retrieval_guidance_section


def run_bounded_retrieval_evolution_postfix() -> dict[str, Any]:
    batches = []
    for sequence in range(1, 10):
        model_count, quality = (
            (0, 0.0) if sequence <= 3 else
            (4, 2.5) if sequence <= 6 else (8, 8.0)
        )
        batches.append(_run_batch(sequence, model_count=model_count, quality_mass=quality))
    evaluation = evaluate_retrieval_evolution(batches)
    objective = {
        "schema_version": "bounded-retrieval-evolution-postfix-objective-v1",
        "population": {"batches": 9, "signals_per_batch": 12, "signals": 108},
        "evaluation": evaluation,
        "production_paths": [
            "retrieval.assembler._select_observations",
            "think.context_use.summarize_context_use",
            "think.prompt._build_retrieval_guidance_section",
        ],
        "proof_boundary": (
            "Deterministic bounded batch scenario through production selection, prompt "
            "guidance and context-use grading; not a replacement for the immutable "
            "45-batch historical company simulation."
        ),
    }
    objective["objective_sha256"] = canonical_sha256(objective)
    return objective


_NAMESPACE = UUID("18c22ad5-2e82-4aa4-bfcc-e2ecb77014a5")


def _id(value: str):
    return uuid5(_NAMESPACE, value)


def _observation(tenant_id, batch: int, sequence: int) -> ObservationRow:
    now = datetime(2026, 7, 17, tzinfo=timezone.utc)
    return ObservationRow(
        id=_id(f"batch:{batch}:observation:{sequence}"), tenant_id=tenant_id,
        occurred_at=now, ingested_at=now,
        kind="signal", source_channel="slack:message", source_actor_ref=None,
        actor_id=None, content={"text": f"batch signal {sequence}"},
        content_text=f"batch signal {sequence}", embedding=None,
        embedding_pending=False, trust_tier="derived", external_id=None,
        cause_id=None, sequence_num=sequence, entities_mentioned=[],
    )


def _run_batch(sequence: int, *, model_count: int, quality_mass: float) -> dict:
    tenant_id = _id(f"batch:{sequence}:tenant")
    observations = [_observation(tenant_id, sequence, i) for i in range(12)]
    trigger = TriggerContext(
        kind="T1", subkind="event_batch", tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[row.id for row in observations[:10]],
        seed_signature={"raw_observation_reopening_reasons": ["contradiction"]
                        if model_count >= 8 else []},
    )
    result = RetrievalResult(trigger=trigger, observations=list(observations))
    selected_observations, observation_selection = _select_observations(
        result, list(observations),
        cfg=RetrievalConfig(
            observation_context_mode="model_gap", assembler_budget_observations=10,
            t1_event_batch_raw_observation_floor=10,
            t1_event_batch_raw_source_floor=10, historical_observation_cap=1,
        ),
        budget_observations=10, explicit_budget=False,
        selected_model_count=model_count, selected_model_quality_mass=quality_mass,
    )
    model_ids = [_id(f"batch:{sequence}:model:{index}") for index in range(model_count)]
    bundle = ContextBundle(observations=selected_observations, notes={
        "model_selection": {"selected_count": model_count,
                            "selected_model_ids": [str(mid) for mid in model_ids],
                            "pathway_survival": {}},
        "observation_selection": observation_selection,
    })
    references = model_ids[:max(1, min(4, model_count))]
    trace = (
        f"Raw evidence {selected_observations[0].id} establishes cold-start memory."
        if not references else
        "Selected semantic memory " + " ".join(str(mid) for mid in references)
        + f" verified against fresh trigger {selected_observations[0].id}"
    )
    if model_ids:
        guidance = "\n".join(_build_retrieval_guidance_section(
            bundle, selected_model_ids={str(mid) for mid in model_ids},
            graph_model_ids=set()))
        if "models_are_primary=true" not in guidance:
            raise AssertionError("production prompt omitted Model-first policy")
    return {"sequence": sequence, "context_use": summarize_context_use(
        bundle, RawDiff(trigger_ref=_id(f"batch:{sequence}:trigger"),
                        tenant_id=tenant_id, reasoning_trace=trace))}


__all__ = ["run_bounded_retrieval_evolution_postfix"]
