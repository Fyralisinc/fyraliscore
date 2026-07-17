from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from services.reasoning.retrieval.assembler import _sort_models_by_retrieval_score
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


def _model(*, tenant_id, natural: str, activation: float, status: str = "active"):
    return SimpleNamespace(
        id=uuid4(),
        tenant_id=tenant_id,
        natural=natural,
        proposition={},
        activation=activation,
        status=status,
    )


def test_current_batch_subject_anchor_outranks_unrelated_high_activation_model():
    tenant_id = uuid4()
    observation_id = uuid4()
    matching = _model(
        tenant_id=tenant_id,
        natural="Quartz initiative has an unresolved renewal-risk pattern.",
        activation=0.35,
    )
    unrelated = _model(
        tenant_id=tenant_id,
        natural="Nimbus migration is the company's highest-priority program.",
        activation=1.0,
    )
    result = RetrievalResult(
        trigger=TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=tenant_id,
            observation_id=observation_id,
            observation_ids=[observation_id],
            seed_signature={
                "batch": True,
                "batch_signal_fragments": [
                    {"observation_id": str(observation_id), "text": "Quartz renewal update"}
                ],
            },
        ),
        models=[unrelated, matching],
        model_scores={unrelated.id: 0.99, matching.id: 0.10},
    )
    models = list(result.models)

    _sort_models_by_retrieval_score(models, result)

    assert [model.id for model in models] == [matching.id, unrelated.id]


def test_archived_matching_model_does_not_receive_batch_anchor_priority():
    tenant_id = uuid4()
    active = _model(
        tenant_id=tenant_id,
        natural="Nimbus migration remains active.",
        activation=0.9,
    )
    archived_match = _model(
        tenant_id=tenant_id,
        natural="Quartz renewal pattern is obsolete.",
        activation=1.0,
        status="archived",
    )
    result = RetrievalResult(
        trigger=TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=tenant_id,
            seed_signature={
                "batch_signal_fragments": [{"text": "Quartz renewal update"}],
            },
        ),
        models=[archived_match, active],
        model_scores={archived_match.id: 0.1, active.id: 0.9},
    )
    models = list(result.models)

    _sort_models_by_retrieval_score(models, result)

    assert [model.id for model in models] == [active.id, archived_match.id]
