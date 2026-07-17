from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from services.reasoning.retrieval import assembler
from services.reasoning.retrieval.assembler import (
    AccessContext,
    _sort_models_by_retrieval_score,
    _supplement_exact_batch_anchor_models,
)
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


async def test_exact_batch_anchor_supplements_model_absent_from_initial_reservoir(
    monkeypatch,
):
    tenant_id = uuid4()
    unrelated = _model(
        tenant_id=tenant_id,
        natural="Nimbus migration remains active.",
        activation=1.0,
    )
    missing_match = _model(
        tenant_id=tenant_id,
        natural="Beta renewal evidence remains unresolved.",
        activation=0.4,
    )
    result = RetrievalResult(
        trigger=TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=tenant_id,
            seed_signature={
                "batch_signal_fragments": [{"text": "Beta renewal update"}],
            },
        ),
        models=[unrelated],
        model_scores={unrelated.id: 0.95},
    )

    class Connection:
        async def fetch(self, sql, query_tenant_id, patterns):
            assert "tenant_id = $1" in sql
            assert "status = 'active'" in sql
            assert query_tenant_id == tenant_id
            assert any("beta" in pattern for pattern in patterns)
            return [missing_match]

    monkeypatch.setattr(assembler, "hydrate_model_row", lambda row, **_: row)

    supplemented = await _supplement_exact_batch_anchor_models(
        result,
        AccessContext(tenant_id=tenant_id),
        Connection(),
    )

    assert {model.id for model in supplemented} == {unrelated.id, missing_match.id}
