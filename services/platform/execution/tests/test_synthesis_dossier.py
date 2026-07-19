from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4, uuid5

import pytest

from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from services.platform.execution.governed_learning_episode import (
    GovernedLearningEpisode,
    GovernedObservationAssertion,
)
from services.platform.execution.synthesis_dossier import (
    DossierContractError,
    EvidenceAnnotation,
    ModelHeadInput,
    assemble_synthesis_dossier,
)


NAMESPACE = UUID("718a3bc4-b93c-49e3-b0b8-9fbdcd191375")


def _assertion(
    tenant_id: UUID,
    *,
    observation_id: UUID | None = None,
    occurred_at: datetime | None = None,
    text: str = "Harbor release is delayed.",
    scope: str = "workstream:harbor-release",
    surface: str = "Harbor release",
    authority: str = "resolved",
    channel: str = "slack:message",
) -> GovernedObservationAssertion:
    observation_id = observation_id or uuid4()
    return GovernedObservationAssertion(
        tenant_id=tenant_id,
        observation_id=observation_id,
        occurred_at=occurred_at or datetime(2026, 7, 1, tzinfo=timezone.utc),
        source_channel=channel,
        assertion_text=text,
        evidence_address=f"observation:{observation_id}:content_text",
        evidence_field_path="content_text",
        evidence_span_start=0,
        evidence_span_end=min(len(surface), len(text)),
        governed_surface=surface,
        canonical_ref=scope,
        coordinate_authority=authority,  # type: ignore[arg-type]
        detection_id=uuid4(),
        trust_tier="authoritative" if channel.startswith("jira") else "unvetted",
    )


def _episode(
    tenant_id: UUID,
    assertions: list[GovernedObservationAssertion],
    *,
    scope: str = "workstream:harbor-release",
) -> GovernedLearningEpisode:
    ordered = tuple(sorted(assertions, key=lambda row: (row.occurred_at, str(row.observation_id))))
    return GovernedLearningEpisode(
        episode_id=f"GLE_{uuid5(NAMESPACE, f'{tenant_id}:{scope}').hex[:24]}",
        tenant_id=tenant_id,
        canonical_ref=scope,
        assertions=ordered,
        temporal_start=ordered[0].occurred_at,
        temporal_end=ordered[-1].occurred_at,
    )


def _model(
    scope: str,
    when: datetime,
    *,
    ordinal: int,
    accepted: bool = True,
) -> ModelHeadInput:
    model_id = uuid5(NAMESPACE, f"{scope}:model:{ordinal}")
    return ModelHeadInput(
        model_id=model_id,
        truth_version_id=uuid5(NAMESPACE, f"{model_id}:v1"),
        natural_text=f"Prior accepted state {ordinal}.",
        proposition={"kind": "belief", "abstraction_level": "atomic"},
        canonical_scope_ref=scope,
        accepted_current=accepted,
        truth_advanced_at=when,
        evidence_observation_ids=(uuid5(NAMESPACE, f"prior:{ordinal}"),),
    )


def test_dossier_is_deterministic_closed_and_provider_payload_hides_ids() -> None:
    tenant_id = uuid4()
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    later = _assertion(tenant_id, occurred_at=start + timedelta(hours=1), channel="jira:issue")
    earlier = _assertion(tenant_id, occurred_at=start, text="Harbor ownership is open.")
    models = [_model("workstream:harbor-release", start, ordinal=value) for value in (2, 1)]
    annotations = [
        EvidenceAnnotation("observation", earlier.observation_id, "direct", ("condition",)),
        EvidenceAnnotation("observation", later.observation_id, "contradictory", ("outcome",)),
    ]

    first = assemble_synthesis_dossier(
        tenant_id=tenant_id, episode=_episode(tenant_id, [later, earlier]),
        as_of_at=start + timedelta(days=1), model_heads=models,
        evidence_annotations=annotations,
    )
    second = assemble_synthesis_dossier(
        tenant_id=tenant_id, episode=_episode(tenant_id, [earlier, later]),
        as_of_at=start + timedelta(days=1), model_heads=reversed(models),
        evidence_annotations=reversed(annotations),
    )

    assert first == second
    assert first.event_order == ("O1", "O2")
    assert first.accepted_model_heads == ("M1", "M2")
    assert first.assembly_receipt.mechanism_opportunity == "mature"
    assert first.contradictory_evidence[0].object_handle == "O2"
    assert first.handles[0].independence_group == "unknown:slack:message"
    assert first.handles[1].independence_group == "unknown:jira:issue"
    payload = first.provider_payload()
    serialized = repr(payload)
    assert str(tenant_id) not in serialized
    assert str(earlier.observation_id) not in serialized
    assert str(models[0].model_id) not in serialized


def test_future_stale_and_malformed_objects_are_excluded_with_receipt() -> None:
    tenant_id = uuid4()
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    current = _assertion(tenant_id, occurred_at=now)
    future = _assertion(tenant_id, occurred_at=now + timedelta(days=1))
    models = [
        _model("workstream:harbor-release", now, ordinal=1, accepted=False),
        _model("workstream:harbor-release", now + timedelta(days=1), ordinal=2),
    ]

    dossier = assemble_synthesis_dossier(
        tenant_id=tenant_id, episode=_episode(tenant_id, [current, future]),
        as_of_at=now, model_heads=models,
    )

    assert dossier.event_order == ("O1",)
    assert dossier.accepted_model_heads == ()
    assert dossier.assembly_receipt.exclusion_reasons == {
        "future_model": 1, "future_observation": 1, "stale_model": 1,
    }
    assert dossier.assembly_receipt.mechanism_opportunity == "none"


@pytest.mark.parametrize("authority", ["provisional", "unresolved"])
def test_non_resolved_coordinates_fail_closed(authority: str) -> None:
    tenant_id = uuid4()
    assertion = _assertion(tenant_id, authority=authority)
    with pytest.raises(DossierContractError, match="provisional or unresolved"):
        assemble_synthesis_dossier(
            tenant_id=tenant_id, episode=_episode(tenant_id, [assertion]),
            as_of_at=assertion.occurred_at,
        )


def test_unknown_annotations_fail_handle_closure() -> None:
    tenant_id = uuid4()
    assertion = _assertion(tenant_id)
    with pytest.raises(DossierContractError, match="unknown object"):
        assemble_synthesis_dossier(
            tenant_id=tenant_id, episode=_episode(tenant_id, [assertion]),
            as_of_at=assertion.occurred_at,
            evidence_annotations=[EvidenceAnnotation("observation", uuid4(), "direct")],
        )


def test_source_channel_does_not_claim_independence_without_identity() -> None:
    tenant_id = uuid4()
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    first = _assertion(tenant_id, occurred_at=start)
    second = _assertion(tenant_id, occurred_at=start + timedelta(minutes=1))
    dossier = assemble_synthesis_dossier(
        tenant_id=tenant_id, episode=_episode(tenant_id, [first, second]),
        as_of_at=start + timedelta(hours=1),
        source_identities={first.observation_id: "document:A"},
    )
    assert [item.independence_group for item in dossier.handles] == [
        "source:document:A", "unknown:slack:message",
    ]


def test_runtime_dossier_has_no_wrapper_or_evaluator_contract_fields() -> None:
    tenant_id = uuid4()
    assertion = _assertion(tenant_id)
    dossier = assemble_synthesis_dossier(
        tenant_id=tenant_id, episode=_episode(tenant_id, [assertion]),
        as_of_at=assertion.occurred_at,
    )
    keys = set(asdict(dossier))
    forbidden = {
        "storyline_id", "expected_thesis", "expected_relation", "threshold",
        "batch_number", "barrier", "prompt", "score", "gold",
    }
    assert not keys & forbidden
    assert not forbidden & set(repr(dossier.provider_payload()).casefold().split("'"))


def test_all_twelve_development_batches_produce_scope_local_mechanical_dossiers() -> None:
    """Evaluation-only adapter exercises TI1 without exposing oracle data to runtime."""
    population = build_p6_population()
    tenant_id = uuid5(NAMESPACE, "ti1-twelve-batch")
    gold_by_signal = {row.signal_id: row for row in population.gold}
    observations_by_scope: dict[str, list[GovernedObservationAssertion]] = {}
    maturity: dict[str, list[int]] = {}
    inspected = 0
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)

    for batch in population.batches:
        current_ids: dict[str, UUID] = {}
        for signal in batch.signals:
            expected = gold_by_signal[signal.signal_id]
            if expected.canonical_ref is None or expected.entity_surface is None:
                continue
            observation_id = uuid5(NAMESPACE, signal.signal_id)
            current_ids[signal.signal_id] = observation_id
            observations_by_scope.setdefault(expected.canonical_ref, []).append(
                _assertion(
                    tenant_id, observation_id=observation_id,
                    occurred_at=start + timedelta(days=batch.batch_number, seconds=signal.position),
                    text=signal.text, scope=expected.canonical_ref,
                    surface=expected.entity_surface, channel=signal.source_channel,
                )
            )

        for scope, assertions in observations_by_scope.items():
            current = [row for row in assertions if row.occurred_at.date() == (start + timedelta(days=batch.batch_number)).date()]
            annotations: list[EvidenceAnnotation] = []
            for row in current:
                expected = gold_by_signal[next(
                    signal_id for signal_id, object_id in current_ids.items()
                    if object_id == row.observation_id
                )]
                if expected.role == "synthesis":
                    annotations.append(EvidenceAnnotation(
                        "observation", row.observation_id, "direct", ("condition", "outcome"),
                    ))
            prior_models = [
                _model(scope, start + timedelta(days=max(1, batch.batch_number - 1)), ordinal=value)
                for value in (1, 2)
            ] if batch.batch_number >= 2 else []
            dossier = assemble_synthesis_dossier(
                tenant_id=tenant_id,
                episode=_episode(tenant_id, assertions, scope=scope),
                as_of_at=start + timedelta(days=batch.batch_number, hours=23),
                model_heads=prior_models,
                evidence_annotations=annotations,
            )
            inspected += 1
            if dossier.assembly_receipt.mechanism_opportunity == "mature":
                maturity.setdefault(scope, []).append(batch.batch_number)
            assert dossier.scope["canonical_ref"] == scope
            assert all(
                item.canonical_scope_ref in {None, scope} for item in dossier.handles
            )

    null_assertion = _assertion(
        tenant_id, scope="workspace:general", surface="General workspace",
        text="The lunch entrance changed.",
    )
    null_dossier = assemble_synthesis_dossier(
        tenant_id=tenant_id,
        episode=_episode(tenant_id, [null_assertion], scope="workspace:general"),
        as_of_at=null_assertion.occurred_at,
    )

    assert inspected == 48
    assert maturity["workstream:atlas-release"] == [4]
    assert maturity["commitment:cobalt-renewal"] == [9]
    assert null_dossier.assembly_receipt.mechanism_opportunity == "none"
