from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lib.evaluation.company_learning_customer_lifecycle import (
    CustomerAliasIntervalEvidence,
    CustomerLifecycleObservation,
    CustomerRef,
    CustomerResolutionProbe,
    ResolutionProbeCategory,
    ResolutionProbeRole,
    build_customer_lifecycle_population,
    evaluate_customer_lifecycle_population,
    load_customer_lifecycle_population,
)


FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "company_learning"
    / "held_out_customer_lifecycle_population_v1.jsonl"
)


def test_committed_customer_lifecycle_registry_is_deterministic() -> None:
    generated = build_customer_lifecycle_population()
    fixture = load_customer_lifecycle_population(FIXTURE)

    assert fixture == generated
    assert fixture.digest == generated.digest
    assert len(fixture.cases) == 8
    assert sum(case.reuse_initial_identity for case in fixture.cases) == 4
    assert len({case.digest for case in fixture.cases}) == 8


def test_safe_lifecycle_evidence_is_continuously_observed() -> None:
    population = build_customer_lifecycle_population()
    observations = _safe_observations()

    report = evaluate_customer_lifecycle_population(
        population=population,
        observations=observations,
    )

    assert report.status == "observed"
    assert report.case_count == 8
    assert report.observed_case_count == 8
    assert report.unsupported_case_count == 0
    assert report.violating_case_count == 0
    assert report.runtime_support_rate.point_estimate == 1.0
    assert report.runtime_support_rate.lower_95 < 1.0
    assert report.rename_continuity_rate.point_estimate == 1.0
    assert report.valid_time_resolution_accuracy.point_estimate == 1.0
    assert report.valid_time_resolution_accuracy.sample_size == 36
    assert report.stale_alias_rejection_rate.point_estimate == 1.0
    assert report.current_alias_safety_rate.point_estimate == 1.0
    assert report.historical_name_reuse_accuracy.point_estimate == 1.0
    assert report.historical_name_reuse_accuracy.sample_size == 8
    assert report.observation_immutability_rate.point_estimate == 1.0
    assert report.model_immutability_rate.point_estimate == 1.0
    assert report.archive_alias_rejection_rate.point_estimate == 1.0
    assert report.archived_mutation_rejection_rate.point_estimate == 1.0
    assert report.alias_interval_non_overlap_rate.point_estimate == 1.0
    assert report.tenant_isolation_rate.point_estimate == 1.0
    assert report.replay_idempotency_rate.point_estimate == 1.0


def test_one_wrong_historical_resolution_is_precisely_contradicted() -> None:
    population = build_customer_lifecycle_population()
    observations = list(_safe_observations())
    first = observations[0]
    probes = list(first.resolution_probes)
    historical_index = next(
        index
        for index, probe in enumerate(probes)
        if probe.role is ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME
    )
    probe = probes[historical_index]
    probes[historical_index] = probe.model_copy(
        update={"observed_ref": CustomerRef(id="wrong-customer")}
    )
    observations[0] = first.model_copy(update={"resolution_probes": tuple(probes)})

    report = evaluate_customer_lifecycle_population(
        population=population,
        observations=tuple(observations),
    )

    assert report.status == "contradicted"
    assert report.violating_case_count == 1
    assert report.historical_name_reuse_accuracy.point_estimate == pytest.approx(7 / 8)
    assert report.valid_time_resolution_accuracy.point_estimate == pytest.approx(
        35 / 36
    )
    assert report.rename_continuity_rate.point_estimate == 1.0
    assert report.observation_immutability_rate.point_estimate == 1.0


def test_unsupported_cases_remain_in_exact_population_accounting() -> None:
    population = build_customer_lifecycle_population()
    observations = list(_safe_observations())
    observations[-1] = CustomerLifecycleObservation(
        case_id=population.cases[-1].case_id,
        execution_status="unsupported",
        unsupported_reason="customer lifecycle API unavailable",
    )

    report = evaluate_customer_lifecycle_population(
        population=population,
        observations=tuple(observations),
    )

    assert report.status == "observed_with_gaps"
    assert report.observed_case_count == 7
    assert report.unsupported_case_count == 1
    assert report.runtime_support_rate.point_estimate == 7 / 8
    assert report.unsupported_reason_counts == {"customer lifecycle API unavailable": 1}
    assert report.rename_continuity_rate.sample_size == 7


def test_lifecycle_evaluator_rejects_selective_reruns_and_reuse_drift() -> None:
    population = build_customer_lifecycle_population()
    observations = _safe_observations()

    with pytest.raises(ValueError, match="exactly cover"):
        evaluate_customer_lifecycle_population(
            population=population,
            observations=observations[:-1],
        )
    with pytest.raises(ValueError, match="unique by case"):
        evaluate_customer_lifecycle_population(
            population=population,
            observations=(*observations[:-1], observations[0]),
        )

    no_reuse_case = population.cases[1]
    no_reuse_observation = observations[1]
    extra = CustomerResolutionProbe(
        probe_id="unexpected-reuse",
        role=ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME,
        phrase=no_reuse_case.initial_identity,
        categories=(
            ResolutionProbeCategory.VALID_TIME,
            ResolutionProbeCategory.HISTORICAL_NAME_REUSE,
        ),
        expected_ref=no_reuse_observation.canonical_ref_before,
        observed_ref=no_reuse_observation.canonical_ref_before,
    )
    drifted = list(observations)
    drifted[1] = no_reuse_observation.model_copy(
        update={
            "resolution_probes": (
                *no_reuse_observation.resolution_probes,
                extra,
            )
        }
    )
    with pytest.raises(ValueError, match="does not match sealed case"):
        evaluate_customer_lifecycle_population(
            population=population,
            observations=tuple(drifted),
        )

    wrong_phrase = list(observations)
    first = wrong_phrase[0]
    probes = list(first.resolution_probes)
    probes[0] = probes[0].model_copy(update={"phrase": "Unsealed Customer"})
    wrong_phrase[0] = first.model_copy(update={"resolution_probes": tuple(probes)})
    with pytest.raises(ValueError, match="phrase does not match sealed role"):
        evaluate_customer_lifecycle_population(
            population=population,
            observations=tuple(wrong_phrase),
        )


def _safe_observations() -> tuple[CustomerLifecycleObservation, ...]:
    population = build_customer_lifecycle_population()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    observations = []
    for index, case in enumerate(population.cases):
        ref = CustomerRef(id=f"customer-{index}")
        reused_ref = CustomerRef(id=f"customer-reused-{index}")
        isolation_ref = CustomerRef(id=f"customer-isolation-{index}")
        rename_at = start + timedelta(days=index * 10 + 2)
        archive_at = rename_at + timedelta(days=2)
        intervals = [
            CustomerAliasIntervalEvidence(
                phrase=case.initial_identity,
                resolved_ref=ref,
                valid_from=rename_at - timedelta(days=2),
                valid_until=rename_at,
                validity_reason="customer_renamed",
            ),
            CustomerAliasIntervalEvidence(
                phrase=case.renamed_identity,
                resolved_ref=ref,
                valid_from=rename_at,
                valid_until=archive_at,
                validity_reason="customer_archived",
            ),
        ]
        probes = [
            CustomerResolutionProbe(
                probe_id="old-before-rename",
                role=ResolutionProbeRole.PRE_RENAME_OLD_NAME,
                phrase=case.initial_identity,
                as_of=rename_at - timedelta(days=1),
                categories=(ResolutionProbeCategory.VALID_TIME,),
                expected_ref=ref,
                observed_ref=ref,
            ),
            CustomerResolutionProbe(
                probe_id="old-after-rename",
                role=ResolutionProbeRole.POST_RENAME_STALE_OLD_NAME,
                phrase=case.initial_identity,
                as_of=rename_at + timedelta(hours=1),
                categories=(
                    ResolutionProbeCategory.VALID_TIME,
                    ResolutionProbeCategory.STALE_ALIAS_REJECTION,
                ),
                expected_ref=None,
                observed_ref=None,
            ),
            CustomerResolutionProbe(
                probe_id="new-during-active",
                role=ResolutionProbeRole.CURRENT_RENAMED_NAME,
                phrase=case.renamed_identity,
                categories=(ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,),
                expected_ref=ref,
                observed_ref=ref,
            ),
            CustomerResolutionProbe(
                probe_id="new-pre-archive-delayed",
                role=ResolutionProbeRole.PRE_ARCHIVE_DELAYED_RENAMED_NAME,
                phrase=case.renamed_identity,
                as_of=rename_at + timedelta(days=1),
                categories=(
                    ResolutionProbeCategory.VALID_TIME,
                    ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,
                ),
                expected_ref=ref,
                observed_ref=ref,
            ),
            CustomerResolutionProbe(
                probe_id="new-after-archive",
                role=ResolutionProbeRole.POST_ARCHIVE_REJECTION,
                phrase=case.renamed_identity,
                as_of=archive_at,
                categories=(
                    ResolutionProbeCategory.VALID_TIME,
                    ResolutionProbeCategory.ARCHIVE_REJECTION,
                ),
                expected_ref=None,
                observed_ref=None,
            ),
            CustomerResolutionProbe(
                probe_id="wrong-tenant",
                role=ResolutionProbeRole.TENANT_ISOLATION,
                phrase=case.initial_identity,
                categories=(ResolutionProbeCategory.TENANT_ISOLATION,),
                expected_ref=isolation_ref,
                observed_ref=isolation_ref,
            ),
        ]
        if case.reuse_initial_identity:
            intervals.append(
                CustomerAliasIntervalEvidence(
                    phrase=case.initial_identity,
                    resolved_ref=reused_ref,
                    valid_from=archive_at + timedelta(days=1),
                )
            )
            probes.extend(
                (
                    CustomerResolutionProbe(
                        probe_id="reused-current",
                        role=ResolutionProbeRole.CURRENT_REUSED_OLD_NAME,
                        phrase=case.initial_identity,
                        categories=(
                            ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,
                            ResolutionProbeCategory.HISTORICAL_NAME_REUSE,
                        ),
                        expected_ref=reused_ref,
                        observed_ref=reused_ref,
                    ),
                    CustomerResolutionProbe(
                        probe_id="reused-historical",
                        role=ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME,
                        phrase=case.initial_identity,
                        as_of=rename_at - timedelta(days=1),
                        categories=(
                            ResolutionProbeCategory.VALID_TIME,
                            ResolutionProbeCategory.HISTORICAL_NAME_REUSE,
                        ),
                        expected_ref=ref,
                        observed_ref=ref,
                    ),
                )
            )
        observations.append(
            CustomerLifecycleObservation(
                case_id=case.case_id,
                canonical_ref_before=ref,
                canonical_ref_after_rename=ref,
                canonical_ref_after_archive=ref,
                resolution_probes=tuple(probes),
                alias_intervals=tuple(intervals),
                old_observation_before=(ref,),
                old_observation_after=(ref,),
                old_model_before=(ref,),
                old_model_after=(ref,),
                rename_replay_alias_count_before=2,
                rename_replay_alias_count_after=2,
                rename_replay_event_count_before=1,
                rename_replay_event_count_after=1,
                archive_replay_alias_count_before=2,
                archive_replay_alias_count_after=2,
                archive_replay_event_count_before=1,
                archive_replay_event_count_after=1,
                post_archive_rename_rejected=True,
                artifact_refs=(f"pytest:{case.case_id}",),
            )
        )
    return tuple(observations)
