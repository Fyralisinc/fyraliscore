#!/usr/bin/env python3
"""Run the working-version company-learning assurance suite."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg
from lib.architecture_registry import load_architecture_registry
from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_assurance import (
    CompanyLearningAssuranceSummary,
    CorrectionAssurance,
    NegativeAssurance,
    PopulationAssurance,
    PositiveAssurance,
    SlackAssurance,
    VariantPopulationAssurance,
    validate_company_learning_assurance_components,
)
from lib.evaluation.proof import EvidenceTier
from lib.evaluation.slack_reconstruction_gold import (
    evaluate_slack_reconstruction,
    load_slack_reconstruction_gold,
)
from scripts.observe_slack_reconstruction_gold import (
    DEFAULT_GOLD,
    observe_existing_slack_reconstruction,
)
from scripts.run_company_learning_negative_controls_db import (
    ARTIFACT_NAME as NEGATIVE_ARTIFACT_NAME,
)
from scripts.run_company_learning_negative_controls_db import (
    run_negative_control_experiment_db,
)
from scripts.run_company_learning_correction_harness import (
    run_company_learning_correction_harness,
)
from scripts.run_company_learning_population_harness import (
    ARTIFACT_NAME as POPULATION_ARTIFACT_NAME,
)
from scripts.run_company_learning_population_harness import (
    run_population_experiment,
)
from scripts.run_company_learning_variant_population_harness import (
    ARTIFACT_NAME as VARIANT_POPULATION_ARTIFACT_NAME,
)
from scripts.run_company_learning_variant_population_harness import (
    run_variant_population_experiment,
)
from scripts.run_company_learning_vitals_harness import (
    _install_json_codec,
    _working_version_failures,
    run_joined_company_learning_vitals,
)


SUMMARY_ARTIFACT_NAME = "company_learning_assurance_summary.json"
SLACK_OBSERVATIONS_NAME = "slack_reconstruction_observations.jsonl"
SLACK_REPORT_NAME = "slack_reconstruction_existing_surface_report.json"
ROOT = Path(__file__).resolve().parents[1]
ARCHITECTURE_REGISTRY = ROOT / "architecture" / "registry.yaml"
IMPLEMENTATION_PLAN = (
    ROOT
    / "docs"
    / "plans"
    / "revised-reality-belief-intent-system-implementation.md"
)


async def run_company_learning_assurance_suite(
    *,
    database_url: str,
    output_dir: Path,
    run_id: str,
    system_version: str,
    llm_call_cost_usd: float = 0.001,
    slack_gold_path: Path = DEFAULT_GOLD,
) -> CompanyLearningAssuranceSummary:
    """Run the complete active company-learning assurance profile."""

    output_dir.mkdir(parents=True, exist_ok=True)
    positive_dir = output_dir / "positive"
    negative_dir = output_dir / "negative"
    population_dir = output_dir / "population"
    variant_dir = output_dir / "variant"
    slack_dir = output_dir / "slack"
    correction_dir = output_dir / "correction"

    positive_result = await run_joined_company_learning_vitals(
        database_url=database_url,
        report_dir=positive_dir,
        run_id=f"{run_id}:positive",
        system_version=system_version,
        llm_call_cost_usd=llm_call_cost_usd,
    )
    positive_failures = tuple(_working_version_failures(positive_result))
    positive_pair_path = (
        positive_dir / "company_learning_scenario_evidence.json"
    )
    positive_scorecard_path = (
        positive_result.output_dir / "vitals_scorecard.json"
    )
    positive_evaluation_path = (
        positive_result.output_dir / "company_learning_evaluation.json"
    )
    positive_bundle_path = (
        positive_result.output_dir / "company_learning_evidence_bundle.json"
    )
    positive_pair = _read_json(positive_pair_path)
    positive_scorecard = _read_json(positive_scorecard_path)
    positive_evaluation = _read_json(positive_evaluation_path)
    experiment = positive_scorecard["company_physics"]["experiments"][
        "corrective_memory_recurrence"
    ]
    positive_metrics = experiment["metrics"]

    negative_pool = await asyncpg.create_pool(
        database_url,
        min_size=1,
        max_size=6,
        init=_install_json_codec,
    )
    try:
        negative_evidence = await run_negative_control_experiment_db(
            pool=negative_pool,
            output_dir=negative_dir,
            run_id=f"{run_id}:negative",
            system_version=system_version,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        population_evidence = await run_population_experiment(
            pool=negative_pool,
            output_dir=population_dir,
            run_id=f"{run_id}:population",
            system_version=system_version,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        variant_population_evidence = (
            await run_variant_population_experiment(
                pool=negative_pool,
                output_dir=variant_dir,
                run_id=f"{run_id}:variant",
                system_version=system_version,
                llm_call_cost_usd=llm_call_cost_usd,
            )
        )
    finally:
        await negative_pool.close()
    negative_path = negative_dir / NEGATIVE_ARTIFACT_NAME
    population_path = population_dir / POPULATION_ARTIFACT_NAME
    variant_population_path = (
        variant_dir / VARIANT_POPULATION_ARTIFACT_NAME
    )
    correction_artifact = await run_company_learning_correction_harness(
        database_url=database_url,
        output_dir=correction_dir,
        run_id=f"{run_id}:correction",
        system_version=system_version,
    )
    correction_path = correction_dir / "correction_assurance.json"

    slack_cases = load_slack_reconstruction_gold(slack_gold_path)
    slack_observations = await observe_existing_slack_reconstruction(
        slack_cases
    )
    slack_report = evaluate_slack_reconstruction(
        cases=slack_cases,
        observations=slack_observations,
        run_id=f"{run_id}:slack",
        system_version=system_version,
        artifact_refs=(
            f"gold:{slack_gold_path.resolve()}",
            "observer:scripts/observe_slack_reconstruction_gold.py",
        ),
    )
    slack_dir.mkdir(parents=True, exist_ok=True)
    slack_observations_path = slack_dir / SLACK_OBSERVATIONS_NAME
    slack_observations_path.write_text(
        "".join(
            json.dumps(observation.model_dump(mode="json"), sort_keys=True)
            + "\n"
            for observation in slack_observations
        ),
        encoding="utf-8",
    )
    slack_report_path = slack_dir / SLACK_REPORT_NAME
    _write_json(
        slack_report_path,
        {
            "report": slack_report.model_dump(mode="json"),
            "report_digest": slack_report.digest,
        },
    )

    artifact_paths = {
        "positive_pair": str(positive_pair_path.resolve()),
        "positive_company_learning_evaluation": str(
            positive_evaluation_path.resolve()
        ),
        "positive_company_learning_evidence_bundle": str(
            positive_bundle_path.resolve()
        ),
        "negative_evidence": str(negative_path.resolve()),
        "population_evidence": str(population_path.resolve()),
        "variant_population_evidence": str(
            variant_population_path.resolve()
        ),
        "correction_evidence": str(correction_path.resolve()),
        "slack_observations": str(slack_observations_path.resolve()),
        "slack_report": str(slack_report_path.resolve()),
    }
    missing = tuple(
        f"required assurance artifact is missing: {name}={path}"
        for name, path in artifact_paths.items()
        if not Path(path).is_file()
    )
    negative_incidents = negative_evidence.report.incidents
    population_incidents = population_evidence.experiment_report.incidents
    variant_population_incidents = (
        variant_population_evidence.experiment_report.incidents
    )
    variant_population_report = (
        variant_population_evidence.population_report
    )
    variant_mechanism_metrics = (
        variant_population_evidence.mechanism_metrics
    )
    if (
        variant_population_report is None
        or variant_mechanism_metrics is None
    ):
        raise RuntimeError(
            "full variant population execution did not produce reports"
        )
    variant_failures = _variant_population_failures(
        variant_population_evidence
    )
    variant_has_invalid_mechanism = bool(
        variant_population_incidents
        or variant_population_report.adaptive_unsafe_rate.point_estimate > 0.0
        or variant_population_report.frozen_unsafe_rate.point_estimate > 0.0
        or variant_mechanism_metrics.hard_safety_incident_count
        or variant_mechanism_metrics.control_integrity_violation_count
        or (
            variant_mechanism_metrics.candidate_memory_mediated_success_rate
            != 1.0
        )
        or (
            variant_mechanism_metrics
            .adaptive_target_candidate_authorization_rate
            != 1.0
        )
        or (
            variant_mechanism_metrics.frozen_target_candidate_exposure_rate
            != 0.0
        )
    )
    blocking_failures = tuple(
        dict.fromkeys(
            (
                *missing,
                *(f"positive: {failure}" for failure in positive_failures),
                *(
                    "negative safety incident: "
                    f"{incident.case_id}/{incident.arm.value}/"
                    f"{incident.incident_class.value}"
                    for incident in negative_incidents
                ),
                *(
                    "population safety incident: "
                    f"{incident.case_id}/{incident.arm.value}/"
                    f"{incident.incident_class.value}"
                    for incident in population_incidents
                ),
                *(
                    "variant population safety incident: "
                    f"{incident.case_id}/{incident.arm.value}/"
                    f"{incident.incident_class.value}"
                    for incident in variant_population_incidents
                ),
                *variant_failures,
                *(
                    f"correction incident: {incident}"
                    for incident in correction_artifact.incidents
                ),
                *(
                    (
                        "correction assurance did not converge across every "
                        "sealed dependency and repair obligation",
                    )
                    if not correction_artifact.metrics.converged
                    else ()
                ),
            )
        )
    )
    positive_digests = {
        "report": str(positive_pair["report_digest"]),
        "company_learning_evaluation": canonical_sha256(
            positive_evaluation
        ),
        "company_learning_evidence_bundle": canonical_sha256(
            _read_json(positive_bundle_path)
        ),
    }
    negative_digests = {
        "evidence": negative_evidence.digest,
        "report": negative_evidence.report.digest,
        "plan": negative_evidence.plan_digest,
    }
    population_digests = {
        "evidence": population_evidence.digest,
        "registry": population_evidence.registry_population_digest,
        "report": canonical_sha256(
            population_evidence.population_report.model_dump(mode="json")
        ),
    }
    variant_population_digests = {
        "evidence": variant_population_evidence.digest,
        "registry": (
            variant_population_evidence.registry_population_digest
        ),
        "report": variant_population_report.digest,
        "experiment_report": (
            variant_population_evidence.experiment_report.digest
        ),
        "mechanism_metrics": canonical_sha256(
            variant_mechanism_metrics.model_dump(mode="json")
        ),
    }
    slack_digests = {
        "report": slack_report.digest,
        "gold_manifest": slack_report.gold_manifest_digest,
        "observations": slack_report.observation_digest,
    }
    correction_digests = {
        "artifact": correction_artifact.digest,
        **correction_artifact.component_digests,
    }
    proof_gaps = tuple(
        dict.fromkeys(
            (
                *(
                    f"positive: {gap}"
                    for gap in positive_pair["report"].get(
                        "proof_gaps",
                        (),
                    )
                ),
                *(
                    f"negative: {gap}"
                    for gap in negative_evidence.report.proof_gaps
                ),
                *(
                    f"population: {gap}"
                    for gap in population_evidence.experiment_report.proof_gaps
                    if not gap.startswith(
                        "Confidence intervals require a larger held-out "
                        "recurrence population."
                    )
                ),
                *(
                    f"variant_population: {gap}"
                    for gap in (
                        variant_population_evidence.experiment_report.proof_gaps
                    )
                ),
                *(
                    (
                        "variant_population: runtime coverage observed "
                        f"{variant_population_report.observed_pair_count}/"
                        f"{variant_population_report.pair_count} sealed cases; "
                        "unsupported variant strata remain explicitly "
                        "accounted for.",
                    )
                    if variant_population_report.unsupported_case_count
                    else ()
                ),
                *(
                    (
                        "population: runtime coverage observed "
                        f"{population_evidence.population_report.observed_pair_count}/"
                        f"{population_evidence.population_report.pair_count} "
                        "sealed cases; unsupported entity strata remain "
                        "explicitly accounted for.",
                    )
                    if population_evidence.population_report.unsupported_case_count
                    else ()
                ),
                *(f"slack: {gap}" for gap in slack_report.proof_gaps),
                *(
                    (
                        "suite: Slack reconstruction remains diagnostic and "
                        "non-blocking until the current surface closes its "
                        "explicit reconstruction gaps.",
                    )
                    if slack_report.status != "observed"
                    else ()
                ),
                *(
                    f"correction: {gap}"
                    for gap in correction_artifact.proof_gaps
                ),
            )
        )
    )
    architecture_digest = load_architecture_registry(
        ARCHITECTURE_REGISTRY
    ).digest
    implementation_plan_digest = hashlib.sha256(
        IMPLEMENTATION_PLAN.read_bytes()
    ).hexdigest()
    slack_scope_complete = bool(
        slack_report.status == "observed"
        and slack_report.metrics.case_count > 0
        and slack_report.metrics.supported_case_count
        == slack_report.metrics.case_count
    )
    summary = CompanyLearningAssuranceSummary(
        run_id=run_id,
        system_version=system_version,
        architecture_digest=architecture_digest,
        implementation_plan_digest=implementation_plan_digest,
        created_at=datetime.now(timezone.utc).isoformat(),
        status="failed" if blocking_failures else "working",
        positive=PositiveAssurance(
            status=str(experiment.get("status") or "unavailable"),
            pair_count=int(positive_metrics.get("pair_count") or 0),
            adaptive_correctness_rate=positive_metrics.get(
                "adaptive_correctness_rate"
            ),
            frozen_correctness_rate=positive_metrics.get(
                "frozen_correctness_rate"
            ),
            adaptive_minus_frozen_correctness=positive_metrics.get(
                "adaptive_minus_frozen_correctness"
            ),
            hard_failures=positive_failures,
            artifact_paths={
                key: value
                for key, value in artifact_paths.items()
                if key.startswith("positive_")
            },
            component_digests=positive_digests,
        ),
        negative=NegativeAssurance(
            status=negative_evidence.report.status,
            pair_count=negative_evidence.report.metrics.pair_count,
            safety_incident_count=len(negative_incidents),
            adaptive_unsafe_count=(
                negative_evidence.report.metrics.adaptive_unsafe_count
            ),
            frozen_unsafe_count=(
                negative_evidence.report.metrics.frozen_unsafe_count
            ),
            artifact_paths={
                "negative_evidence": artifact_paths["negative_evidence"]
            },
            component_digests=negative_digests,
        ),
        slack=SlackAssurance(
            status=slack_report.status,
            metrics=slack_report.metrics.model_dump(mode="json"),
            evidence_tier=EvidenceTier.E4,
            scope_complete=slack_scope_complete,
            open_world_complete=False,
            blocking_for_active_slice=True,
            artifact_paths={
                "slack_observations": artifact_paths["slack_observations"],
                "slack_report": artifact_paths["slack_report"],
            },
            component_digests=slack_digests,
        ),
        correction=CorrectionAssurance(
            status=correction_artifact.status,
            evidence_tier=EvidenceTier.E4,
            expected_dependency_count=(
                correction_artifact.metrics.expected_dependency_count
            ),
            discovered_dependency_count=(
                correction_artifact.metrics.discovered_dependency_count
            ),
            dependency_discovery_rate=(
                correction_artifact.metrics.dependency_discovery_rate
            ),
            immediate_fence_rate=(
                correction_artifact.metrics.immediate_fence_rate
            ),
            direct_repair_rate=(
                correction_artifact.metrics.direct_repair_rate
            ),
            recursive_repair_rate=(
                correction_artifact.metrics.recursive_repair_rate
            ),
            relation_retirement_rate=(
                correction_artifact.metrics.relation_retirement_rate
            ),
            projection_invalidation_rate=(
                correction_artifact.metrics.projection_invalidation_rate
            ),
            projection_rebuild_rate=(
                correction_artifact.metrics.projection_rebuild_rate
            ),
            residual_unsafe_debt_count=(
                correction_artifact.metrics.residual_unsafe_debt_count
            ),
            convergence_ratio=(
                correction_artifact.metrics.convergence_ratio
            ),
            replay_idempotent=correction_artifact.metrics.replay_idempotent,
            source_immutable=correction_artifact.metrics.source_immutable,
            tenant_isolated=correction_artifact.metrics.tenant_isolated,
            converged=correction_artifact.metrics.converged,
            incidents=correction_artifact.incidents,
            artifact_paths={
                "correction_evidence": artifact_paths["correction_evidence"]
            },
            component_digests=correction_digests,
        ),
        variant_population=VariantPopulationAssurance(
            status=(
                "failed"
                if variant_has_invalid_mechanism
                else (
                    "observed"
                    if (
                        variant_population_report.observed_pair_count
                        == variant_population_report.pair_count
                        and not (
                            variant_population_report.unsupported_case_count
                        )
                    )
                    else "observed_with_gaps"
                )
            ),
            evidence_tier=EvidenceTier.E4,
            registry_pair_count=variant_population_report.pair_count,
            observed_pair_count=(
                variant_population_report.observed_pair_count
            ),
            unsupported_case_count=(
                variant_population_report.unsupported_case_count
            ),
            runtime_support_rate=(
                variant_population_report.observed_pair_count
                / max(1, variant_population_report.pair_count)
            ),
            adaptive_correctness=(
                variant_population_report.adaptive_correctness
            ),
            frozen_correctness=(
                variant_population_report.frozen_correctness
            ),
            adaptive_minus_frozen_correctness=(
                variant_population_report.adaptive_minus_frozen_correctness
            ),
            adaptive_unsafe_rate=(
                variant_population_report.adaptive_unsafe_rate
            ),
            frozen_unsafe_rate=(
                variant_population_report.frozen_unsafe_rate
            ),
            mechanism_metrics=variant_mechanism_metrics,
            artifact_paths={
                "variant_population_evidence": artifact_paths[
                    "variant_population_evidence"
                ]
            },
            component_digests=variant_population_digests,
        ),
        population=PopulationAssurance(
            status=(
                "observed_with_gaps"
                if population_evidence.population_report.unsupported_case_count
                else "observed"
            ),
            registry_pair_count=(
                population_evidence.population_report.pair_count
            ),
            observed_pair_count=(
                population_evidence.population_report.observed_pair_count
            ),
            unsupported_case_count=(
                population_evidence.population_report.unsupported_case_count
            ),
            runtime_support_rate=(
                population_evidence.population_report.observed_pair_count
                / max(1, population_evidence.population_report.pair_count)
            ),
            metrics={
                "safety_incident_count": len(population_incidents),
                **{
                    key: value
                    for key, value in (
                        population_evidence.population_report.model_dump(
                            mode="json"
                        )
                    ).items()
                    if key
                    not in {
                        "strata_counts",
                        "observed_strata_counts",
                        "unsupported_strata_counts",
                        "unsupported_reason_counts",
                    }
                },
            },
            unsupported_strata_counts=(
                population_evidence.population_report.unsupported_strata_counts
            ),
            unsupported_reason_counts=(
                population_evidence.population_report.unsupported_reason_counts
            ),
            artifact_paths={
                "population_evidence": artifact_paths["population_evidence"]
            },
            component_digests=population_digests,
        ),
        proof_gaps=proof_gaps,
        blocking_failures=blocking_failures,
        component_digests={
            **{
                f"positive_{key}": value
                for key, value in positive_digests.items()
            },
            **{
                f"negative_{key}": value
                for key, value in negative_digests.items()
            },
            **{
                f"slack_{key}": value
                for key, value in slack_digests.items()
            },
            **{
                f"population_{key}": value
                for key, value in population_digests.items()
            },
            **{
                f"variant_population_{key}": value
                for key, value in variant_population_digests.items()
            },
            **{
                f"correction_{key}": value
                for key, value in correction_digests.items()
            },
        },
        artifact_paths=artifact_paths,
    )
    validate_company_learning_assurance_components(summary)
    summary_path = output_dir / SUMMARY_ARTIFACT_NAME
    _write_summary(summary, summary_path)
    _write_summary(summary, positive_dir / SUMMARY_ARTIFACT_NAME)
    from scripts.company_vitals import write_vitals_artifacts

    write_vitals_artifacts(positive_dir)
    return summary


def _variant_population_failures(evidence: Any) -> tuple[str, ...]:
    report = evidence.population_report
    metrics = evidence.mechanism_metrics
    if report is None or metrics is None:
        return ("variant population: full typed reports are missing",)
    failures: list[str] = []
    if report.pair_count != 24:
        failures.append(
            "variant population: sealed registry did not contain 24 cases"
        )
    if report.unsupported_case_count:
        failures.append(
            "variant population: "
            f"{report.unsupported_case_count} sealed cases were unsupported"
        )
    expected_metrics = {
        "adaptive correctness": report.adaptive_correctness.point_estimate,
        "adaptive-minus-frozen correctness": (
            report.adaptive_minus_frozen_correctness.point_estimate
        ),
        "candidate-memory-mediated success": (
            metrics.candidate_memory_mediated_success_rate
        ),
        "adaptive target authorization": (
            metrics.adaptive_target_candidate_authorization_rate
        ),
        "adaptive closed-set match": (
            metrics.adaptive_closed_set_match_rate
        ),
        "source immutability": metrics.source_immutability_rate,
        "both-arm model invocation": metrics.both_arms_one_llm_call_rate,
        "both-arm scripted target response": (
            metrics.both_arms_scripted_target_response_rate
        ),
        "frozen safe review or abstention": (
            metrics.frozen_safe_review_or_abstention_rate
        ),
    }
    for label, value in expected_metrics.items():
        if value != 1.0:
            failures.append(
                f"variant population: {label} was {value!r}, expected 1.0"
            )
    if report.frozen_correctness.point_estimate != 0.0:
        failures.append(
            "variant population: frozen correctness was "
            f"{report.frozen_correctness.point_estimate!r}, expected 0.0"
        )
    if report.adaptive_unsafe_rate.point_estimate > 0.0:
        failures.append(
            "variant population: adaptive unsafe rate was "
            f"{report.adaptive_unsafe_rate.point_estimate!r}, expected 0.0"
        )
    if report.frozen_unsafe_rate.point_estimate > 0.0:
        failures.append(
            "variant population: frozen unsafe rate was "
            f"{report.frozen_unsafe_rate.point_estimate!r}, expected 0.0"
        )
    if metrics.frozen_target_candidate_exposure_rate != 0.0:
        failures.append(
            "variant population: frozen target candidate exposure was "
            f"{metrics.frozen_target_candidate_exposure_rate!r}, expected 0.0"
        )
    if metrics.frozen_closed_set_match_rate != 0.0:
        failures.append(
            "variant population: frozen closed-set match rate was "
            f"{metrics.frozen_closed_set_match_rate!r}, expected 0.0"
        )
    if metrics.hard_safety_incident_count:
        failures.append(
            "variant population: mechanism evidence recorded "
            f"{metrics.hard_safety_incident_count} hard safety incidents"
        )
    if metrics.control_integrity_violation_count:
        failures.append(
            "variant population: mechanism evidence recorded "
            f"{metrics.control_integrity_violation_count} control-integrity "
            "violations"
        )
    return tuple(failures)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected one JSON object in {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_summary(
    summary: CompanyLearningAssuranceSummary,
    path: Path,
) -> None:
    _write_json(path, summary.artifact_payload())


async def _run(args: argparse.Namespace) -> int:
    database_url = args.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL or --database-url is required", file=sys.stderr)
        return 2
    summary = await run_company_learning_assurance_suite(
        database_url=database_url,
        output_dir=args.output_dir,
        run_id=args.run_id,
        system_version=args.system_version,
        llm_call_cost_usd=args.llm_call_cost_usd,
        slack_gold_path=args.slack_gold,
    )
    summary_path = args.output_dir / SUMMARY_ARTIFACT_NAME
    print(f"summary={summary_path}")
    print(
        "status={status} positive_lift={lift} negative_incidents={incidents} "
        "negative_status={negative} population={observed}/{registry} "
        "variant={variant_observed}/{variant_registry} "
        "slack_status={slack} correction_status={correction}".format(
            status=summary.status,
            lift=summary.positive.adaptive_minus_frozen_correctness,
            incidents=summary.negative.safety_incident_count,
            negative=summary.negative.status,
            observed=(
                summary.population.observed_pair_count
                if summary.population is not None
                else 0
            ),
            registry=(
                summary.population.registry_pair_count
                if summary.population is not None
                else 0
            ),
            variant_observed=summary.variant_population.observed_pair_count,
            variant_registry=summary.variant_population.registry_pair_count,
            slack=summary.slack.status,
            correction=summary.correction.status,
        )
    )
    for failure in summary.blocking_failures:
        print(f"assurance failure: {failure}", file=sys.stderr)
    return 2 if summary.blocking_failures else 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser = argparse.ArgumentParser(
        description=(
            "Run positive corrective-memory Vitals, negative safety controls, "
            "the sealed exact and variant held-out populations, Slack "
            "reconstruction, and a recursive correction convergence burn in "
            "one assurance command."
        )
    )
    parser.add_argument("--database-url", default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / f"company-learning-assurance-{timestamp}",
    )
    parser.add_argument(
        "--run-id",
        default=f"company-learning-assurance-{timestamp}",
    )
    parser.add_argument("--system-version", default="local-working-tree")
    parser.add_argument("--llm-call-cost-usd", type=float, default=0.001)
    parser.add_argument(
        "--slack-gold",
        type=Path,
        default=DEFAULT_GOLD,
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_run(_parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
