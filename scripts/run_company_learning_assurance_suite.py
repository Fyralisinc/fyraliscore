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
    ActiveSurfacesAssurance,
    CanonicalReplacementAssurance,
    CompanyLearningAssuranceSummary,
    CorrectionAssurance,
    CustomerLifecycleAssurance,
    NegativeAssurance,
    PopulationAssurance,
    PositiveAssurance,
    RetentionAssurance,
    SlackAssurance,
    SourceBindingLifecycleAssurance,
    VariantCollisionAssurance,
    VariantPopulationAssurance,
    validate_company_learning_assurance_components,
)
from lib.evaluation.proof import EvidenceTier
from lib.evaluation.company_learning_variant_collisions import (
    VariantCollisionFamily,
)
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
from scripts.run_company_learning_active_surfaces_db import (
    ARTIFACT_NAME as ACTIVE_SURFACES_ARTIFACT_NAME,
)
from scripts.run_company_learning_active_surfaces_db import (
    run_active_surfaces_experiment,
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
from scripts.run_company_learning_variant_collisions_db import (
    ARTIFACT_NAME as VARIANT_COLLISION_ARTIFACT_NAME,
)
from scripts.run_company_learning_customer_lifecycle_db import (
    ARTIFACT_NAME as CUSTOMER_LIFECYCLE_ARTIFACT_NAME,
)
from scripts.run_company_learning_retention_db import (
    ARTIFACT_NAME as RETENTION_ARTIFACT_NAME,
)
from scripts.run_company_learning_retention_db import (
    run_company_learning_retention_experiment,
)
from scripts.run_company_learning_customer_lifecycle_db import (
    run_customer_lifecycle_experiment,
)
from scripts.run_company_learning_variant_collisions_db import (
    run_variant_collision_experiment,
)
from scripts.run_canonical_resource_replacement_db import (
    ARTIFACT_NAME as CANONICAL_REPLACEMENT_ARTIFACT_NAME,
)
from scripts.run_canonical_resource_replacement_db import (
    run_canonical_resource_replacement_experiment,
)
from scripts.run_source_identity_binding_lifecycle_db import (
    ARTIFACT_NAME as SOURCE_BINDING_LIFECYCLE_ARTIFACT_NAME,
)
from scripts.run_source_identity_binding_lifecycle_db import (
    run_source_identity_binding_lifecycle_experiment,
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
    ROOT / "docs" / "plans" / "revised-reality-belief-intent-system-implementation.md"
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
    collision_dir = output_dir / "collision"
    lifecycle_dir = output_dir / "customer-lifecycle"
    active_surfaces_dir = output_dir / "active-surfaces"
    retention_dir = output_dir / "retention"
    canonical_replacement_dir = output_dir / "canonical-replacement"
    source_binding_lifecycle_dir = output_dir / "source-binding-lifecycle"
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
    positive_pair_path = positive_dir / "company_learning_scenario_evidence.json"
    positive_scorecard_path = positive_result.output_dir / "vitals_scorecard.json"
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
        variant_population_evidence = await run_variant_population_experiment(
            pool=negative_pool,
            output_dir=variant_dir,
            run_id=f"{run_id}:variant",
            system_version=system_version,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        variant_collision_evidence = await run_variant_collision_experiment(
            pool=negative_pool,
            output_dir=collision_dir,
            run_id=f"{run_id}:collision",
            system_version=system_version,
        )
        customer_lifecycle_evidence = await run_customer_lifecycle_experiment(
            pool=negative_pool,
            output_dir=lifecycle_dir,
            run_id=f"{run_id}:customer-lifecycle",
            system_version=system_version,
        )
        active_surfaces_evidence = await run_active_surfaces_experiment(
            pool=negative_pool,
            output_dir=active_surfaces_dir,
            run_id=f"{run_id}:active-surfaces",
            system_version=system_version,
        )
        retention_report = await run_company_learning_retention_experiment(
            pool=negative_pool,
            output_dir=retention_dir,
            run_id=f"{run_id}:retention",
            system_version=system_version,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        canonical_replacement_evidence = (
            await run_canonical_resource_replacement_experiment(
                pool=negative_pool,
                output_dir=canonical_replacement_dir,
                run_id=f"{run_id}:canonical-replacement",
                system_version=system_version,
            )
        )
        source_binding_lifecycle_evidence = (
            await run_source_identity_binding_lifecycle_experiment(
                pool=negative_pool,
                output_dir=source_binding_lifecycle_dir,
                run_id=f"{run_id}:source-binding-lifecycle",
                system_version=system_version,
            )
        )
    finally:
        await negative_pool.close()
    negative_path = negative_dir / NEGATIVE_ARTIFACT_NAME
    population_path = population_dir / POPULATION_ARTIFACT_NAME
    variant_population_path = variant_dir / VARIANT_POPULATION_ARTIFACT_NAME
    variant_collision_path = collision_dir / VARIANT_COLLISION_ARTIFACT_NAME
    customer_lifecycle_path = lifecycle_dir / CUSTOMER_LIFECYCLE_ARTIFACT_NAME
    active_surfaces_path = active_surfaces_dir / ACTIVE_SURFACES_ARTIFACT_NAME
    retention_path = retention_dir / RETENTION_ARTIFACT_NAME
    canonical_replacement_path = (
        canonical_replacement_dir / CANONICAL_REPLACEMENT_ARTIFACT_NAME
    )
    source_binding_lifecycle_path = (
        source_binding_lifecycle_dir / SOURCE_BINDING_LIFECYCLE_ARTIFACT_NAME
    )
    correction_artifact = await run_company_learning_correction_harness(
        database_url=database_url,
        output_dir=correction_dir,
        run_id=f"{run_id}:correction",
        system_version=system_version,
    )
    correction_path = correction_dir / "correction_assurance.json"

    slack_cases = load_slack_reconstruction_gold(slack_gold_path)
    slack_observations = await observe_existing_slack_reconstruction(slack_cases)
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
            json.dumps(observation.model_dump(mode="json"), sort_keys=True) + "\n"
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
        "positive_company_learning_evaluation": str(positive_evaluation_path.resolve()),
        "positive_company_learning_evidence_bundle": str(
            positive_bundle_path.resolve()
        ),
        "negative_evidence": str(negative_path.resolve()),
        "population_evidence": str(population_path.resolve()),
        "variant_population_evidence": str(variant_population_path.resolve()),
        "variant_collision_evidence": str(variant_collision_path.resolve()),
        "customer_lifecycle_evidence": str(customer_lifecycle_path.resolve()),
        "active_surfaces_evidence": str(active_surfaces_path.resolve()),
        "retention_evidence": str(retention_path.resolve()),
        "canonical_replacement_evidence": str(
            canonical_replacement_path.resolve()
        ),
        "source_binding_lifecycle_evidence": str(
            source_binding_lifecycle_path.resolve()
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
    variant_population_report = variant_population_evidence.population_report
    variant_mechanism_metrics = variant_population_evidence.mechanism_metrics
    if variant_population_report is None or variant_mechanism_metrics is None:
        raise RuntimeError("full variant population execution did not produce reports")
    variant_failures = _variant_population_failures(variant_population_evidence)
    variant_has_invalid_mechanism = bool(
        variant_population_incidents
        or variant_population_report.adaptive_unsafe_rate.point_estimate > 0.0
        or variant_population_report.frozen_unsafe_rate.point_estimate > 0.0
        or variant_mechanism_metrics.hard_safety_incident_count
        or variant_mechanism_metrics.control_integrity_violation_count
        or (variant_mechanism_metrics.candidate_memory_mediated_success_rate != 1.0)
        or (
            variant_mechanism_metrics.adaptive_target_candidate_authorization_rate
            != 1.0
        )
        or (variant_mechanism_metrics.frozen_target_candidate_exposure_rate != 0.0)
    )
    variant_collision_report = variant_collision_evidence.report
    source_native_collision_report = variant_collision_report.stratum_reports[
        "collision_family"
    ][VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER.value]
    variant_collision_failures = _variant_collision_failures(variant_collision_evidence)
    customer_lifecycle_report = customer_lifecycle_evidence.report
    customer_lifecycle_failures = _customer_lifecycle_failures(
        customer_lifecycle_evidence
    )
    active_surfaces_report = active_surfaces_evidence.report
    active_surfaces_failures = _active_surfaces_failures(
        active_surfaces_report
    )
    retention_failures = _retention_failures(retention_report)
    canonical_replacement_failures = _sealed_lifecycle_failures(
        canonical_replacement_evidence.report,
        label="canonical replacement",
    )
    source_binding_lifecycle_failures = _sealed_lifecycle_failures(
        source_binding_lifecycle_evidence.report,
        label="source binding lifecycle",
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
                *variant_collision_failures,
                *customer_lifecycle_failures,
                *active_surfaces_failures,
                *retention_failures,
                *canonical_replacement_failures,
                *source_binding_lifecycle_failures,
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
        "company_learning_evaluation": canonical_sha256(positive_evaluation),
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
        "registry": (variant_population_evidence.registry_population_digest),
        "report": variant_population_report.digest,
        "experiment_report": (variant_population_evidence.experiment_report.digest),
        "mechanism_metrics": canonical_sha256(
            variant_mechanism_metrics.model_dump(mode="json")
        ),
    }
    variant_collision_digests = {
        "evidence": variant_collision_evidence.digest,
        "registry": variant_collision_evidence.registry_population_digest,
        "report": variant_collision_report.digest,
        "observations": canonical_sha256(
            [
                row.model_dump(mode="json")
                for row in variant_collision_evidence.observations
            ]
        ),
    }
    customer_lifecycle_digests = {
        "evidence": customer_lifecycle_evidence.digest,
        "registry": customer_lifecycle_evidence.registry_population_digest,
        "report": customer_lifecycle_report.digest,
        "observations": canonical_sha256(
            [
                row.model_dump(mode="json")
                for row in customer_lifecycle_evidence.observations
            ]
        ),
    }
    active_surfaces_digests = {
        "evidence": active_surfaces_evidence.digest,
        "report": active_surfaces_report.digest,
        "structured_identity_report": (
            active_surfaces_report.structured_identity.digest
        ),
        "source_salience_report": active_surfaces_report.source_salience.digest,
        "identity_observations": canonical_sha256(
            [
                row.model_dump(mode="json")
                for row in active_surfaces_evidence.identity_observations
            ]
        ),
        "salience_observations": canonical_sha256(
            [
                row.model_dump(mode="json")
                for row in active_surfaces_evidence.salience_observations
            ]
        ),
    }
    retention_payload = _read_json(retention_path)
    retention_digests = {
        "artifact": canonical_sha256(retention_payload),
        "spec": str(retention_report.spec_digest),
        "report": retention_report.digest,
        "observations": retention_report.observation_digest,
    }
    canonical_replacement_digests = {
        "evidence": canonical_replacement_evidence.digest,
        "report": canonical_replacement_evidence.report.digest,
        "observation": canonical_sha256(
            canonical_replacement_evidence.observation.model_dump(mode="json")
        ),
    }
    source_binding_lifecycle_digests = {
        "evidence": source_binding_lifecycle_evidence.digest,
        "report": source_binding_lifecycle_evidence.report.digest,
        "observation": canonical_sha256(
            source_binding_lifecycle_evidence.observation.model_dump(mode="json")
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
                *(f"negative: {gap}" for gap in negative_evidence.report.proof_gaps),
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
                (
                    "customer_lifecycle: current proof is customer-only and "
                    "covers rename, archive and historical name reuse; merge, "
                    "split and resurrection remain unproven. Canonical resource "
                    "replacement is measured separately."
                ),
                (
                    "canonical_replacement: the sealed proof exercises one "
                    "persisted resource replacement with representative aliases, "
                    "source bindings, attachments, Models, projections, lineage "
                    "and hard dependencies; replacement of other referent types "
                    "and broader dependency families remains unproven."
                ),
                (
                    "source_binding_lifecycle: the sealed proof exercises one "
                    "persisted Jira resource binding lifecycle; equivalent "
                    "behavior across every source system, canonical referent "
                    "type and independent writer remains unproven."
                ),
                (
                    "source_scope: persisted normalized Jira, Linear, Google "
                    "Drive and Gmail-attributed identity semantics are within "
                    "the measured scope; connector and listener delivery are "
                    "explicitly excluded from this assurance profile."
                ),
                (
                    "retention: worker restarts are object re-instantiation "
                    "inside one database process, not operating-system or "
                    "database failover."
                ),
                (
                    "retention: intervening learning directly exercises the "
                    "alias registry and does not yet reproduce every "
                    "production learning pathway."
                ),
                (
                    "retention: Model consistency is only a cardinality and ID "
                    "round trip; it does not validate proposition semantics, "
                    "lifecycle, referent, or projections. Lineage consistency "
                    "only proves observation, answered clarification, and "
                    "adjudicated alias rows exist; it does not prove complete "
                    "linkage, digest continuity, or correction propagation."
                ),
                (
                    "retention: corrected retention reuses the final exact-alias "
                    "recurrence and validates the original clarification-learned "
                    "replay authority; it does not execute a second correction "
                    "that replaces a previously learned wrong target."
                ),
                (
                    "retention: three representative collision families are "
                    "exercised; five sealed collision families remain deferred."
                ),
                (
                    "retention: tenant noninterference is inherited from the "
                    "adaptive runtime path and is not independently measured "
                    "across every retention horizon."
                ),
                *(
                    (
                        "variant_collision: supported collision safety "
                        f"observed {variant_collision_report.observed_pair_count}/"
                        f"{variant_collision_report.pair_count} sealed cases; "
                        f"{variant_collision_report.unsupported_case_count} "
                        "authenticated source-identity cases remain "
                        "unsupported until persisted SourceIdentityBinding "
                        "evidence exists.",
                    )
                    if variant_collision_report.unsupported_case_count
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
                *(f"correction: {gap}" for gap in correction_artifact.proof_gaps),
            )
        )
    )
    architecture_digest = load_architecture_registry(ARCHITECTURE_REGISTRY).digest
    implementation_plan_digest = hashlib.sha256(
        IMPLEMENTATION_PLAN.read_bytes()
    ).hexdigest()
    slack_scope_complete = bool(
        slack_report.status == "observed"
        and slack_report.metrics.case_count > 0
        and slack_report.metrics.supported_case_count == slack_report.metrics.case_count
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
            adaptive_correctness_rate=positive_metrics.get("adaptive_correctness_rate"),
            frozen_correctness_rate=positive_metrics.get("frozen_correctness_rate"),
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
            frozen_unsafe_count=(negative_evidence.report.metrics.frozen_unsafe_count),
            artifact_paths={"negative_evidence": artifact_paths["negative_evidence"]},
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
            immediate_fence_rate=(correction_artifact.metrics.immediate_fence_rate),
            direct_repair_rate=(correction_artifact.metrics.direct_repair_rate),
            recursive_repair_rate=(correction_artifact.metrics.recursive_repair_rate),
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
            convergence_ratio=(correction_artifact.metrics.convergence_ratio),
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
                        and not (variant_population_report.unsupported_case_count)
                    )
                    else "observed_with_gaps"
                )
            ),
            evidence_tier=EvidenceTier.E4,
            registry_pair_count=variant_population_report.pair_count,
            observed_pair_count=(variant_population_report.observed_pair_count),
            unsupported_case_count=(variant_population_report.unsupported_case_count),
            runtime_support_rate=(
                variant_population_report.observed_pair_count
                / max(1, variant_population_report.pair_count)
            ),
            adaptive_correctness=(variant_population_report.adaptive_correctness),
            frozen_correctness=(variant_population_report.frozen_correctness),
            adaptive_minus_frozen_correctness=(
                variant_population_report.adaptive_minus_frozen_correctness
            ),
            adaptive_unsafe_rate=(variant_population_report.adaptive_unsafe_rate),
            frozen_unsafe_rate=(variant_population_report.frozen_unsafe_rate),
            mechanism_metrics=variant_mechanism_metrics,
            artifact_paths={
                "variant_population_evidence": artifact_paths[
                    "variant_population_evidence"
                ]
            },
            component_digests=variant_population_digests,
        ),
        variant_collision=VariantCollisionAssurance(
            status=variant_collision_report.status,
            evidence_tier=EvidenceTier.E4,
            registry_pair_count=variant_collision_report.pair_count,
            observed_pair_count=(variant_collision_report.observed_pair_count),
            unsupported_case_count=(variant_collision_report.unsupported_case_count),
            runtime_support_rate=(variant_collision_report.runtime_support_rate),
            adaptive_safe_containment_rate=(
                variant_collision_report.adaptive_safe_containment_rate
            ),
            frozen_safe_containment_rate=(
                variant_collision_report.frozen_safe_containment_rate
            ),
            adaptive_unsafe_rate=(variant_collision_report.adaptive_unsafe_rate),
            frozen_unsafe_rate=variant_collision_report.frozen_unsafe_rate,
            adaptive_unsafe_resolution_rate=(
                variant_collision_report.adaptive_unsafe_resolution_rate
            ),
            frozen_unsafe_resolution_rate=(
                variant_collision_report.frozen_unsafe_resolution_rate
            ),
            adaptive_authoritative_resolution_rate=(
                variant_collision_report.adaptive_authoritative_resolution_rate
            ),
            frozen_authoritative_resolution_rate=(
                variant_collision_report.frozen_authoritative_resolution_rate
            ),
            adaptive_candidate_visibility_rate=(
                variant_collision_report.adaptive_candidate_visibility_rate
            ),
            frozen_candidate_visibility_rate=(
                variant_collision_report.frozen_candidate_visibility_rate
            ),
            adaptive_none_of_above_availability_rate=(
                variant_collision_report.adaptive_none_of_above_availability_rate
            ),
            frozen_none_of_above_availability_rate=(
                variant_collision_report.frozen_none_of_above_availability_rate
            ),
            adaptive_learned_promotion_rate=(
                variant_collision_report.adaptive_learned_promotion_rate
            ),
            frozen_learned_promotion_rate=(
                variant_collision_report.frozen_learned_promotion_rate
            ),
            adaptive_wrong_model_rate=(
                variant_collision_report.adaptive_wrong_model_rate
            ),
            frozen_wrong_model_rate=(variant_collision_report.frozen_wrong_model_rate),
            adaptive_wrong_model_count=(
                variant_collision_report.adaptive_wrong_model_count
            ),
            frozen_wrong_model_count=(
                variant_collision_report.frozen_wrong_model_count
            ),
            adaptive_source_immutability_rate=(
                variant_collision_report.adaptive_source_immutability_rate
            ),
            frozen_source_immutability_rate=(
                variant_collision_report.frozen_source_immutability_rate
            ),
            safety_incident_count=(variant_collision_report.safety_incident_count),
            source_native_observed_case_count=(
                source_native_collision_report.observed_case_count
            ),
            source_native_unsupported_case_count=(
                source_native_collision_report.unsupported_case_count
            ),
            source_native_adaptive_authoritative_resolution_rate=(
                source_native_collision_report.adaptive_authoritative_resolution_rate
            ),
            source_native_frozen_authoritative_resolution_rate=(
                source_native_collision_report.frozen_authoritative_resolution_rate
            ),
            unsupported_strata_counts=(
                variant_collision_report.unsupported_strata_counts
            ),
            unsupported_reason_counts=(
                variant_collision_report.unsupported_reason_counts
            ),
            artifact_paths={
                "variant_collision_evidence": artifact_paths[
                    "variant_collision_evidence"
                ]
            },
            component_digests=variant_collision_digests,
        ),
        customer_lifecycle=CustomerLifecycleAssurance(
            status=(
                "failed"
                if customer_lifecycle_report.status == "contradicted"
                else customer_lifecycle_report.status
            ),
            evidence_tier=EvidenceTier.E4,
            case_count=customer_lifecycle_report.case_count,
            observed_case_count=(customer_lifecycle_report.observed_case_count),
            unsupported_case_count=(customer_lifecycle_report.unsupported_case_count),
            violating_case_count=(customer_lifecycle_report.violating_case_count),
            runtime_support_rate=(customer_lifecycle_report.runtime_support_rate),
            rename_continuity_rate=(customer_lifecycle_report.rename_continuity_rate),
            valid_time_resolution_accuracy=(
                customer_lifecycle_report.valid_time_resolution_accuracy
            ),
            stale_alias_rejection_rate=(
                customer_lifecycle_report.stale_alias_rejection_rate
            ),
            current_alias_safety_rate=(
                customer_lifecycle_report.current_alias_safety_rate
            ),
            historical_name_reuse_accuracy=(
                customer_lifecycle_report.historical_name_reuse_accuracy
            ),
            observation_immutability_rate=(
                customer_lifecycle_report.observation_immutability_rate
            ),
            model_immutability_rate=(customer_lifecycle_report.model_immutability_rate),
            archive_alias_rejection_rate=(
                customer_lifecycle_report.archive_alias_rejection_rate
            ),
            archived_mutation_rejection_rate=(
                customer_lifecycle_report.archived_mutation_rejection_rate
            ),
            alias_interval_non_overlap_rate=(
                customer_lifecycle_report.alias_interval_non_overlap_rate
            ),
            tenant_isolation_rate=(customer_lifecycle_report.tenant_isolation_rate),
            replay_idempotency_rate=(customer_lifecycle_report.replay_idempotency_rate),
            unsupported_reason_counts=(
                customer_lifecycle_report.unsupported_reason_counts
            ),
            artifact_paths={
                "customer_lifecycle_evidence": artifact_paths[
                    "customer_lifecycle_evidence"
                ]
            },
            component_digests=customer_lifecycle_digests,
        ),
        active_surfaces=ActiveSurfacesAssurance(
            status=(
                "observed"
                if active_surfaces_report.status == "observed"
                else "failed"
            ),
            evidence_tier=EvidenceTier.E4,
            structured_identity=active_surfaces_report.structured_identity,
            source_salience=active_surfaces_report.source_salience,
            artifact_paths={
                "active_surfaces_evidence": artifact_paths[
                    "active_surfaces_evidence"
                ]
            },
            component_digests=active_surfaces_digests,
        ),
        retention=RetentionAssurance(
            status=(
                "failed"
                if retention_report.status == "contradicted"
                else retention_report.status
            ),
            evidence_tier=EvidenceTier.E4,
            expected_observation_count=(
                retention_report.expected_observation_count
            ),
            observed_observation_count=(
                retention_report.observed_observation_count
            ),
            exact_retention_rate=retention_report.exact_retention_rate,
            variant_retention_rate=retention_report.variant_retention_rate,
            corrected_retention_rate=retention_report.corrected_retention_rate,
            overall_positive_retention_rate=(
                retention_report.overall_positive_retention_rate
            ),
            overall_forgetting_rate=retention_report.overall_forgetting_rate,
            restart_survival_rate=retention_report.restart_survival_rate,
            correction_authority_rate=retention_report.correction_authority_rate,
            unsafe_globalization_rate=retention_report.unsafe_globalization_rate,
            negative_control_safety_rate=(
                retention_report.negative_control_safety_rate
            ),
            collision_control_safety_rate=(
                retention_report.collision_control_safety_rate
            ),
            source_immutability_rate=retention_report.source_immutability_rate,
            model_consistency_rate=retention_report.model_consistency_rate,
            evidence_lineage_consistency_rate=(
                retention_report.evidence_lineage_consistency_rate
            ),
            hard_safety_incident_rate=(
                retention_report.hard_safety_incident_rate
            ),
            retention_horizon_auc=retention_report.retention_horizon_auc,
            horizon_metrics=retention_report.horizon_metrics,
            family_counts=retention_report.family_counts,
            artifact_paths={
                "retention_evidence": artifact_paths["retention_evidence"]
            },
            component_digests=retention_digests,
        ),
        canonical_replacement=CanonicalReplacementAssurance(
            status=_sealed_lifecycle_status(
                canonical_replacement_evidence.report
            ),
            evidence_tier=EvidenceTier.E4,
            report=canonical_replacement_evidence.report,
            artifact_paths={
                "canonical_replacement_evidence": artifact_paths[
                    "canonical_replacement_evidence"
                ]
            },
            component_digests=canonical_replacement_digests,
        ),
        source_binding_lifecycle=SourceBindingLifecycleAssurance(
            status=_sealed_lifecycle_status(
                source_binding_lifecycle_evidence.report
            ),
            evidence_tier=EvidenceTier.E4,
            report=source_binding_lifecycle_evidence.report,
            artifact_paths={
                "source_binding_lifecycle_evidence": artifact_paths[
                    "source_binding_lifecycle_evidence"
                ]
            },
            component_digests=source_binding_lifecycle_digests,
        ),
        population=PopulationAssurance(
            status=(
                "observed_with_gaps"
                if population_evidence.population_report.unsupported_case_count
                else "observed"
            ),
            registry_pair_count=(population_evidence.population_report.pair_count),
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
                        population_evidence.population_report.model_dump(mode="json")
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
            **{f"positive_{key}": value for key, value in positive_digests.items()},
            **{f"negative_{key}": value for key, value in negative_digests.items()},
            **{f"slack_{key}": value for key, value in slack_digests.items()},
            **{f"population_{key}": value for key, value in population_digests.items()},
            **{
                f"variant_population_{key}": value
                for key, value in variant_population_digests.items()
            },
            **{
                f"variant_collision_{key}": value
                for key, value in variant_collision_digests.items()
            },
            **{
                f"customer_lifecycle_{key}": value
                for key, value in customer_lifecycle_digests.items()
            },
            **{
                f"active_surfaces_{key}": value
                for key, value in active_surfaces_digests.items()
            },
            **{
                f"retention_{key}": value
                for key, value in retention_digests.items()
            },
            **{
                f"canonical_replacement_{key}": value
                for key, value in canonical_replacement_digests.items()
            },
            **{
                f"source_binding_lifecycle_{key}": value
                for key, value in source_binding_lifecycle_digests.items()
            },
            **{f"correction_{key}": value for key, value in correction_digests.items()},
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


def _sealed_lifecycle_status(report: Any) -> str:
    if (
        report.violating_measurement_count
        or report.safety_violation_count
        or report.immutability_violation_count
    ):
        return "failed"
    return "observed" if report.full_scope_complete else "observed_with_gaps"


def _sealed_lifecycle_failures(
    report: Any,
    *,
    label: str,
) -> tuple[str, ...]:
    failures: list[str] = []
    if report.status != "observed":
        failures.append(f"{label}: report status was {report.status!r}")
    if report.expected_measurement_count <= 0:
        failures.append(f"{label}: sealed measurement registry was empty")
    if report.observed_measurement_count != report.expected_measurement_count:
        failures.append(
            f"{label}: observed {report.observed_measurement_count}/"
            f"{report.expected_measurement_count} sealed measurements"
        )
    if report.unsupported_measurement_count:
        failures.append(
            f"{label}: {report.unsupported_measurement_count} measurements "
            "were unsupported"
        )
    if report.violating_measurement_count:
        failures.append(
            f"{label}: {report.violating_measurement_count} measurements "
            "violated the sealed lifecycle contract"
        )
    if report.safety_violation_count:
        failures.append(
            f"{label}: {report.safety_violation_count} safety violations "
            "were observed"
        )
    if report.immutability_violation_count:
        failures.append(
            f"{label}: {report.immutability_violation_count} immutability "
            "violations were observed"
        )
    if (
        report.overall_satisfaction_rate is None
        or report.overall_satisfaction_rate.point_estimate != 1.0
    ):
        failures.append(f"{label}: overall satisfaction was not exactly 1.0")
    if not report.full_scope_complete:
        failures.append(f"{label}: full sealed scope was incomplete")
    return tuple(dict.fromkeys(failures))


def _variant_population_failures(evidence: Any) -> tuple[str, ...]:
    report = evidence.population_report
    metrics = evidence.mechanism_metrics
    if report is None or metrics is None:
        return ("variant population: full typed reports are missing",)
    failures: list[str] = []
    if report.pair_count != 24:
        failures.append("variant population: sealed registry did not contain 24 cases")
    if report.unsupported_case_count:
        failures.append(
            "variant population: "
            f"{report.unsupported_case_count} sealed cases were unsupported"
        )
    failures.extend(
        _variant_exact_metric_failures(
            expected=1.0,
            values={
                "adaptive correctness": (report.adaptive_correctness.point_estimate),
                "adaptive-minus-frozen correctness": (
                    report.adaptive_minus_frozen_correctness.point_estimate
                ),
                "candidate-memory-mediated success": (
                    metrics.candidate_memory_mediated_success_rate
                ),
                "adaptive target authorization": (
                    metrics.adaptive_target_candidate_authorization_rate
                ),
                "adaptive closed-set match": (metrics.adaptive_closed_set_match_rate),
                "source immutability": metrics.source_immutability_rate,
                "both-arm model invocation": (metrics.both_arms_one_llm_call_rate),
                "both-arm scripted target response": (
                    metrics.both_arms_scripted_target_response_rate
                ),
                "frozen safe review or abstention": (
                    metrics.frozen_safe_review_or_abstention_rate
                ),
            },
        )
    )
    failures.extend(
        _variant_exact_metric_failures(
            expected=0.0,
            values={
                "frozen correctness": (report.frozen_correctness.point_estimate),
                "adaptive unsafe rate": (report.adaptive_unsafe_rate.point_estimate),
                "frozen unsafe rate": (report.frozen_unsafe_rate.point_estimate),
                "frozen target candidate exposure": (
                    metrics.frozen_target_candidate_exposure_rate
                ),
                "frozen closed-set match rate": (metrics.frozen_closed_set_match_rate),
            },
        )
    )
    failures.extend(_variant_incident_failures(metrics))
    return tuple(failures)


def _variant_collision_failures(evidence: Any) -> tuple[str, ...]:
    report = evidence.report
    source_native = report.stratum_reports["collision_family"][
        VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER.value
    ]
    failures: list[str] = []
    if report.pair_count != 16:
        failures.append("variant collision: sealed registry did not contain 16 cases")
    if report.observed_pair_count != 16:
        failures.append("variant collision: all 16 sealed cases must execute")
    if report.unsupported_case_count:
        failures.append("variant collision: unsupported sealed cases remain")
    source_rates = {
        "adaptive source-native authoritative resolution": (
            source_native.adaptive_authoritative_resolution_rate
        ),
        "frozen source-native authoritative resolution": (
            source_native.frozen_authoritative_resolution_rate
        ),
    }
    if (
        source_native.observed_case_count != 2
        or source_native.unsupported_case_count != 0
    ):
        failures.append(
            "variant collision: source-native scope did not execute both sealed cases"
        )
    failures.extend(
        f"variant collision: {label} lacked a two-case 1.0 result"
        for label, rate in source_rates.items()
        if (rate is None or rate.sample_size != 2 or rate.point_estimate != 1.0)
    )
    one_metrics = {
        "adaptive safe containment": (
            report.adaptive_safe_containment_rate.point_estimate
        ),
        "frozen safe containment": (report.frozen_safe_containment_rate.point_estimate),
        "adaptive candidate visibility": (
            report.adaptive_candidate_visibility_rate.point_estimate
        ),
        "frozen candidate visibility": (
            report.frozen_candidate_visibility_rate.point_estimate
        ),
        "adaptive none-of-the-above availability": (
            report.adaptive_none_of_above_availability_rate.point_estimate
        ),
        "frozen none-of-the-above availability": (
            report.frozen_none_of_above_availability_rate.point_estimate
        ),
        "adaptive source immutability": (
            report.adaptive_source_immutability_rate.point_estimate
        ),
        "frozen source immutability": (
            report.frozen_source_immutability_rate.point_estimate
        ),
    }
    zero_metrics = {
        "adaptive unsafe rate": (report.adaptive_unsafe_rate.point_estimate),
        "frozen unsafe rate": report.frozen_unsafe_rate.point_estimate,
        "adaptive unsafe resolution": (
            report.adaptive_unsafe_resolution_rate.point_estimate
        ),
        "frozen unsafe resolution": (
            report.frozen_unsafe_resolution_rate.point_estimate
        ),
        "adaptive learned promotion": (
            report.adaptive_learned_promotion_rate.point_estimate
        ),
        "frozen learned promotion": (
            report.frozen_learned_promotion_rate.point_estimate
        ),
        "adaptive wrong Model rate": (report.adaptive_wrong_model_rate.point_estimate),
        "frozen wrong Model rate": (report.frozen_wrong_model_rate.point_estimate),
    }
    failures.extend(
        f"variant collision: {label} was {value!r}, expected 1.0"
        for label, value in one_metrics.items()
        if value != 1.0
    )
    failures.extend(
        f"variant collision: {label} was {value!r}, expected 0.0"
        for label, value in zero_metrics.items()
        if value != 0.0
    )
    counts = {
        "safety incidents": report.safety_incident_count,
        "adaptive wrong Models": report.adaptive_wrong_model_count,
        "frozen wrong Models": report.frozen_wrong_model_count,
    }
    failures.extend(
        f"variant collision: recorded {count} {label}"
        for label, count in counts.items()
        if count
    )
    return tuple(failures)


def _customer_lifecycle_failures(evidence: Any) -> tuple[str, ...]:
    report = evidence.report
    failures: list[str] = []
    if report.case_count != 8:
        failures.append("customer lifecycle: sealed registry did not contain 8 cases")
    if report.observed_case_count != 8 or report.unsupported_case_count:
        failures.append("customer lifecycle: all 8 sealed cases must execute")
    if report.violating_case_count:
        failures.append(
            "customer lifecycle: "
            f"{report.violating_case_count} cases violated lifecycle safety"
        )
    metrics = {
        "runtime support": report.runtime_support_rate,
        "rename continuity": report.rename_continuity_rate,
        "valid-time resolution": report.valid_time_resolution_accuracy,
        "stale alias rejection": report.stale_alias_rejection_rate,
        "current alias safety": report.current_alias_safety_rate,
        "historical name reuse": report.historical_name_reuse_accuracy,
        "Observation immutability": report.observation_immutability_rate,
        "Model immutability": report.model_immutability_rate,
        "archived alias rejection": report.archive_alias_rejection_rate,
        "archived mutation rejection": (report.archived_mutation_rejection_rate),
        "alias interval non-overlap": (report.alias_interval_non_overlap_rate),
        "tenant isolation": report.tenant_isolation_rate,
        "replay idempotency": report.replay_idempotency_rate,
    }
    failures.extend(
        f"customer lifecycle: {label} was {metric.point_estimate!r}, expected 1.0"
        for label, metric in metrics.items()
        if metric.point_estimate != 1.0
    )
    return tuple(failures)


def _active_surfaces_failures(report: Any) -> tuple[str, ...]:
    failures: list[str] = []
    for name, surface in (
        ("structured identity", report.structured_identity),
        ("source salience", report.source_salience),
    ):
        if surface.status != "observed":
            failures.append(f"active surfaces: {name} status={surface.status}")
        if surface.unsupported_case_count:
            failures.append(
                f"active surfaces: {name} has "
                f"{surface.unsupported_case_count} unsupported cases"
            )
        if surface.violating_case_count:
            failures.append(
                f"active surfaces: {name} has "
                f"{surface.violating_case_count} violating cases"
            )
    identity = report.structured_identity
    salience = report.source_salience
    metrics = {
        "identity runtime support": identity.runtime_support_rate.point_estimate,
        "claim emission": identity.claim_emission_rate.point_estimate,
        "claim preservation": identity.claim_preservation_rate.point_estimate,
        "governed attachment": identity.governed_attachment_rate.point_estimate,
        "handler non-authority": identity.handler_non_authority_rate.point_estimate,
        "ingest non-authority": identity.ingest_non_authority_rate.point_estimate,
        "forged text rejection": identity.forged_text_rejection_rate.point_estimate,
        "missing binding non-authority": (
            identity.missing_binding_non_authority_rate.point_estimate
        ),
        "cross-source isolation": identity.cross_source_isolation_rate.point_estimate,
        "cross-tenant isolation": identity.cross_tenant_isolation_rate.point_estimate,
        "source immutability": identity.source_immutability_rate.point_estimate,
        "salience runtime support": salience.runtime_support_rate.point_estimate,
        "useful salience increase": (
            salience.useful_salience_increase_rate.point_estimate
        ),
        "corrected nonincrease": salience.corrected_nonincrease_rate.point_estimate,
        "pending zero credit": salience.pending_zero_credit_rate.point_estimate,
        "foreign tenant isolation": (
            salience.foreign_tenant_isolation_rate.point_estimate
        ),
        "canonical truth immutability": (
            salience.canonical_truth_immutability_rate.point_estimate
        ),
        "grounding truth immutability": (
            salience.grounding_truth_immutability_rate.point_estimate
        ),
        "salience direction": salience.salience_direction_rate.point_estimate,
    }
    failures.extend(
        f"active surfaces: {name} regressed to {value:.6f}"
        for name, value in metrics.items()
        if value != 1.0
    )
    return tuple(failures)


def _retention_failures(report: Any) -> tuple[str, ...]:
    expected = {
        "exact retention": (report.exact_retention_rate, 1.0),
        "variant retention": (report.variant_retention_rate, 1.0),
        "corrected retention": (report.corrected_retention_rate, 1.0),
        "overall positive retention": (
            report.overall_positive_retention_rate,
            1.0,
        ),
        "overall forgetting": (report.overall_forgetting_rate, 0.0),
        "restart survival": (report.restart_survival_rate, 1.0),
        "correction authority": (report.correction_authority_rate, 1.0),
        "unsafe globalization": (report.unsafe_globalization_rate, 0.0),
        "negative control safety": (
            report.negative_control_safety_rate,
            1.0,
        ),
        "collision control safety": (
            report.collision_control_safety_rate,
            1.0,
        ),
        "source immutability": (report.source_immutability_rate, 1.0),
        "model consistency": (report.model_consistency_rate, 1.0),
        "evidence lineage consistency": (
            report.evidence_lineage_consistency_rate,
            1.0,
        ),
        "hard safety incident rate": (
            report.hard_safety_incident_rate,
            0.0,
        ),
        "retention horizon AUC": (report.retention_horizon_auc, 1.0),
    }
    failures: list[str] = []
    if report.status != "observed":
        failures.append(f"retention: status={report.status}")
    if report.expected_observation_count != report.observed_observation_count:
        failures.append(
            "retention: raw observation coverage "
            f"{report.observed_observation_count}/"
            f"{report.expected_observation_count}"
        )
    failures.extend(
        f"retention: {name}={value:.6f}, expected {target:.1f}"
        for name, (value, target) in expected.items()
        if value != target
    )
    for horizon in report.horizon_metrics:
        prefix = (
            f"retention: horizon cycles={horizon.cycle_count} "
            f"restarts={horizon.restart_count}"
        )
        for name, value, target in (
            ("positive retention", horizon.positive_retention_rate, 1.0),
            ("forgetting", horizon.forgetting_rate, 0.0),
            ("negative safety", horizon.negative_safety_rate, 1.0),
            ("collision safety", horizon.collision_safety_rate, 1.0),
            ("source immutability", horizon.source_immutability_rate, 1.0),
            ("model consistency", horizon.model_consistency_rate, 1.0),
            (
                "evidence lineage consistency",
                horizon.evidence_lineage_consistency_rate,
                1.0,
            ),
        ):
            if value is not None and value != target:
                failures.append(
                    f"{prefix} {name}={value:.6f}, expected {target:.1f}"
                )
    return tuple(failures)


def _variant_exact_metric_failures(
    *,
    expected: float,
    values: dict[str, float | None],
) -> tuple[str, ...]:
    return tuple(
        f"variant population: {label} was {value!r}, expected {expected}"
        for label, value in values.items()
        if value != expected
    )


def _variant_incident_failures(metrics: Any) -> tuple[str, ...]:
    counts = {
        "hard safety incidents": metrics.hard_safety_incident_count,
        "control-integrity violations": (metrics.control_integrity_violation_count),
    }
    return tuple(
        f"variant population: mechanism evidence recorded {count} {label}"
        for label, count in counts.items()
        if count
    )


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
        "collision={collision_observed}/{collision_registry} "
        "lifecycle={lifecycle_observed}/{lifecycle_registry} "
        "active_identity={identity_observed}/{identity_registry} "
        "active_salience={salience_observed}/{salience_registry} "
        "retention={retention_observed}/{retention_expected} "
        "replacement={replacement_observed}/{replacement_expected} "
        "source_binding={binding_observed}/{binding_expected} "
        "forgetting={forgetting} "
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
            collision_observed=(summary.variant_collision.observed_pair_count),
            collision_registry=(summary.variant_collision.registry_pair_count),
            lifecycle_observed=(summary.customer_lifecycle.observed_case_count),
            lifecycle_registry=summary.customer_lifecycle.case_count,
            identity_observed=(
                summary.active_surfaces.structured_identity.observed_case_count
            ),
            identity_registry=summary.active_surfaces.structured_identity.case_count,
            salience_observed=(
                summary.active_surfaces.source_salience.observed_case_count
            ),
            salience_registry=summary.active_surfaces.source_salience.case_count,
            retention_observed=summary.retention.observed_observation_count,
            retention_expected=summary.retention.expected_observation_count,
            replacement_observed=(
                summary.canonical_replacement.report.observed_measurement_count
            ),
            replacement_expected=(
                summary.canonical_replacement.report.expected_measurement_count
            ),
            binding_observed=(
                summary.source_binding_lifecycle.report.observed_measurement_count
            ),
            binding_expected=(
                summary.source_binding_lifecycle.report.expected_measurement_count
            ),
            forgetting=summary.retention.overall_forgetting_rate,
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
            "the sealed exact, variant and collision populations, Slack "
            "reconstruction, customer identity lifecycle, active structured "
            "identity and salience surfaces, retention across learning and "
            "restart horizons, canonical resource replacement, source binding "
            "lifecycle, and a recursive correction convergence burn in one "
            "assurance command."
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
