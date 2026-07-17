"""Compose sealed extraction and company-physics evidence without trust inflation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict

from lib.evaluation.entity_extraction_gold import (
    EntityExtractionMetrics,
    GoldEntityExtractionReport,
)
from lib.evaluation.entity_pipeline_gold import GoldEntityPipelineReport
from lib.evaluation.entity_readiness import (
    AdversarialCompanyPhysicsEvidence,
    EntityReadinessEvidence,
    EntityReadinessThresholds,
    ExactRatePopulation,
    evaluate_entity_readiness,
)

V3_SCHEMA = "learned-entity-discovery-quality-v3"
VERTICAL_SCHEMA = "sealed-company-physics-objective-v1"
ADVERSARIAL_SCHEMA = "sealed-company-physics-adversarial-objective-v2"
READINESS_INPUT_SCHEMA = "sealed-company-physics-readiness-evidence-v1"
OUTPUT_SCHEMA = "objective-entity-evidence-v2"
_REQUIRED_POPULATIONS = frozenset({
    "pipeline.candidate_recall_at_3",
    "pipeline.canonical_link_coverage",
    "pipeline.canonical_link_accuracy",
    "pipeline.no_admission_no_model_safety_rate",
    "pipeline.harmful_semantic_propagation_rate",
    "pipeline.relation_lineage_integrity",
})
_REQUIRED_INCIDENTS = frozenset({
    "cross_tenant_identity_incidents",
    "untraceable_canonical_assignments",
    "known_wrong_type_consequential_admissions",
})


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class VerticalReadinessInput(_Strict):
    schema_version: str
    exact_rate_populations: Mapping[str, ExactRatePopulation]
    incidents: Mapping[str, int]


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_bound_json(path: Path, *, expected_sha256: str) -> dict[str, Any]:
    raw = path.read_bytes()
    actual = sha256_bytes(raw)
    if actual != expected_sha256:
        raise ValueError(f"artifact SHA mismatch for {path}: {actual} != {expected_sha256}")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"artifact root must be an object: {path}")
    return payload


def compose_objective_entity_evidence(
    *,
    v3: Mapping[str, Any],
    vertical: Mapping[str, Any],
    v3_artifact_sha256: str,
    vertical_artifact_sha256: str,
    adversarial: Mapping[str, Any],
    adversarial_artifact_sha256: str,
    thresholds: EntityReadinessThresholds | None = None,
) -> dict[str, Any]:
    """Validate, normalize and compose two already SHA-bound artifact objects."""

    if v3.get("benchmark") != V3_SCHEMA:
        raise ValueError("unsupported sealed v3 benchmark schema")
    if v3.get("evidence_class") != "sealed_untouched_holdout_one_shot_completed":
        raise ValueError("v3 is not sealed completed one-shot evidence")
    if vertical.get("schema_version") != VERTICAL_SCHEMA:
        raise ValueError("unsupported company-physics objective schema")
    _verify_vertical_objective_digest(vertical)
    if adversarial.get("schema_version") != ADVERSARIAL_SCHEMA:
        raise ValueError("unsupported adversarial company-physics schema")
    _verify_vertical_objective_digest(adversarial)
    if adversarial.get("base_objective_sha256") != vertical.get("objective_sha256"):
        raise ValueError("adversarial evidence is not bound to the supplied base vertical")
    adversarial_evidence = _adversarial_readiness_evidence(adversarial)

    post = _object(v3.get("post_verification"), "v3.post_verification")
    extraction_payload = _object(post.get("metrics"), "v3 post metrics")
    extraction = GoldEntityExtractionReport.model_validate({
        key: extraction_payload[key]
        for key in GoldEntityExtractionReport.model_fields
        if key in extraction_payload
    })
    per_type_payload = _object(
        extraction_payload.get("by_entity_type"), "v3 by_entity_type"
    )
    per_type = {
        str(name): EntityExtractionMetrics.model_validate(value)
        for name, value in per_type_payload.items()
    }
    if not per_type:
        raise ValueError("v3 lacks per-type extraction populations")

    pipeline = GoldEntityPipelineReport.model_validate(
        _object(vertical.get("entity_pipeline_v4"), "vertical entity_pipeline_v4")
    )
    readiness_input = VerticalReadinessInput.model_validate(
        _object(vertical.get("readiness_evidence_v1"), "vertical readiness evidence")
    )
    if readiness_input.schema_version != READINESS_INPUT_SCHEMA:
        raise ValueError("unsupported vertical readiness evidence schema")
    _require_exact_keys(
        readiness_input.exact_rate_populations,
        _REQUIRED_POPULATIONS,
        "exact readiness populations",
    )
    _require_exact_keys(readiness_input.incidents, _REQUIRED_INCIDENTS, "incidents")
    if any(not isinstance(value, int) or value < 0
           for value in readiness_input.incidents.values()):
        raise ValueError("readiness incidents must be nonnegative integers")
    _verify_population_rates(pipeline, readiness_input.exact_rate_populations)

    negative = _object(post.get("negative_cleanliness"), "v3 negative cleanliness")
    negative_population = ExactRatePopulation(
        numerator=_integer(negative.get("clean_negative_signals"), "clean negatives"),
        denominator=_integer(negative.get("negative_signal_count"), "negative signals"),
    )
    if negative_population.rate != negative.get("rate"):
        raise ValueError("v3 negative-cleanliness rate disagrees with exact population")

    evidence = EntityReadinessEvidence(
        per_type_extraction=per_type,
        negative_cleanliness=negative_population,
        exact_rate_populations=readiness_input.exact_rate_populations,
        cross_tenant_identity_incidents=readiness_input.incidents[
            "cross_tenant_identity_incidents"
        ],
        untraceable_canonical_assignments=readiness_input.incidents[
            "untraceable_canonical_assignments"
        ],
        known_wrong_type_consequential_admissions=readiness_input.incidents[
            "known_wrong_type_consequential_admissions"
        ],
        adversarial_company_physics=adversarial_evidence,
    )
    readiness = evaluate_entity_readiness(
        extraction=extraction, pipeline=pipeline, evidence=evidence,
        thresholds=thresholds,
    )
    proof_gaps = sorted(set(
        list(extraction.uncertainties)
        + list(pipeline.uncertainties)
        + [f"readiness_coverage_gap:{item}" for item in readiness.coverage_gaps]
        + [f"readiness_blocker_unknown:{item.code}"
           for item in readiness.blockers if item.status == "unknown"]
        + [
            "adversarial_company_physics_is_bounded:"
            f"critical={adversarial_evidence.critical_safe_rejections.denominator},"
            f"high={adversarial_evidence.high_safe_rejections.denominator},"
            f"open_world={adversarial_evidence.open_world_safe_decisions.denominator};"
            "does_not_establish_company_scale_generalization"
        ]
    ))
    output: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA,
        "artifact_bindings": {
            "sealed_v3": {
                "artifact_sha256": v3_artifact_sha256,
                "corpus_sha256": v3.get("frozen_corpus_sha256"),
                "schema": V3_SCHEMA,
            },
            "company_physics_vertical": {
                "artifact_sha256": vertical_artifact_sha256,
                "objective_sha256": vertical.get("objective_sha256"),
                "schema": VERTICAL_SCHEMA,
            },
            "company_physics_adversarial": {
                "artifact_sha256": adversarial_artifact_sha256,
                "objective_sha256": adversarial.get("objective_sha256"),
                "base_objective_sha256": adversarial.get("base_objective_sha256"),
                "schema": ADVERSARIAL_SCHEMA,
            },
        },
        "extraction": extraction.model_dump(mode="json"),
        "pipeline": pipeline.model_dump(mode="json"),
        "per_type_extraction": {
            name: value.model_dump(mode="json") for name, value in per_type.items()
        },
        "negative_cleanliness": negative_population.model_dump(mode="json"),
        "readiness": readiness.model_dump(mode="json"),
        "adversarial_company_physics": dict(adversarial),
        "proof_gaps": proof_gaps,
    }
    output["composition_sha256"] = sha256_bytes(canonical_json_bytes(output))
    return output


def write_atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _verify_vertical_objective_digest(vertical: Mapping[str, Any]) -> None:
    expected = vertical.get("objective_sha256")
    if not isinstance(expected, str) or len(expected) != 64:
        raise ValueError("vertical objective_sha256 is missing")
    body = dict(vertical)
    body.pop("objective_sha256", None)
    actual = sha256_bytes(canonical_json_bytes(body))
    if actual != expected:
        raise ValueError("vertical objective_sha256 mismatch")


def _adversarial_readiness_evidence(
    adversarial: Mapping[str, Any],
) -> AdversarialCompanyPhysicsEvidence:
    tiers = _object(adversarial.get("consequence_tier_denominators"), "adversarial tiers")
    critical = _exact_tier(tiers.get("critical"), "critical")
    high = _exact_tier(tiers.get("high"), "high")
    attempts = tuple(adversarial.get("adversarial_attempts") or ())
    if len(attempts) != critical.denominator + high.denominator:
        raise ValueError("adversarial attempt count disagrees with tier denominators")
    population = _object(adversarial.get("population"), "adversarial population")
    if _integer(
        population.get("adversarial_relation_attempts"), "adversarial population attempts"
    ) != len(attempts):
        raise ValueError("adversarial population count disagrees with attempt rows")
    unsafe_writes = sum(
        not bool(_object(row, "adversarial attempt").get("rejected_without_write"))
        for row in attempts
    )
    multi_hop = _object(adversarial.get("multi_hop"), "adversarial multi_hop")
    correction = _object(
        adversarial.get("correction_propagation"), "adversarial correction"
    )
    open_world = _object(
        adversarial.get("open_world_abstention"), "adversarial open_world"
    )
    novel_count = _integer(open_world.get("novel_and_homonym_cases"), "open-world cases")
    safe_rate = open_world.get("safe_decision_rate")
    if not isinstance(safe_rate, (int, float)) or not 0 <= safe_rate <= 1:
        raise ValueError("open-world safe decision rate must be in [0,1]")
    safe_count = round(float(safe_rate) * novel_count)
    if abs((safe_count / novel_count if novel_count else 0.0) - float(safe_rate)) > 1e-12:
        raise ValueError("open-world rate lacks an exact integral population")
    correction_failures = sum(not bool(correction.get(name)) for name in (
        "first_hop_retired", "downstream_reevaluation_enqueued",
        "second_hop_preserved_pending_reevaluation",
    )) + int(bool(correction.get("transitive_repair_claimed")))
    return AdversarialCompanyPhysicsEvidence(
        critical_safe_rejections=critical,
        high_safe_rejections=high,
        cycle_closure_safe=bool(multi_hop.get("cycle_closure_rejected")),
        multi_hop_observed=_integer(
            multi_hop.get("observed_active_hops_before_correction"), "observed hops"
        ),
        multi_hop_expected=_integer(multi_hop.get("expected_hops"), "expected hops"),
        first_hop_retired=bool(correction.get("first_hop_retired")),
        downstream_reevaluation_enqueued=bool(
            correction.get("downstream_reevaluation_enqueued")
        ),
        second_hop_preserved_pending_reevaluation=bool(
            correction.get("second_hop_preserved_pending_reevaluation")
        ),
        transitive_repair_claimed=bool(correction.get("transitive_repair_claimed")),
        open_world_safe_decisions=ExactRatePopulation(
            numerator=safe_count, denominator=novel_count
        ),
        unsafe_relation_writes=unsafe_writes,
        correction_propagation_failures=correction_failures,
    )


def _exact_tier(value: Any, label: str) -> ExactRatePopulation:
    tier = _object(value, f"{label} tier")
    population = ExactRatePopulation(
        numerator=_integer(tier.get("safe_rejections"), f"{label} safe rejections"),
        denominator=_integer(tier.get("attempts"), f"{label} attempts"),
    )
    if population.rate != tier.get("safe_rejection_rate"):
        raise ValueError(f"{label} tier rate disagrees with exact population")
    return population


def _verify_population_rates(
    pipeline: GoldEntityPipelineReport,
    populations: Mapping[str, ExactRatePopulation],
) -> None:
    metrics = pipeline.overall
    values = {
        "pipeline.candidate_recall_at_3": metrics.candidate_recall_at_k.get(3),
        "pipeline.canonical_link_coverage": metrics.canonical_link_coverage,
        "pipeline.canonical_link_accuracy": metrics.canonical_link_accuracy,
        "pipeline.no_admission_no_model_safety_rate": (
            metrics.no_admission_no_model_safety_rate
        ),
        "pipeline.harmful_semantic_propagation_rate": (
            metrics.harmful_semantic_propagation_rate
        ),
        "pipeline.relation_lineage_integrity": metrics.relation_lineage_integrity,
    }
    for name, value in values.items():
        population_rate = populations[name].rate
        if value is None and population_rate is None:
            continue
        if value is None or population_rate is None or abs(value - population_rate) > 1e-12:
            raise ValueError(f"{name} disagrees with its exact population")


def _require_exact_keys(values: Mapping[str, Any], required: frozenset[str], label: str) -> None:
    if set(values) != required:
        raise ValueError(
            f"{label} must contain exactly {sorted(required)}; got {sorted(values)}"
        )


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _integer(value: Any, label: str) -> int:
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


__all__ = [
    "compose_objective_entity_evidence", "load_bound_json", "write_atomic_json",
]
