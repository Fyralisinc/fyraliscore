"""Sealed deterministic populations and schedules for P8 characterization."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from itertools import product

from lib.contracts.kernel import canonical_sha256


P8_VERSION = "epistemic-repair-p8-fault-scale-v1"


@dataclass(frozen=True, slots=True)
class FaultCase:
    case_id: str
    boundary: str
    terminal_fate: str = "converged"
    max_physical_attempts: int = 2


@dataclass(frozen=True, slots=True)
class FaultSchedule:
    version: str
    cases: tuple[FaultCase, ...]
    digest: str


@dataclass(frozen=True, slots=True)
class ScaleCell:
    cell_id: str
    batch_size: int
    memory_horizon_batches: int
    tenant_concurrency: int


@dataclass(frozen=True, slots=True)
class CharacterizationManifest:
    population: str
    exact_size: int
    unit: str
    required_composition: tuple[tuple[str, int], ...]
    sealed_digest: str


_FAULTS = (
    ("provider_timeout_before_response", "provider_call:before_response"),
    ("provider_timeout_after_partial_work", "provider_call:partial_response"),
    ("invalid_structured_output", "provider_parse:structured_output"),
    ("validation_rejection", "truth_admission:validation"),
    ("database_serialization_failure", "truth_apply:serialization"),
    ("crash_after_validation_before_apply", "truth_apply:before_commit"),
    ("crash_after_apply_before_queue_ack", "truth_apply:after_commit_before_ack"),
    ("crash_during_dependent_lifecycle_fencing", "lifecycle:fence_dependents"),
    ("crash_during_projection_refresh", "projection:post_commit_refresh"),
    ("restart_with_pending_truth_critical_work", "restart:pending_truth_work"),
    ("duplicate_delivery_replay", "delivery:duplicate"),
    ("authority_revocation_selection_to_commit", "authority:commit_fence"),
)


def build_fault_schedule() -> FaultSchedule:
    cases = tuple(FaultCase(f"P8-F-{i:02d}", name) for i, (name, _) in enumerate(_FAULTS, 1))
    payload = {"version": P8_VERSION, "cases": [asdict(case) for case in cases]}
    return FaultSchedule(P8_VERSION, cases, canonical_sha256(payload))


def fault_injection_points() -> dict[str, str]:
    return {name: point for name, point in _FAULTS}


def build_scale_matrix() -> tuple[ScaleCell, ...]:
    return tuple(
        ScaleCell(f"p8-bs{batch}-h{horizon}-t{tenants}", batch, horizon, tenants)
        for batch, horizon, tenants in product((10, 25, 50), (12, 50, 100), (1, 5, 20))
    )


def _manifest(name: str, size: int, unit: str, composition: dict[str, int]) -> CharacterizationManifest:
    body = {"population": name, "exact_size": size, "unit": unit, "required_composition": sorted(composition.items())}
    return CharacterizationManifest(name, size, unit, tuple(sorted(composition.items())), canonical_sha256(body))


def build_characterization_manifests() -> tuple[CharacterizationManifest, ...]:
    # Categories intentionally overlap. Exact denominators and every required
    # minimum are sealed; runtime inputs never contain these labels.
    return (
        _manifest("boundary_discovery", 1200, "normalized_observation", {
            "episodes": 240, "structured": 60, "conversational": 120, "cross_source": 60,
            "reply_thread_edit": 200, "discourse_reference": 120, "topic_drift": 100,
            "split_merge": 80, "temporal_distractor": 80, "quote_link": 60,
            "incomplete_topology": 60, "cross_source_object_link": 100,
        }),
        _manifest("context_selection", 600, "frozen_decision", {
            "topology_sufficient": 200, "temporal_combined_expansion": 150,
            "semantically_unstable_multi_context": 100, "needs_expansion": 75,
            "needs_clarification": 50, "budget_exhausted": 25,
        }),
        _manifest("entity_grounding", 2400, "mention_opportunity", {
            "explicit": 1200, "discourse_deictic": 600, "open_world_none_known": 300,
            "negative": 300, "ambiguous_alias": 300, "near_name_collision": 200,
            "cross_customer_trap": 120, "novel_referent": 100, "merge_split_correction": 80,
        }),
        _manifest("retrieval", 600, "claim_local_decision", {
            "supporting_equivalent": 150, "contradiction_lifecycle": 150,
            "multi_hop_relation": 120, "sparse_no_match_raw_reopen": 90,
            "noise_noop": 90, "cold": 200, "intermediate": 200, "mature": 200,
        }),
        _manifest("feedback", 360, "base_decision_paired_policies", {
            "later_confirmed": 120, "revised": 80, "falsified": 60,
            "justified_noop": 40, "entity_human_correction": 30,
            "no_observable_outcome_control": 30, "route_policy_executions": 720,
        }),
    )
