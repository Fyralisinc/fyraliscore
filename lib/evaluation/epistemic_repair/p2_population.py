"""Sealed, implementation-independent population for the P2 truth-kernel run.

This module describes evaluator inputs and expected outcomes.  It deliberately
does not import domain commands, repositories, validators, or database models;
the eventual database harness must translate these records at its boundary.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Literal


Disposition = Literal["accept", "reject", "remain_noncanonical"]


@dataclass(frozen=True, slots=True)
class P2Case:
    case_id: str
    family: str
    operation: str
    expected_disposition: Disposition
    facts: tuple[tuple[str, str], ...] = ()
    expected_invariants: tuple[str, ...] = ()

    def fact(self, name: str) -> str | None:
        return dict(self.facts).get(name)


@dataclass(frozen=True, slots=True)
class P2RaceScenario:
    scenario_id: str
    operation: str
    fault_point: str | None
    expected_outcome: str
    expected_invariants: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class P2Population:
    version: str
    cases: tuple[P2Case, ...]
    races: tuple[P2RaceScenario, ...]

    @property
    def digest(self) -> str:
        encoded = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return sha256(encoded.encode()).hexdigest()

    def family(self, name: str) -> tuple[P2Case, ...]:
        return tuple(case for case in self.cases if case.family == name)


def _case(
    family: str,
    ordinal: int,
    operation: str,
    expected: Disposition,
    *,
    facts: dict[str, str] | None = None,
    invariants: tuple[str, ...] = (),
) -> P2Case:
    return P2Case(
        case_id=f"p2-{family}-{ordinal:02d}",
        family=family,
        operation=operation,
        expected_disposition=expected,
        facts=tuple(sorted((facts or {}).items())),
        expected_invariants=invariants,
    )


def build_p2_population() -> P2Population:
    """Return the complete minimum P2 population and transactional scenarios."""

    cases: list[P2Case] = []

    # Ten distinct nonaccepted attempts: candidate, review, and rejected states.
    states = ("candidate",) * 4 + ("needs_review",) * 3 + ("rejected",) * 3
    for i, state in enumerate(states, 1):
        cases.append(_case("nonaccepted_admission", i, "admit_model", "remain_noncanonical", facts={"admission_state": state}, invariants=("HG-04",)))

    for i in range(1, 11):
        cases.append(_case("accepted_atomic", i, "admit_atomic_model", "accept", facts={"evidence_kind": "observation", "scope_role": "subject", "semantic_version": str(i)}, invariants=("HG-05", "HG-06", "HG-07")))

    for i in range(1, 6):
        cases.append(_case("accepted_synthesis", i, "admit_synthesis_model", "accept", facts={"evidence_kind": "accepted_model_version", "transitive_provenance": "complete", "scope_role": "subject"}, invariants=("HG-05", "HG-06")))

    wrappers = ("batch_envelope", "prompt_instruction", "control_language", "processing_wrapper", "generic_summary")
    for i, kind in enumerate(wrappers, 1):
        cases.append(_case("wrapper_control", i, "admit_model", "remain_noncanonical", facts={"wrapper_kind": kind}, invariants=("HG-04",)))

    conflicting_types = (("person", "team"), ("company", "person"), ("project", "customer"), ("team", "product"), ("customer", "issue"))
    for i, (existing, proposed) in enumerate(conflicting_types, 1):
        cases.append(_case("entity_type_conflict", i, "bind_scope_entity", "reject", facts={"entity_id": f"shared-{i}", "existing_type": existing, "proposed_type": proposed}, invariants=("HG-06",)))

    for i in range(1, 6):
        cases.append(_case("representation_divergence", i, "admit_model_version", "reject", facts={"proposition_digest": f"prop-{i}", "rendering_digest": f"render-{i}"}, invariants=("HG-07",)))

    for i in range(1, 6):
        cases.append(_case("falsification", i, "falsify_model", "accept", facts={"support_count": str(i), "incident_projection_count": "5" if i == 1 else str(i)}, invariants=("HG-08",)))

    for i in range(1, 6):
        cases.append(_case("valid_supersession", i, "supersede_model", "accept", facts={"same_lineage": "true", "target_newer": "true", "target_active": "true"}, invariants=("HG-08",)))
    invalid_reasons = ("different_lineage", "target_older", "target_inactive", "source_already_terminal", "self_supersession")
    for i, reason in enumerate(invalid_reasons, 1):
        cases.append(_case("invalid_supersession", i, "supersede_model", "reject", facts={"invalid_reason": reason}, invariants=("HG-08",)))

    relation_shapes = (
        ("causal_influence", "valid_direction"),
        ("dependency_constraint", "valid_direction"),
        ("enablement", "valid_direction"),
        ("predictive_indicator", "valid_direction"),
        ("causal_influence", "reverse_direction"),
        ("dependency_constraint", "reverse_direction"),
        ("enablement", "reverse_direction"),
        ("predictive_indicator", "reverse_direction"),
        ("causal_influence", "wrong_role"),
        ("dependency_constraint", "wrong_role"),
        ("enablement", "wrong_endpoint"),
        ("predictive_indicator", "wrong_endpoint"),
        ("causal_influence", "self_negating_rationale"),
        ("dependency_constraint", "self_negating_rationale"),
        ("unknown_relation_kind", "unknown_type"),
        ("analogous", "structural_only"),
        ("co_occurs", "structural_only"),
        ("same_issue", "structural_only"),
        ("causal_influence", "reciprocal_invalidity"),
        ("enablement", "reciprocal_invalidity"),
    )
    valid_shapes = {"valid_direction"}
    for i, (kind, shape) in enumerate(relation_shapes, 1):
        cases.append(_case("business_relation", i, "admit_relation", "accept" if shape in valid_shapes else "remain_noncanonical", facts={"relation_kind": kind, "shape": shape, "evidence_id": f"rel-evidence-{i}"}, invariants=("HG-09",)))

    components = ("topology", "sage", "projection", "embedding", "evaluator")
    for i, component in enumerate(components, 1):
        cases.append(_case("derived_direct_write", i, "write_canonical_truth", "reject", facts={"component": component}, invariants=("HG-10",)))

    # Explicit idempotence/side-effect probes required by the P2 hard criteria.
    cases.extend(
        (
            _case("retrieval_stability", 1, "retrieve_model_100_times", "accept", facts={"repeat_count": "100"}, invariants=("HG-07", "HG-10")),
            _case("evidence_idempotence", 1, "replay_duplicate_relation_evidence", "accept", facts={"repeat_count": "5"}, invariants=("HG-09",)),
            _case("projection_idempotence", 1, "rebuild_relation_projection", "accept", facts={"repeat_count": "5"}, invariants=("HG-09", "HG-10")),
            _case("command_idempotence", 1, "replay_each_mutating_command", "accept", facts={"expected_extra_effects": "0"}, invariants=("HG-08",)),
        )
    )

    races = (
        P2RaceScenario("p2-race-rollback-after-third-fence", "falsify_model_with_five_projections", "after_projection_fence_3", "wholly_old_state", ("HG-08",)),
        P2RaceScenario("p2-race-retry-falsification", "retry_falsify_model_with_five_projections", None, "wholly_fenced_new_state_one_event", ("HG-08",)),
        P2RaceScenario("p2-race-confirm-vs-falsify", "concurrent_confirm_and_falsify_same_expected_version", None, "exactly_one_cas_winner_no_resurrection", ("HG-08",)),
        P2RaceScenario("p2-race-relation-evidence-falsified", "falsify_nonparticipant_relation_evidence", None, "excluded_from_consequential_reads_one_repair_obligation", ("HG-05", "HG-08", "HG-09")),
        P2RaceScenario("p2-race-participant-superseded", "supersede_relation_participant", None, "no_automatic_endpoint_rebinding", ("HG-08", "HG-09")),
    )
    return P2Population("p2-truth-kernel-population-v1", tuple(cases), races)


__all__ = ["P2Case", "P2Population", "P2RaceScenario", "build_p2_population"]
