"""Independent evidence and oracle contracts for P2 hard gates HG-04..HG-10."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Literal, Mapping, Sequence


P2_GATE_IDS = ("HG-04", "HG-05", "HG-06", "HG-07", "HG-08", "HG-09", "HG-10")
ObservationStatus = Literal["observed", "missing", "unrun"]


@dataclass(frozen=True, slots=True)
class P2CaseObservation:
    case_id: str
    status: ObservationStatus
    observed_disposition: Literal["accept", "reject", "remain_noncanonical"] | None = None
    invariant_checks: tuple[tuple[str, bool], ...] = ()
    before_digest: str | None = None
    after_digest: str | None = None
    command_receipt_id: str | None = None
    violation_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class P2RaceObservation:
    scenario_id: str
    status: ObservationStatus
    observed_outcome: str | None = None
    before_digest: str | None = None
    after_digest: str | None = None
    lifecycle_event_count: int | None = None
    repair_obligation_count: int | None = None
    violation_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class P2GateResult:
    gate_id: str
    status: Literal["pass", "fail", "missing", "unrun"]
    eligible_count: int
    observed_count: int
    conforming_count: int
    violation_count: int
    coverage: float
    conformance: float | None
    violation_codes: tuple[str, ...]


def stable_digest(value: object) -> str:
    """Return a deterministic SHA-256 digest for JSON-compatible evidence."""

    if hasattr(value, "__dataclass_fields__"):
        value = asdict(value)  # type: ignore[arg-type]
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(payload.encode()).hexdigest()


def evaluate_gate(
    gate_id: str,
    *,
    eligible_case_ids: Sequence[str],
    observations: Mapping[str, P2CaseObservation],
    expected_dispositions: Mapping[str, str] | None = None,
) -> P2GateResult:
    """Score one gate without calling production truth validators.

    An observed case conforms only when the harness explicitly records the
    named invariant as true and records no violation. Missing evidence can
    never become a pass through absence of findings.
    """

    if gate_id not in P2_GATE_IDS:
        raise ValueError(f"unknown P2 gate: {gate_id}")
    eligible = tuple(dict.fromkeys(eligible_case_ids))
    present = [observations[item] for item in eligible if item in observations]
    observed = [item for item in present if item.status == "observed"]
    unrun = [item for item in present if item.status == "unrun"]
    expected_dispositions = expected_dispositions or {}
    conforming = [
        item
        for item in observed
        if dict(item.invariant_checks).get(gate_id) is True
        and not item.violation_codes
        and (
            item.case_id not in expected_dispositions
            or item.observed_disposition == expected_dispositions[item.case_id]
        )
    ]
    violations = tuple(sorted({code for item in observed for code in item.violation_codes}))
    coverage = len(observed) / len(eligible) if eligible else 1.0
    conformance = len(conforming) / len(observed) if observed else None
    if len(observed) < len(eligible):
        status: Literal["pass", "fail", "missing", "unrun"] = "unrun" if unrun and not observed else "missing"
    elif len(conforming) != len(observed):
        status = "fail"
    else:
        status = "pass"
    return P2GateResult(gate_id, status, len(eligible), len(observed), len(conforming), len(observed) - len(conforming), coverage, conformance, violations)


def race_conforms(observation: P2RaceObservation, expected_outcome: str) -> bool:
    """Compare a transactional observation with the sealed external oracle."""

    return (
        observation.status == "observed"
        and observation.observed_outcome == expected_outcome
        and not observation.violation_codes
        and observation.before_digest is not None
        and observation.after_digest is not None
    )


__all__ = ["P2CaseObservation", "P2GateResult", "P2RaceObservation", "P2_GATE_IDS", "evaluate_gate", "race_conforms", "stable_digest"]
