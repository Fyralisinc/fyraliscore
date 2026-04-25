"""Ablation framework for the LSOB harness.

Phase 2.1 (see ``LSOB-BUILD-PLAN.md`` session 2.1) introduces a formal
registry of named ablation configs plus a generic application+validation
helper.

The registry is populated at import time with the eight named ablations
from the build plan:

    none, no-bridge, no-calibration, no-second-pass, no-activation,
    no-entity-resolver, no-pattern-precipitation, no-model-composition,
    all-off.

Validation works by asking the SUT to answer a small set of smoke
queries after the ablation has been applied. If the SUT reports that
a disabled feature is "still active" via a diff-metadata or belief
rationale tag, :class:`AblationValidationError` is raised.

This module is intentionally light on SUT assumptions — the SUT side
must implement ``apply_ablation``; the validation probe re-uses the
regular ``query_beliefs_at`` / ``query_at_risk_at`` / ``produce_diff_for_trigger``
surface so no extra protocol methods are required.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from lsob_contracts import (
    AblationConfig,
    AtRiskReport,
    Belief,
    BeliefQuery,
    DiffOp,
    EntityRef,
    Trigger,
)


class AblationError(Exception):
    """Base class for ablation-framework failures."""


class AblationValidationError(AblationError):
    """Raised when post-apply validation detects the disabled feature still active."""


# ---------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------


def _canonical(name: str) -> str:
    """Canonicalise an ablation name.

    Accepts dashes, underscores, and mixed case — users may type
    either ``no-bridge`` (plan style) or ``no_bridge`` (Python-y style).
    """
    return name.strip().lower().replace("_", "-")


@dataclass
class AblationRegistry:
    """Keyed registry of :class:`AblationConfig` objects.

    Names are canonicalised to the plan's dash-separated form on both
    registration and lookup, so ``no_bridge`` and ``no-bridge`` both
    resolve to the same entry.
    """

    _entries: dict[str, AblationConfig] = field(default_factory=dict)

    def register(self, name: str, config: AblationConfig) -> None:
        key = _canonical(name)
        # Ensure the stored config's ``name`` matches the canonical key so
        # downstream persistence (run_id, manifests) stays consistent.
        stored = config.model_copy(update={"name": key})
        self._entries[key] = stored

    def get(self, name: str) -> AblationConfig:
        key = _canonical(name)
        if key not in self._entries:
            raise KeyError(
                f"unknown ablation {name!r} "
                f"(known: {sorted(self._entries.keys())})"
            )
        return self._entries[key]

    def list_names(self) -> list[str]:
        return sorted(self._entries.keys())

    def __contains__(self, name: str) -> bool:
        return _canonical(name) in self._entries


REGISTRY = AblationRegistry()


def _seed_registry(reg: AblationRegistry) -> None:
    """Register the canonical named ablations from the build plan."""
    reg.register("none", AblationConfig(name="none"))
    reg.register(
        "no-bridge",
        AblationConfig(name="no-bridge", disable_bridge=True),
    )
    reg.register(
        "no-calibration",
        AblationConfig(name="no-calibration", disable_calibration=True),
    )
    reg.register(
        "no-second-pass",
        AblationConfig(name="no-second-pass", disable_second_pass=True),
    )
    reg.register(
        "no-activation",
        AblationConfig(name="no-activation", disable_activation=True),
    )
    reg.register(
        "no-entity-resolver",
        AblationConfig(
            name="no-entity-resolver", disable_entity_resolver=True
        ),
    )
    reg.register(
        "no-pattern-precipitation",
        AblationConfig(
            name="no-pattern-precipitation",
            disable_pattern_precipitation=True,
        ),
    )
    reg.register(
        "no-model-composition",
        AblationConfig(
            name="no-model-composition",
            disable_model_composition=True,
        ),
    )
    reg.register(
        "all-off",
        AblationConfig(
            name="all-off",
            disable_bridge=True,
            disable_calibration=True,
            disable_second_pass=True,
            disable_activation=True,
            disable_entity_resolver=True,
            disable_pattern_precipitation=True,
            disable_model_composition=True,
        ),
    )


_seed_registry(REGISTRY)


# ---------------------------------------------------------------------
# Apply + validate
# ---------------------------------------------------------------------


# Map from the ``disable_*`` flag name to human-readable tokens that
# might appear in belief / diff metadata for the disabled feature. If
# any of these tokens show up in a post-apply probe, we conclude the
# SUT has not actually disabled the feature.
_ACTIVE_MARKERS: dict[str, tuple[str, ...]] = {
    "disable_bridge": ("bridge", "at_risk"),
    "disable_calibration": ("calibration", "calibrator"),
    "disable_second_pass": ("second_pass", "second-pass"),
    "disable_activation": ("activation",),
    "disable_entity_resolver": ("entity_resolver", "entity-resolver"),
    "disable_pattern_precipitation": ("pattern_precipitation", "precipitation"),
    "disable_model_composition": ("model_composition", "composer"),
}


def _marker_hit(blob: Any, tokens: tuple[str, ...]) -> bool:
    """Recursively check whether any token appears (case-insensitive) in ``blob``."""
    if blob is None:
        return False
    if isinstance(blob, str):
        low = blob.lower()
        return any(tok in low for tok in tokens)
    if isinstance(blob, dict):
        return any(_marker_hit(v, tokens) for v in blob.values())
    if isinstance(blob, (list, tuple, set)):
        return any(_marker_hit(v, tokens) for v in blob)
    return False


def _collect_evidence_strings(belief: Belief) -> list[str]:
    return [
        belief.proposition or "",
        belief.proposition_kind or "",
        *(belief.entities or []),
    ]


async def _smoke_probes(sut: Any) -> dict[str, Any]:
    """Run a tiny fixed set of probes against the SUT and collect responses.

    The probes are deliberately generic and side-effect free: they should
    not require any particular corpus to have been ingested. Every baseline
    implementation returns well-formed (possibly empty) results for these.
    """
    now = datetime.now(tz=timezone.utc)
    belief_query = BeliefQuery(
        query_id="ablation-probe-belief",
        entity_ref=EntityRef(kind="commitment", id="ablation-probe"),
        timestamp=now,
        proposition_kind="status",
        k=1,
    )
    trigger = Trigger(
        trigger_id="ablation-probe-trigger",
        kind="ablation_probe",
        payload={"entity_ref": "ablation-probe"},
        timestamp=now,
    )
    beliefs: list[Belief] = await sut.query_beliefs_at(belief_query)
    at_risk: AtRiskReport = await sut.query_at_risk_at(now)
    diff: DiffOp = await sut.produce_diff_for_trigger(trigger)
    return {"beliefs": beliefs, "at_risk": at_risk, "diff": diff}


def _validate_probes(ablation: AblationConfig, probes: dict[str, Any]) -> None:
    """Raise :class:`AblationValidationError` if any disabled feature looks active."""
    for field_name, tokens in _ACTIVE_MARKERS.items():
        if not getattr(ablation, field_name, False):
            continue
        # Scan belief propositions / entities
        for belief in probes.get("beliefs", []) or []:
            for s in _collect_evidence_strings(belief):
                if _marker_hit(s, tokens):
                    raise AblationValidationError(
                        f"ablation {ablation.name!r}: flag {field_name} set "
                        f"but belief evidence still mentions {tokens!r}"
                    )
        # Scan at-risk rationales + risk_kind
        at_risk: AtRiskReport | None = probes.get("at_risk")
        if at_risk is not None:
            for item in at_risk.items:
                if _marker_hit(item.rationale, tokens) or _marker_hit(
                    item.risk_kind, tokens
                ):
                    # ``disable_bridge`` specifically requires at_risk to
                    # return nothing — an at-risk item is a smoking gun.
                    if field_name == "disable_bridge":
                        raise AblationValidationError(
                            f"ablation {ablation.name!r}: disable_bridge set "
                            f"but at_risk still produced {item.entity_ref}"
                        )
                    raise AblationValidationError(
                        f"ablation {ablation.name!r}: flag {field_name} set "
                        f"but at_risk rationale mentions {tokens!r}"
                    )
        # Special-case disable_bridge: any item at all means the Bridge ran.
        if field_name == "disable_bridge" and at_risk is not None and at_risk.items:
            raise AblationValidationError(
                f"ablation {ablation.name!r}: disable_bridge set "
                f"but AtRiskReport has {len(at_risk.items)} items"
            )
        # Scan diff metadata + rationale
        diff: DiffOp | None = probes.get("diff")
        if diff is not None:
            if _marker_hit(diff.metadata, tokens) or _marker_hit(
                diff.rationale, tokens
            ):
                raise AblationValidationError(
                    f"ablation {ablation.name!r}: flag {field_name} set "
                    f"but diff metadata/rationale mentions {tokens!r}"
                )


async def apply_ablation(sut: Any, ablation: AblationConfig) -> None:
    """Apply ``ablation`` to ``sut`` and verify disabled features stay off.

    The SUT is expected to:

    1. Honour ``apply_ablation`` by turning off the feature flags
       corresponding to the set ``disable_*`` fields.
    2. Return probe responses (belief, at-risk, diff) that do not
       advertise any of the disabled features in their
       rationale/metadata/evidence fields.

    Raises :class:`AblationValidationError` on validation failure.
    """
    await sut.apply_ablation(ablation)
    if not ablation.any_disabled():
        # Nothing to validate for the ``none`` ablation — skip the probes
        # to avoid perturbing state unnecessarily.
        return
    probes = await _smoke_probes(sut)
    _validate_probes(ablation, probes)


__all__ = [
    "AblationError",
    "AblationRegistry",
    "AblationValidationError",
    "REGISTRY",
    "apply_ablation",
]
