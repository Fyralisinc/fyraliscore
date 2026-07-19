"""Frozen evaluator-owned dossier cases for the bounded TI3 experiment.

Provider payloads and evaluator annotations are deliberately separate.  This
module is evaluation data: production dossier construction must never import it.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, Mapping

from lib.contracts.kernel import canonical_sha256

FIXTURE_SCHEMA_VERSION = "ti3-frozen-dossier-cases-v1"
MANIFEST_SCHEMA_VERSION = "ti3-frozen-dossier-manifest-v1"


@dataclass(frozen=True, slots=True)
class FrozenGoldAnnotation:
    expected_decision: Literal["synthesis", "abstain"]
    required_scope_facets: tuple[str, ...]
    required_mechanism_facets: tuple[str, ...]
    required_direction: str | None
    allowed_cause_handles: tuple[str, ...]
    allowed_condition_handles: tuple[str, ...]
    allowed_effect_handles: tuple[str, ...]
    required_support_handles: tuple[str, ...]
    required_counterevidence_handles: tuple[str, ...]
    acceptable_abstention_reasons: tuple[str, ...]
    forbidden_claims: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FrozenDossierCase:
    case_id: str
    provider_payload: Mapping[str, Any]
    gold: FrozenGoldAnnotation

    @property
    def dossier_digest(self) -> str:
        return canonical_sha256(self.provider_payload)

    @property
    def gold_digest(self) -> str:
        return canonical_sha256(asdict(self.gold))

    @property
    def case_digest(self) -> str:
        return canonical_sha256(
            {
                "case_id": self.case_id,
                "dossier_digest": self.dossier_digest,
                "gold_digest": self.gold_digest,
            }
        )


def _object(
    handle: str,
    kind: str,
    text: str,
    *,
    evidence_role: str,
    occurred_at: str | None = None,
    authority: str | None = None,
    independence: str | None = None,
) -> dict[str, Any]:
    semantic: dict[str, Any] = {"text": text}
    if occurred_at:
        semantic["occurred_at"] = occurred_at
    return {
        "handle": handle,
        "object_kind": kind,
        "semantic_content": semantic,
        "evidence_role": evidence_role,
        "authority_tier": authority,
        "independence_group": independence,
    }


def _payload(
    *,
    dossier_id: str,
    display_label: str,
    handles: list[dict[str, Any]],
    event_order: list[str],
    model_heads: list[str],
    direct_observations: list[str],
    supporting: list[str],
    contradictory: list[str],
    auxiliary: list[str],
    causes: list[str],
    conditions: list[str],
    outcomes: list[str],
    uncertainties: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "synthesis-dossier-v1",
        "dossier_id": dossier_id,
        "scope": {"display_label": display_label},
        "window": {
            "start_at": "2026-07-01T09:00:00+00:00",
            "end_at": "2026-07-04T16:00:00+00:00",
            "as_of_at": "2026-07-04T18:00:00+00:00",
            "ordering": "occurred_at_observation_id",
        },
        "handles": handles,
        "event_order": event_order,
        "accepted_model_heads": model_heads,
        "direct_observations": direct_observations,
        "supporting_evidence": [
            {
                "object_handle": handle,
                "role": "direct" if handle.startswith("O") else "transitive",
                "reason": "",
            }
            for handle in supporting
        ],
        "contradictory_evidence": [
            {"object_handle": handle, "role": "contradictory", "reason": ""}
            for handle in contradictory
        ],
        "auxiliary_evidence": [
            {"object_handle": handle, "role": "auxiliary", "reason": ""}
            for handle in auxiliary
        ],
        "open_uncertainty": list(uncertainties or []),
        "candidate_mechanism_slots": {
            "causes": causes,
            "conditions": conditions,
            "outcomes": outcomes,
        },
        "considered_explanations": [],
        "discriminating_missing_evidence": list(uncertainties or []),
    }


def build_frozen_dossier_cases() -> tuple[FrozenDossierCase, ...]:
    """Return the three immutable development cases in deterministic order."""
    atlas = FrozenDossierCase(
        case_id="atlas_positive_v1",
        provider_payload=_payload(
            dossier_id="DOS_7c1e5a28f9044bd1",
            display_label="Atlas release",
            handles=[
                _object(
                    "M1",
                    "accepted_model_head",
                    "The release certificate had no clearly recorded owner before the rollout window moved.",
                    evidence_role="transitive",
                ),
                _object(
                    "M2",
                    "accepted_model_head",
                    "A later handoff record placed the ownership transition immediately before another rollout delay.",
                    evidence_role="transitive",
                ),
                _object(
                    "O1",
                    "observation",
                    "The infrastructure owner supplied a timestamp matching the earlier ownership handoff.",
                    occurred_at="2026-07-04T14:00:00+00:00",
                    evidence_role="direct",
                    authority="authoritative",
                    independence="source:infrastructure-owner-record",
                ),
                _object(
                    "O2",
                    "observation",
                    "The release dashboard still says ready, although it does not record certificate ownership.",
                    occurred_at="2026-07-04T15:00:00+00:00",
                    evidence_role="contradictory",
                    authority="unvetted",
                    independence="source:release-dashboard",
                ),
                _object(
                    "O3",
                    "observation",
                    "Atlas release is delayed after the certificate ownership handoff remained incomplete.",
                    occurred_at="2026-07-04T16:00:00+00:00",
                    evidence_role="direct",
                    authority="authoritative",
                    independence="source:release-control-record",
                ),
            ],
            event_order=["O1", "O2", "O3"],
            model_heads=["M1", "M2"],
            direct_observations=["O1", "O3"],
            supporting=["M1", "M2", "O1", "O3"],
            contradictory=["O2"],
            auxiliary=[],
            causes=["M1", "M2"],
            conditions=["O1"],
            outcomes=["O3"],
        ),
        gold=FrozenGoldAnnotation(
            expected_decision="synthesis",
            required_scope_facets=("Atlas", "release"),
            required_mechanism_facets=("certificate", "ownership", "handoff", "delay"),
            required_direction="incomplete ownership handoff -> release delay",
            allowed_cause_handles=("M1", "M2"),
            allowed_condition_handles=("O1",),
            allowed_effect_handles=("O3",),
            required_support_handles=("M1", "M2", "O1", "O3"),
            required_counterevidence_handles=("O2",),
            acceptable_abstention_reasons=(),
            forbidden_claims=(
                "dashboard readiness caused the delay",
                "ownership is confirmed complete",
            ),
        ),
    )
    cobalt = FrozenDossierCase(
        case_id="cobalt_positive_v1",
        provider_payload=_payload(
            dossier_id="DOS_b6932fd847ac501e",
            display_label="Cobalt renewal",
            handles=[
                _object(
                    "M1",
                    "accepted_model_head",
                    "The renewal signature remained pending while customer procurement approval was absent.",
                    evidence_role="transitive",
                ),
                _object(
                    "M2",
                    "accepted_model_head",
                    "An optimistic CRM health field preceded no signed renewal in the customer record.",
                    evidence_role="transitive",
                ),
                _object(
                    "O1",
                    "observation",
                    "Customer procurement says the approval email has not been issued.",
                    occurred_at="2026-07-04T13:00:00+00:00",
                    evidence_role="direct",
                    authority="authoritative_external",
                    independence="source:customer-procurement-email",
                ),
                _object(
                    "O2",
                    "observation",
                    "The CRM health field remains green and predicts an on-time close.",
                    occurred_at="2026-07-04T14:30:00+00:00",
                    evidence_role="contradictory",
                    authority="unvetted",
                    independence="source:crm-health-field",
                ),
                _object(
                    "O3",
                    "observation",
                    "The renewal signature is blocked pending customer procurement approval.",
                    occurred_at="2026-07-04T16:00:00+00:00",
                    evidence_role="direct",
                    authority="authoritative_external",
                    independence="source:customer-contract-record",
                ),
            ],
            event_order=["O1", "O2", "O3"],
            model_heads=["M1", "M2"],
            direct_observations=["O1", "O3"],
            supporting=["M1", "M2", "O1", "O3"],
            contradictory=["O2"],
            auxiliary=[],
            causes=["O1"],
            conditions=["M1"],
            outcomes=["O3"],
        ),
        gold=FrozenGoldAnnotation(
            expected_decision="synthesis",
            required_scope_facets=("Cobalt", "renewal"),
            required_mechanism_facets=(
                "customer",
                "procurement",
                "approval",
                "signature",
            ),
            required_direction="missing customer procurement approval -> blocked renewal signature",
            allowed_cause_handles=("O1",),
            allowed_condition_handles=("M1",),
            allowed_effect_handles=("O3",),
            required_support_handles=("M1", "M2", "O1", "O3"),
            required_counterevidence_handles=("O2",),
            acceptable_abstention_reasons=(),
            forbidden_claims=(
                "CRM health caused approval",
                "customer approval was issued",
            ),
        ),
    )
    null = FrozenDossierCase(
        case_id="null_adversarial_v1",
        provider_payload=_payload(
            dossier_id="DOS_41d8e7c2a9560fb3",
            display_label="Harbor launch",
            handles=[
                _object(
                    "M1",
                    "accepted_model_head",
                    "Two earlier launch notes mentioned rainy weather near schedule changes.",
                    evidence_role="transitive",
                ),
                _object(
                    "O1",
                    "observation",
                    "Rain is forecast on the morning of the launch window.",
                    occurred_at="2026-07-04T13:00:00+00:00",
                    evidence_role="auxiliary",
                    authority="authoritative_external",
                    independence="source:weather-feed",
                ),
                _object(
                    "O2",
                    "observation",
                    "The launch window moved, but the scheduling note gives no reason.",
                    occurred_at="2026-07-04T14:00:00+00:00",
                    evidence_role="direct",
                    authority="unvetted",
                    independence="source:scheduling-note",
                ),
                _object(
                    "O3",
                    "observation",
                    "Operations reports no weather restriction and says the venue remains available.",
                    occurred_at="2026-07-04T15:00:00+00:00",
                    evidence_role="contradictory",
                    authority="authoritative",
                    independence="source:operations-record",
                ),
                {
                    "handle": "U1",
                    "object_kind": "uncertainty",
                    "semantic_content": {
                        "question": "What authorized record explains why the launch window moved?",
                        "status": "missing",
                        "discriminates_between": [],
                        "retrieval_target": "launch change record",
                    },
                    "evidence_role": None,
                    "authority_tier": None,
                    "independence_group": None,
                },
            ],
            event_order=["O1", "O2", "O3"],
            model_heads=["M1"],
            direct_observations=["O2"],
            supporting=["M1", "O2"],
            contradictory=["O3"],
            auxiliary=["O1"],
            causes=[],
            conditions=[],
            outcomes=["O2"],
            uncertainties=["U1"],
        ),
        gold=FrozenGoldAnnotation(
            expected_decision="abstain",
            required_scope_facets=("Harbor", "launch"),
            required_mechanism_facets=(),
            required_direction=None,
            allowed_cause_handles=(),
            allowed_condition_handles=(),
            allowed_effect_handles=(),
            required_support_handles=(),
            required_counterevidence_handles=("O3",),
            acceptable_abstention_reasons=(
                "insufficient_evidence",
                "conflicting_evidence",
                "no_coherent_mechanism",
            ),
            forbidden_claims=(
                "rain caused the launch move",
                "weather blocked the venue",
            ),
        ),
    )
    return atlas, cobalt, null


def build_fixture_manifest() -> dict[str, Any]:
    cases = build_frozen_dossier_cases()
    body = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "fixture_schema_version": FIXTURE_SCHEMA_VERSION,
        "case_count": len(cases),
        "cases": [
            {
                "case_id": case.case_id,
                "dossier_digest": case.dossier_digest,
                "gold_digest": case.gold_digest,
                "case_digest": case.case_digest,
            }
            for case in cases
        ],
    }
    return {**body, "manifest_digest": canonical_sha256(body)}


def write_frozen_dossier_artifacts(output_dir: Path) -> dict[str, Any]:
    """Write separate provider and gold artifacts plus their sealed manifest."""
    output_dir.mkdir(parents=True, exist_ok=True)
    cases = build_frozen_dossier_cases()
    for case in cases:
        (output_dir / f"{case.case_id}.dossier.json").write_text(
            json.dumps(case.provider_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (output_dir / f"{case.case_id}.gold.json").write_text(
            json.dumps(asdict(case.gold), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    manifest = build_fixture_manifest()
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


__all__ = [
    "FIXTURE_SCHEMA_VERSION",
    "MANIFEST_SCHEMA_VERSION",
    "FrozenDossierCase",
    "FrozenGoldAnnotation",
    "build_fixture_manifest",
    "build_frozen_dossier_cases",
    "write_frozen_dossier_artifacts",
]
