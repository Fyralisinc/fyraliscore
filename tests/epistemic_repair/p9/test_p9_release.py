from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import pytest

from lib.evaluation.epistemic_repair.p9_release import (
    CONTENT_DIGEST_ALGORITHM, ManifestEvidence, READY_VERDICT,
    REVIEW_SCHEMA_VERSION, REQUIRED_PHASES, build_release_report, reproduce, seal_manifest,
)
from lib.evaluation.epistemic_repair.p9_contracts import PHASE_EVIDENCE_CONTRACTS
from lib.evaluation.epistemic_repair.p6_p9 import GATE_IDS as P6_GATES, METRIC_SPECS as P6_METRICS
from lib.evaluation.epistemic_repair.p7_p9 import P7_P9_GATES, P7_METRIC_SPECS
from lib.evaluation.epistemic_repair.p8_p9 import GATE_IDS as P8_GATES, METRIC_CONTRACTS as P8_METRICS


def _digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _phase(path: Path, phase: str, commit: str, *, metric=True):
    contract = PHASE_EVIDENCE_CONTRACTS[phase]
    metrics = contract["metrics"] if metric else ()
    body = {
        "schema_version": contract["schema_version"], "commit": commit,
        "phase_exit_ready": True, "hard_gates": {name: True for name in contract["gates"]},
        "p9_continuous_metrics": [{
            "name": name, "numerator": 1, "denominator": 1, "value": 1.0,
            "coverage": 1.0, "uncertainty": "not_applicable", "status": "pass",
            "operator": ">=", "threshold": 1.0, "source_artifact_digest": "a" * 64,
            "worst_cases": [],
        } for name in metrics],
        "p9_member_contributions": {
            "schema_version": contract["contribution_schema"],
            "preregistered_contract_digest": "b" * 64,
            "gate_members": {name: [{
                "member_id": f"{name}:member", "raw_source_digest": "c" * 64,
                "conforms": True,
            }] for name in contract["gates"]},
            "metric_members": {name: [{
                "member_id": f"{name}:member", "raw_source_digest": "d" * 64,
                "numerator": 1, "denominator": 1,
            }] for name in metrics},
            "member_source_digests": ["c" * 64, *(["d" * 64] if metrics else [])],
        },
        "strategic_decision": "primary_memory_earned" if phase == "p7" else None,
    }
    body["content_digest"] = _digest(body)
    path.write_text(json.dumps(body, sort_keys=True))
    return ManifestEvidence(
        str(path), body["schema_version"], commit, sha256(path.read_bytes()).hexdigest(),
        body["content_digest"], "content_digest", CONTENT_DIGEST_ALGORITHM,
        "integrated_current", contract["gates"], metrics,
    )


def _setup(tmp_path: Path, *, p0_metric=False):
    commit = "a" * 40
    entries = {
        phase: _phase(tmp_path / f"{phase}.json", phase, commit, metric=(phase != "p0" or p0_metric))
        for phase in REQUIRED_PHASES
    }
    manifest = seal_manifest(
        coordinator_id="coordinator", release_commit=commit, required_current=entries,
    )
    reproduction = reproduce(manifest)
    receipt_body = {
        "schema_version": REVIEW_SCHEMA_VERSION, "reviewer_id": "independent-reviewer",
        "status": "reproduced", "reviewed_manifest_digest": manifest["manifest_digest"],
        "reproduced_report_digest": reproduction["reproduced_report_digest"],
    }
    receipt = {**receipt_body, "receipt_digest": _digest(receipt_body)}
    return commit, entries, manifest, receipt


def test_strict_manifest_authorizes_exact_normalized_evidence(tmp_path: Path):
    commit, _, manifest, receipt = _setup(tmp_path)
    report = build_release_report(
        manifest=manifest, verified_release_commit=commit,
        verified_worktree_clean=True, reviewer_receipt=receipt,
    )
    assert report["verdict"] == READY_VERDICT
    assert report["completion_authorized"] is True


def test_canonical_registry_matches_strict_late_phase_normalizers() -> None:
    assert PHASE_EVIDENCE_CONTRACTS["p6"]["gates"] == P6_GATES
    assert PHASE_EVIDENCE_CONTRACTS["p6"]["metrics"] == tuple(P6_METRICS)
    assert PHASE_EVIDENCE_CONTRACTS["p7"]["gates"] == P7_P9_GATES
    assert PHASE_EVIDENCE_CONTRACTS["p7"]["metrics"] == tuple(P7_METRIC_SPECS)
    assert PHASE_EVIDENCE_CONTRACTS["p8"]["gates"] == P8_GATES
    assert PHASE_EVIDENCE_CONTRACTS["p8"]["metrics"] == tuple(P8_METRICS)


def test_metric_free_phase_requires_explicit_empty_required_set(tmp_path: Path):
    _, entries, manifest, _ = _setup(tmp_path)
    assert reproduce(manifest)["phase_evidence"][0]["metrics_valid"] is True
    bad = dict(entries)
    bad["p0"] = ManifestEvidence(**{
        **bad["p0"].__dict__, "required_metric_ids": ("invented",),
    })
    with pytest.raises(ValueError, match="weakens"):
        seal_manifest(coordinator_id="coordinator", release_commit="a" * 40, required_current=bad)


def test_adversarial_metric_and_gate_shapes_fail_closed(tmp_path: Path):
    commit, entries, _, _ = _setup(tmp_path)
    path = Path(entries["p3"].path)
    artifact = json.loads(path.read_text())
    artifact["p9_continuous_metrics"][0]["denominator"] = 0
    artifact["p9_continuous_metrics"][0]["status"] = "pass"
    artifact["hard_gates"]["undeclared-extra"] = True
    artifact["content_digest"] = _digest({k: v for k, v in artifact.items() if k != "content_digest"})
    path.write_text(json.dumps(artifact, sort_keys=True))
    attacked = dict(entries)
    attacked["p3"] = ManifestEvidence(**{
        **attacked["p3"].__dict__, "sha256": sha256(path.read_bytes()).hexdigest(),
        "content_digest": artifact["content_digest"],
    })
    manifest = seal_manifest(
        coordinator_id="coordinator", release_commit=commit, required_current=attacked,
    )
    # Even after outer SHA and embedded digest resealing, exact gate IDs and
    # the positive-denominator contract reject the artifact.
    assert reproduce(manifest)["required_evidence_green"] is False


@pytest.mark.parametrize("attack", [
    "missing_gate_members", "duplicate_member_ids", "undeclared_source_digest",
    "member_arithmetic",
])
def test_member_contribution_contract_cannot_be_resealed_away(
    tmp_path: Path, attack: str,
) -> None:
    commit, entries, _, _ = _setup(tmp_path)
    path = Path(entries["p5"].path)
    artifact = json.loads(path.read_text())
    contributions = artifact["p9_member_contributions"]
    if attack == "missing_gate_members":
        contributions["gate_members"].pop(next(iter(contributions["gate_members"])))
    elif attack == "duplicate_member_ids":
        members = next(iter(contributions["metric_members"].values()))
        members.append(dict(members[0]))
    elif attack == "undeclared_source_digest":
        next(iter(contributions["metric_members"].values()))[0]["raw_source_digest"] = "e" * 64
    else:
        next(iter(contributions["metric_members"].values()))[0]["numerator"] = 999
    artifact["content_digest"] = _digest({k: v for k, v in artifact.items() if k != "content_digest"})
    path.write_text(json.dumps(artifact, sort_keys=True))
    changed = dict(entries)
    changed["p5"] = ManifestEvidence(**{
        **changed["p5"].__dict__, "sha256": sha256(path.read_bytes()).hexdigest(),
        "content_digest": artifact["content_digest"],
    })
    manifest = seal_manifest(
        coordinator_id="coordinator", release_commit=commit, required_current=changed,
    )
    reproduction = reproduce(manifest)
    assert reproduction["evidence_contract_complete"] is False
    assert reproduction["phase_evidence"][5]["contributions_valid"] is False


def test_insufficient_calibration_is_valid_evidence_but_never_authorizes(tmp_path: Path):
    commit, entries, _, _ = _setup(tmp_path)
    path = Path(entries["p6"].path)
    artifact = json.loads(path.read_text())
    artifact["phase_exit_ready"] = False
    for metric in artifact["p9_continuous_metrics"]:
        if metric["name"] in {"resolved_outcome_model_ece", "resolved_outcome_model_brier"}:
            metric.update(
                numerator=None, denominator=19, value=None,
                uncertainty={"status": "insufficient_population", "eligible_population": 19,
                             "minimum_required": 20},
                status="insufficient_population", operator="<=",
                threshold=.15 if metric["name"].endswith("ece") else .20,
            )
            artifact["p9_member_contributions"]["metric_members"][metric["name"]][0].update(
                numerator=None, denominator=19,
            )
    artifact["content_digest"] = _digest({k: v for k, v in artifact.items() if k != "content_digest"})
    path.write_text(json.dumps(artifact, sort_keys=True))
    changed = dict(entries)
    changed["p6"] = ManifestEvidence(**{
        **changed["p6"].__dict__, "sha256": sha256(path.read_bytes()).hexdigest(),
        "content_digest": artifact["content_digest"],
    })
    manifest = seal_manifest(
        coordinator_id="coordinator", release_commit=commit, required_current=changed,
    )
    reproduction = reproduce(manifest)
    assert reproduction["evidence_contract_complete"] is True
    assert reproduction["phase_green"]["p6"] is False
    receipt_body = {
        "schema_version": REVIEW_SCHEMA_VERSION, "reviewer_id": "independent-reviewer",
        "status": "reproduced", "reviewed_manifest_digest": manifest["manifest_digest"],
        "reproduced_report_digest": reproduction["reproduced_report_digest"],
    }
    report = build_release_report(
        manifest=manifest, verified_release_commit=commit, verified_worktree_clean=True,
        reviewer_receipt={**receipt_body, "receipt_digest": _digest(receipt_body)},
    )
    assert report["completion_authorized"] is False


def test_embedded_digest_and_reviewer_identity_are_mandatory(tmp_path: Path):
    commit, entries, manifest, receipt = _setup(tmp_path)
    path = Path(entries["p2"].path)
    artifact = json.loads(path.read_text())
    artifact["content_digest"] = "0" * 64
    path.write_text(json.dumps(artifact, sort_keys=True))
    assert reproduce(manifest)["required_evidence_green"] is False
    receipt["reviewer_id"] = "coordinator"
    report = build_release_report(
        manifest=manifest, verified_release_commit=commit,
        verified_worktree_clean=True, reviewer_receipt=receipt,
    )
    assert report["reviewer_receipt_valid"] is False
    assert report["completion_authorized"] is False


def test_diagnostics_never_substitute_for_required_phase(tmp_path: Path):
    _, entries, _, _ = _setup(tmp_path)
    missing = dict(entries)
    diagnostic = missing.pop("p8")
    try:
        seal_manifest(
            coordinator_id="coordinator", release_commit="a" * 40,
            required_current=missing,
            diagnostics=(ManifestEvidence(**{**diagnostic.__dict__, "evidence_class": "historical_falsifying"}),),
        )
    except ValueError as exc:
        assert "exactly p0 through p8" in str(exc)
    else:
        raise AssertionError("diagnostic substituted for required p8")
