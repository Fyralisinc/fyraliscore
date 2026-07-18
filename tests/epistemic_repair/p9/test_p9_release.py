from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path

from lib.evaluation.epistemic_repair.p9_release import (
    CONTENT_DIGEST_ALGORITHM, ManifestEvidence, READY_VERDICT,
    REVIEW_SCHEMA_VERSION, REQUIRED_PHASES, build_release_report, reproduce, seal_manifest,
)


def _digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _phase(path: Path, phase: str, commit: str, *, metric=True):
    body = {
        "schema_version": f"{phase}-normalized-v1", "commit": commit,
        "phase_exit_ready": True, "hard_gates": {f"{phase}-gate": True},
        "p9_continuous_metrics": ([{
            "name": f"{phase}-metric", "numerator": 1, "denominator": 1, "value": 1.0,
            "coverage": 1.0, "uncertainty": "not_applicable", "status": "pass",
            "operator": ">=", "threshold": 1.0, "source_artifact_digest": "a" * 64,
            "worst_cases": [],
        }] if metric else []),
        "strategic_decision": "primary_memory_earned" if phase == "p7" else None,
    }
    body["content_digest"] = _digest(body)
    path.write_text(json.dumps(body, sort_keys=True))
    return ManifestEvidence(
        str(path), body["schema_version"], commit, sha256(path.read_bytes()).hexdigest(),
        body["content_digest"], "content_digest", CONTENT_DIGEST_ALGORITHM,
        "integrated_current", (f"{phase}-gate",), (f"{phase}-metric",) if metric else (),
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


def test_metric_free_phase_requires_explicit_empty_required_set(tmp_path: Path):
    _, entries, manifest, _ = _setup(tmp_path)
    assert reproduce(manifest)["phase_evidence"][0]["metrics_valid"] is True
    bad = dict(entries)
    bad["p0"] = ManifestEvidence(**{
        **bad["p0"].__dict__, "required_metric_ids": ("invented",),
    })
    rejected = seal_manifest(coordinator_id="coordinator", release_commit="a" * 40, required_current=bad)
    assert reproduce(rejected)["required_evidence_green"] is False


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
