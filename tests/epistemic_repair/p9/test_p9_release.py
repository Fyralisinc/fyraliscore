from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path

from lib.evaluation.epistemic_repair.p9_release import (
    ALLOWED_VERDICTS,
    PhaseEvidence,
    READY_VERDICT,
    build_release_report,
)


def _artifact(path: Path, phase: str, commit: str, *, ready: bool = True) -> PhaseEvidence:
    payload = {
        "schema_version": f"{phase}-test-v1",
        "commit": commit,
        "passed": ready,
        "phase_exit_ready": ready,
        "hard_gates": {"constitutional": ready},
        "p9_continuous_metrics": [
            {
                "name": "coverage",
                "value": 1.0,
                "numerator": 1,
                "denominator": 1,
                "coverage": 1.0,
                "uncertainty": "not_applicable",
            }
        ],
    }
    if phase == "p7":
        payload["strategic_decision"] = "primary_memory_earned"
    raw = json.dumps(payload, sort_keys=True).encode()
    path.write_bytes(raw)
    return PhaseEvidence(phase, path, sha256(raw).hexdigest())


def test_p9_authorizes_only_one_commit_with_all_green_evidence(tmp_path: Path) -> None:
    commit = "a" * 40
    evidence = [_artifact(tmp_path / f"{phase}.json", phase, commit) for phase in (f"p{i}" for i in range(9))]
    report = build_release_report(release_commit=commit, worktree_clean=True, evidence=evidence)
    assert report["verdict"] == READY_VERDICT
    assert report["completion_authorized"] is True
    assert report["evidence_complete"] is True


def test_p9_fails_closed_on_missing_mixed_or_tampered_evidence(tmp_path: Path) -> None:
    commit = "b" * 40
    evidence = [_artifact(tmp_path / f"{phase}.json", phase, commit) for phase in (f"p{i}" for i in range(8))]
    evidence[2] = PhaseEvidence(evidence[2].phase, evidence[2].path, "0" * 64)
    report = build_release_report(release_commit=commit, worktree_clean=False, evidence=evidence)
    assert report["verdict"] == "insufficient_evidence"
    assert report["completion_authorized"] is False
    assert report["missing_phases"] == ["p8"]
    assert report["phase_evidence"][2]["digest_verified"] is False


def test_p9_verdict_precedence_is_constitutional_then_memory_then_ops(tmp_path: Path) -> None:
    commit = "c" * 40
    evidence = [_artifact(tmp_path / f"{phase}.json", phase, commit) for phase in (f"p{i}" for i in range(9))]
    p2 = json.loads(evidence[2].path.read_text())
    p2["hard_gates"] = {"truth": False}
    p2["phase_exit_ready"] = False
    raw = json.dumps(p2, sort_keys=True).encode()
    evidence[2].path.write_bytes(raw)
    evidence[2] = PhaseEvidence("p2", evidence[2].path, sha256(raw).hexdigest())
    report = build_release_report(release_commit=commit, worktree_clean=True, evidence=evidence)
    assert report["verdict"] == "safety_or_truth_blocked"
    assert report["verdict"] in ALLOWED_VERDICTS


def test_p9_recomputes_semantic_and_operational_health_from_hard_gates(
    tmp_path: Path,
) -> None:
    commit = "d" * 40
    evidence = [
        _artifact(tmp_path / f"{phase}.json", phase, commit)
        for phase in (f"p{i}" for i in range(9))
    ]
    p6 = json.loads(evidence[6].path.read_text())
    p6["hard_gates"] = {"mixed_stream_quality": False}
    # A stale or dishonest summary flag must not override member evidence.
    p6["phase_exit_ready"] = True
    raw = json.dumps(p6, sort_keys=True).encode()
    evidence[6].path.write_bytes(raw)
    evidence[6] = PhaseEvidence("p6", evidence[6].path, sha256(raw).hexdigest())

    report = build_release_report(
        release_commit=commit,
        worktree_clean=True,
        evidence=evidence,
    )

    assert report["semantic_green"] is False
    assert report["verdict"] == "mechanically_ready_semantically_insufficient"
    assert report["completion_authorized"] is False


def test_p9_rejects_historical_evidence_as_required_current_phase(
    tmp_path: Path,
) -> None:
    commit = "e" * 40
    evidence = [
        _artifact(tmp_path / f"{phase}.json", phase, commit)
        for phase in (f"p{i}" for i in range(9))
    ]
    p5 = evidence[5]
    evidence[5] = PhaseEvidence(
        p5.phase,
        p5.path,
        p5.expected_sha256,
        evidence_class="historical_falsifying",
    )

    report = build_release_report(
        release_commit=commit,
        worktree_clean=True,
        evidence=evidence,
    )

    assert report["evidence_complete"] is False
    assert report["verdict"] == "insufficient_evidence"
    assert report["completion_authorized"] is False
