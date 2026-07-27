from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.validate_source_certification_manifest import main as script_main
from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.cli import main as certification_main
from services.ingest.source_certification.evaluator import sign_manifest
from services.ingest.source_certification.promotion import (
    PromotionManifestError,
    load_promotion_manifest,
    validate_promotion_manifest,
)
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


TARGET_SHA = "a" * 40
OTHER_SHA = "b" * 40
SIGNING_KEY = b"unit-test-source-certification-key"


def _suite_result(
    kind: str,
    *,
    stable_rate: float,
    headroom_ratio: float | None = None,
) -> dict[str, object]:
    metrics: dict[str, float] = {
        "stable_rate": stable_rate,
        "tenants": 2.0,
        "installations_per_tenant": 2.0,
        "replicas": 2.0,
    }
    if headroom_ratio is not None:
        metrics["headroom_ratio"] = headroom_ratio
    return {
        "kind": kind,
        "state": "passed",
        "artifact_uri": f"artifact://suite/{kind}",
        "started_at": "2026-07-25T12:00:00+00:00",
        "completed_at": "2026-07-25T13:00:00+00:00",
        "metrics": metrics,
        "limiting_component": "provider_quota",
        "failures": [],
    }


def _source_artifact(source_id: str) -> dict[str, object]:
    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    return {
        "source_id": source_id,
        "state": "passed",
        "failures": [],
        "input": {
            "local_correctness": "passed",
            "scenario_results": [
                {
                    "scenario_id": scenario_id,
                    "state": "passed",
                    "artifact_uri": f"artifact://scenario/{scenario_id}",
                    "failures": [],
                }
                for scenario_id in spec.required_scenarios
            ],
            "provider_safe_suites": [
                _suite_result(kind, stable_rate=100.0)
                for kind in ("historical", "live", "combined")
            ],
            "fyralis_ceiling_suites": [
                _suite_result(
                    kind,
                    stable_rate=120.0,
                    headroom_ratio=1.2,
                )
                for kind in ("historical", "live", "combined")
            ],
            "fault_recovery_suites": [
                _suite_result(kind, stable_rate=100.0)
                for kind in ("historical", "live", "combined")
            ],
            "legacy_reference_count": 0,
            "skipped_tests": [],
            "todos": [],
            "canary": {
                "state": "passed",
                "account_type": spec.canary.account_type,
                "operation_results": [
                    {
                        "operation_id": operation_id,
                        "state": "passed",
                        "artifact_uri": f"artifact://canary/{operation_id}",
                        "failures": [],
                    }
                    for operation_id in spec.canary.required_operations
                ],
            },
        },
    }


def _signed_manifest(
    *,
    commit_sha: str = TARGET_SHA,
) -> dict[str, object]:
    manifest: dict[str, object] = {
        "manifest_version": 2,
        "state": "passed",
        "evaluated_at": "2026-07-25T12:00:00+00:00",
        "commit_sha": commit_sha,
        "required_sources": len(CANONICAL_SOURCE_IDS),
        "passed_sources": len(CANONICAL_SOURCE_IDS),
        "missing_sources": [],
        "failures": {},
        "sources": [
            _source_artifact(source_id) for source_id in CANONICAL_SOURCE_IDS
        ],
        "legacy_ratchet_clean": True,
    }
    manifest["signature"] = sign_manifest(manifest, SIGNING_KEY)
    return manifest


def _resign(manifest: dict[str, object]) -> None:
    manifest.pop("signature", None)
    manifest["signature"] = sign_manifest(manifest, SIGNING_KEY)


def test_valid_signed_27_source_manifest_is_accepted() -> None:
    result = validate_promotion_manifest(
        _signed_manifest(),
        target_sha=TARGET_SHA,
        signing_key=SIGNING_KEY,
    )
    assert result["state"] == "passed"
    assert result["commit_sha"] == TARGET_SHA
    assert result["verified_sources"] == 27
    assert result["legacy_ratchet_clean"] is True


def test_downloaded_evaluator_metric_pair_shape_is_accepted() -> None:
    manifest = _signed_manifest()
    first_input = manifest["sources"][0]["input"]  # type: ignore[index]
    for field in (
        "provider_safe_suites",
        "fyralis_ceiling_suites",
        "fault_recovery_suites",
    ):
        for suite in first_input[field]:
            suite["metrics"] = list(suite["metrics"].items())
    _resign(manifest)

    result = validate_promotion_manifest(
        manifest,
        target_sha=TARGET_SHA,
        signing_key=SIGNING_KEY,
    )

    assert result["state"] == "passed"


def test_tampered_manifest_fails_signature_verification() -> None:
    manifest = _signed_manifest()
    manifest["state"] = "blocked"
    with pytest.raises(
        PromotionManifestError,
        match="content digest verification failed",
    ):
        validate_promotion_manifest(
            manifest,
            target_sha=TARGET_SHA,
            signing_key=SIGNING_KEY,
        )


def test_manifest_signed_by_another_key_is_rejected() -> None:
    with pytest.raises(PromotionManifestError, match="HMAC verification failed"):
        validate_promotion_manifest(
            _signed_manifest(),
            target_sha=TARGET_SHA,
            signing_key=b"different-key",
        )


def test_authentic_blocked_manifest_cannot_promote() -> None:
    manifest = _signed_manifest()
    manifest["state"] = "blocked"
    _resign(manifest)
    with pytest.raises(PromotionManifestError, match="state must equal passed"):
        validate_promotion_manifest(
            manifest,
            target_sha=TARGET_SHA,
            signing_key=SIGNING_KEY,
        )


def test_manifest_and_target_commit_must_match() -> None:
    with pytest.raises(PromotionManifestError, match="does not match target"):
        validate_promotion_manifest(
            _signed_manifest(commit_sha=OTHER_SHA),
            target_sha=TARGET_SHA,
            signing_key=SIGNING_KEY,
        )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda manifest: manifest["sources"].pop(),  # type: ignore[union-attr]
            "each canonical source exactly once",
        ),
        (
            lambda manifest: manifest.__setitem__("passed_sources", 26),
            "passed_sources must equal 27",
        ),
        (
            lambda manifest: manifest.__setitem__("manifest_version", 1),
            "manifest_version must equal 2",
        ),
        (
            lambda manifest: manifest.__setitem__(
                "legacy_ratchet_clean",
                False,
            ),
            "legacy_ratchet_clean must be true",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"].__setitem__(  # type: ignore[index,union-attr]
                "legacy_reference_count",
                1,
            ),
            "legacy_reference_count must equal 0",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"][  # type: ignore[index]
                "scenario_results"
            ].pop(),
            "scenario_results must cover exactly",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"]["canary"][  # type: ignore[index]
                "operation_results"
            ].pop(),
            "operation_results must cover exactly",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"][  # type: ignore[index]
                "scenario_results"
            ][0].__setitem__("state", "failed"),
            "scenario_results\\[0\\].state must equal passed",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"][  # type: ignore[index]
                "provider_safe_suites"
            ].pop(),
            "provider_safe_suites must cover exactly",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"].pop(  # type: ignore[index]
                "fyralis_ceiling_suites"
            ),
            "fyralis_ceiling_suites must be an array",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"].pop(  # type: ignore[index]
                "fault_recovery_suites"
            ),
            "fault_recovery_suites must be an array",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"][  # type: ignore[index]
                "fyralis_ceiling_suites"
            ][0]["metrics"].__setitem__("headroom_ratio", 1.1),
            "headroom_ratio must equal ceiling stable_rate",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"][  # type: ignore[index]
                "fyralis_ceiling_suites"
            ][0]["metrics"].__setitem__("stable_rate", 90.0),
            "stable_rate must be >= the provider-safe stable_rate",
        ),
        (
            lambda manifest: manifest["sources"][0]["input"][  # type: ignore[index]
                "provider_safe_suites"
            ][0]["metrics"].__setitem__("tenants", 1.0),
            "tenants must equal declared topology value 2",
        ),
    ],
)
def test_signed_but_incomplete_release_state_is_rejected(
    mutate,
    message: str,
) -> None:  # noqa: ANN001
    manifest = _signed_manifest()
    mutate(manifest)
    _resign(manifest)
    with pytest.raises(PromotionManifestError, match=message):
        validate_promotion_manifest(
            manifest,
            target_sha=TARGET_SHA,
            signing_key=SIGNING_KEY,
        )


def test_loader_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text('{"state":"passed","state":"blocked"}', encoding="utf-8")
    with pytest.raises(PromotionManifestError, match="duplicate JSON object key"):
        load_promotion_manifest(path)


def test_dependency_light_script_fails_closed_without_key(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # noqa: ANN001
    path = tmp_path / "source-certification-manifest.json"
    path.write_text(json.dumps(_signed_manifest()), encoding="utf-8")
    monkeypatch.delenv("CERTIFICATION_TEST_KEY", raising=False)
    assert script_main(
        [
            "--manifest",
            str(path),
            "--target-sha",
            TARGET_SHA,
            "--signing-key-env",
            "CERTIFICATION_TEST_KEY",
        ]
    ) == 2
    assert "empty or unset" in capsys.readouterr().err


def test_dependency_light_script_accepts_valid_bundle(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # noqa: ANN001
    path = tmp_path / "source-certification-manifest.json"
    path.write_text(json.dumps(_signed_manifest()), encoding="utf-8")
    monkeypatch.setenv("CERTIFICATION_TEST_KEY", SIGNING_KEY.decode())
    assert script_main(
        [
            "--manifest",
            str(path),
            "--target-sha",
            TARGET_SHA,
            "--signing-key-env",
            "CERTIFICATION_TEST_KEY",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["verified_sources"] == 27


def test_certification_cli_verifies_promotion_manifest(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # noqa: ANN001
    path = tmp_path / "source-certification-manifest.json"
    path.write_text(json.dumps(_signed_manifest()), encoding="utf-8")
    monkeypatch.setenv("CERTIFICATION_TEST_KEY", SIGNING_KEY.decode())
    assert certification_main(
        [
            "verify-manifest",
            "--manifest",
            str(path),
            "--target-sha",
            TARGET_SHA,
            "--signing-key-env",
            "CERTIFICATION_TEST_KEY",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["verified_sources"] == 27


def test_existing_diagnostic_signing_without_commit_remains_supported(
    tmp_path: Path,
    monkeypatch,
) -> None:  # noqa: ANN001
    output = tmp_path / "diagnostic-manifest.json"
    monkeypatch.setenv("CERTIFICATION_TEST_KEY", SIGNING_KEY.decode())
    monkeypatch.setattr(
        "services.ingest.source_certification.cli._strict_ratchet_clean",
        lambda: False,
    )
    assert certification_main(
        [
            "manifest",
            "--input-dir",
            str(tmp_path),
            "--output",
            str(output),
            "--signing-key-env",
            "CERTIFICATION_TEST_KEY",
        ]
    ) == 1
    rendered = json.loads(output.read_text(encoding="utf-8"))
    signature = rendered.pop("signature")
    assert "commit_sha" not in rendered
    assert signature == sign_manifest(rendered, SIGNING_KEY)


def test_signing_cli_binds_signature_to_commit_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:  # noqa: ANN001
    output = tmp_path / "manifest.json"
    monkeypatch.setenv("CERTIFICATION_TEST_KEY", SIGNING_KEY.decode())
    monkeypatch.setattr(
        "services.ingest.source_certification.cli._strict_ratchet_clean",
        lambda: False,
    )
    # Empty inputs intentionally produce a blocked diagnostic manifest.  This
    # test checks commit/signature serialization, not release eligibility.
    assert certification_main(
        [
            "manifest",
            "--input-dir",
            str(tmp_path),
            "--output",
            str(output),
            "--commit-sha",
            TARGET_SHA,
            "--signing-key-env",
            "CERTIFICATION_TEST_KEY",
        ]
    ) == 1
    rendered = json.loads(output.read_text(encoding="utf-8"))
    signature = rendered.pop("signature")
    assert rendered["commit_sha"] == TARGET_SHA
    assert signature == sign_manifest(rendered, SIGNING_KEY)


def test_promotion_workflow_checks_artifact_before_branch_push() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    workflow = (
        repo_root / ".github/workflows/promote-production.yml"
    ).read_text(encoding="utf-8")

    assert "source_certification_run_id:" in workflow
    assert "source_certification_artifact_name:" in workflow
    assert "actions/download-artifact@v4" in workflow
    assert "SOURCE_CERTIFICATION_MANIFEST_SIGNING_KEY" in workflow
    assert '"${workflow_name}" != "Source Certification"' in workflow
    assert 'head_sha}" != "${TARGET_SHA}' in workflow
    assert (
        workflow.index("Verify source certification workflow run")
        < workflow.index("Download signed source certification bundle")
        < workflow.index("Validate signed 27-source certification manifest")
        < workflow.index("Promote SHA to production branch")
    )


def test_source_certification_workflow_is_fail_closed_and_sha_bound() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    workflow = (
        repo_root / ".github/workflows/source-certification.yml"
    ).read_text(encoding="utf-8")

    assert "name: Source Certification" in workflow
    assert "environment:" in workflow
    assert "name: source-certification" in workflow
    assert '"${workflow_name}" != "Source Certification Evidence"' in workflow
    assert '"${head_sha}" != "${TARGET_SHA}"' in workflow
    assert "inventory --require-ready" in workflow
    assert "check_source_architecture_ratchet.py --no-baseline" in workflow
    assert "--signing-key-env SOURCE_CERTIFICATION_MANIFEST_SIGNING_KEY" in workflow
    assert "actions/upload-artifact@v4" in workflow
