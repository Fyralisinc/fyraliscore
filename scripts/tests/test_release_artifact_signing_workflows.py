from __future__ import annotations

from pathlib import Path

import yaml  # type: ignore[import-untyped]


ROOT = Path(__file__).resolve().parents[2]


def _load_workflow(path: str) -> dict:
    with open(ROOT / path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _step_names(job: dict) -> list[str]:
    return [step.get("name", "") for step in job.get("steps", [])]


def _step_run(job: dict, name: str) -> str:
    for step in job.get("steps", []):
        if step.get("name") == name:
            return str(step.get("run") or step.get("with", {}).get("script", ""))
    raise AssertionError(f"missing step {name!r}")


def _github_on(workflow: dict) -> dict:
    return workflow.get("on") or workflow.get(True)


def test_ci_signs_and_verifies_sbom_artifacts() -> None:
    workflow = _load_workflow(".github/workflows/ci.yml")
    job = workflow["jobs"]["security-supply-chain"]
    names = _step_names(job)

    assert job["permissions"]["id-token"] == "write"
    assert "Generate SBOM checksums" in names
    assert "Install cosign" in names
    assert "Sign SBOM artifacts" in names
    assert "Verify SBOM signatures" in names

    sign_run = _step_run(job, "Sign SBOM artifacts")
    verify_run = _step_run(job, "Verify SBOM signatures")
    assert "cosign sign-blob --yes" in sign_run
    assert "cosign verify-blob" in verify_run
    assert "sha256sum -c fyraliscore-sboms.SHA256SUMS" in verify_run

    upload = next(
        step
        for step in job["steps"]
        if step.get("name") == "Upload SBOM artifacts"
    )
    uploaded_paths = upload["with"]["path"]
    assert "fyraliscore-source.spdx.json.sigstore" in uploaded_paths
    assert "fyraliscore-image.cdx.json.sigstore" in uploaded_paths
    assert "fyraliscore-sboms.SHA256SUMS.sigstore" in uploaded_paths


def test_deploy_verifies_signed_artifacts_before_ssh() -> None:
    workflow = _load_workflow(".github/workflows/deploy-production.yml")
    deploy = workflow["jobs"]["deploy"]
    names = _step_names(deploy)

    assert workflow["permissions"]["actions"] == "read"
    assert _github_on(workflow)["workflow_dispatch"]["inputs"]["ci_run_id"][
        "required"
    ] is True
    assert "Download signed SBOM artifacts from CI run" in names
    assert "Download signed SBOM artifacts from requested CI run" in names
    assert "Verify signed release artifacts" in names
    assert names.index("Verify signed release artifacts") < names.index("Deploy via SSH")

    verify_run = _step_run(deploy, "Verify signed release artifacts")
    assert "refs/heads/production" in verify_run
    assert "cosign verify-blob" in verify_run
    assert "sha256sum -c fyraliscore-sboms.SHA256SUMS" in verify_run

    deploy_run = _step_run(deploy, "Deploy via SSH")
    assert 'PREV_SHA="$(git rev-parse HEAD)"' in deploy_run
    assert "Gateway failed health after deploy; rolling back" in deploy_run
    assert "scripts/check_product_slo_gate.py" in deploy_run
    assert "Product SLO gate failed after deploy; rolling back" in deploy_run
    assert 'git reset --hard "${PREV_SHA}"' in deploy_run


def test_staging_deploy_verifies_signed_main_artifacts_before_ssh() -> None:
    workflow = _load_workflow(".github/workflows/deploy-staging.yml")
    deploy = workflow["jobs"]["deploy"]
    names = _step_names(deploy)

    assert workflow["permissions"]["actions"] == "read"
    assert _github_on(workflow)["workflow_run"]["branches"] == ["main"]
    assert _github_on(workflow)["workflow_dispatch"]["inputs"]["ci_run_id"][
        "required"
    ] is True
    assert "Download signed SBOM artifacts from CI run" in names
    assert "Download signed SBOM artifacts from requested CI run" in names
    assert "Verify signed release artifacts" in names
    assert names.index("Verify signed release artifacts") < names.index("Deploy via SSH")

    verify_run = _step_run(deploy, "Verify signed release artifacts")
    assert "refs/heads/main" in verify_run
    assert "cosign verify-blob" in verify_run
    assert "sha256sum -c fyraliscore-sboms.SHA256SUMS" in verify_run

    deploy_run = _step_run(deploy, "Deploy via SSH")
    assert "git fetch origin main" in deploy_run
    assert 'PREV_SHA="$(git rev-parse HEAD)"' in deploy_run
    assert "Gateway failed health after staging deploy; rolling back" in deploy_run
    assert "scripts/check_product_slo_gate.py" in deploy_run
    assert "Product SLO gate failed after staging deploy; rolling back" in deploy_run
    assert 'git reset --hard "${PREV_SHA}"' in deploy_run


def test_production_promotion_requires_approved_successful_staging_sha() -> None:
    workflow = _load_workflow(".github/workflows/promote-production.yml")
    promote = workflow["jobs"]["promote"]
    names = _step_names(promote)
    on = _github_on(workflow)

    assert "workflow_dispatch" in on
    assert on["workflow_dispatch"]["inputs"]["target_sha"]["required"] is True
    assert (
        on["workflow_dispatch"]["inputs"]["staging_deploy_run_id"]["required"]
        is True
    )
    assert (
        on["workflow_dispatch"]["inputs"]["confirm_staging_validation"]["type"]
        == "boolean"
    )
    assert workflow["permissions"]["contents"] == "write"
    assert workflow["permissions"]["actions"] == "read"
    assert promote["if"] == "inputs.confirm_staging_validation == true"
    assert promote["environment"]["name"] == "production"

    assert "Validate promotion inputs" in names
    assert "Verify staging deployment run" in names
    assert "Promote SHA to production branch" in names
    assert names.index("Verify staging deployment run") < names.index(
        "Promote SHA to production branch"
    )

    validate_run = _step_run(promote, "Validate promotion inputs")
    assert "git merge-base --is-ancestor" in validate_run
    assert "origin/main" in validate_run
    assert "release_notes_path does not exist" in validate_run

    verify_run = _step_run(promote, "Verify staging deployment run")
    assert "Deploy to Staging" in verify_run
    assert "conclusion" in verify_run
    assert "success" in verify_run
    assert "head_sha" in verify_run
    assert "TARGET_SHA" in verify_run

    promote_run = _step_run(promote, "Promote SHA to production branch")
    assert 'git push origin "${TARGET_SHA}:refs/heads/production"' in promote_run
