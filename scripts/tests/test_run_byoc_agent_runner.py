from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.run_byoc_agent_runner import DEFAULT_INSTALL_TOKEN_ENV, main


INSTALL_TOKEN = "local-install-token-for-agent-runner-cli-tests"
ROOT = Path(__file__).resolve().parents[2]
BUNDLE_NEXT = ROOT / "deploy/byoc/bootstrap-bundle.next.example.yaml"


def test_run_byoc_agent_runner_json_output(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main(["--json", "--iterations", "2"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    serialized = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["status"] == "pass"
    assert payload["control_plane_mode"] == "mock"
    assert payload["enrollment_status"] == "pass"
    assert payload["iterations_completed"] == 2
    assert payload["desired_state_poll_count"] == 2
    assert payload["heartbeat_count"] == 2
    assert payload["final_rollout_action"] == "none"
    assert INSTALL_TOKEN not in serialized


def test_run_byoc_agent_runner_apply_plan_output(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main(
        [
            "--json",
            "--mock-desired-revision",
            "2026.06.26-2",
            "--mock-config-epoch",
            "4",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    serialized = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["final_rollout_action"] == "apply_revision"
    assert payload["apply_plan_count"] == 1
    assert payload["apply_plans"][0]["execution_mode"] == "plan_only"
    assert payload["apply_plans"][0]["mutating_step_count"] == 0
    assert payload["apply_plans"][0]["config_epoch"] == 4
    assert INSTALL_TOKEN not in serialized
    assert "signature" not in serialized.lower()


def test_run_byoc_agent_runner_artifact_verification_output(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main(
        [
            "--json",
            "--mock-desired-revision",
            "2026.06.26-2",
            "--mock-config-epoch",
            "4",
            "--bootstrap-bundle",
            str(BUNDLE_NEXT),
            "--verify-local-bundle-files",
            "--repo-root",
            str(ROOT),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    serialized_artifacts = json.dumps(
        payload["artifact_verifications"],
        sort_keys=True,
    )
    assert code == 0
    assert payload["artifact_verification_count"] == 1
    assert payload["artifact_verifications"][0]["artifact_count"] == 7
    assert payload["artifact_verifications"][0]["digest_pinned_artifact_count"] == 7
    assert payload["artifact_verifications"][0]["local_digest_checked_count"] == 1
    assert payload["apply_plans"][0]["artifact_verification_status"] == "pass"
    assert INSTALL_TOKEN not in serialized_artifacts
    assert "://" not in serialized_artifacts
    assert "signature" not in serialized_artifacts.lower()
    assert "sigstore" not in serialized_artifacts.lower()


def test_run_byoc_agent_runner_yaml_output(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main([])

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert code == 0
    assert payload["required_checks_passed"] is True
    assert payload["checks"][0]["name"] == "manifest_schema"


def test_run_byoc_agent_runner_writes_output_file(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)
    output = tmp_path / "reports" / "byoc-agent-runner.json"

    code = main(["--json", "--output", str(output)])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_run_byoc_agent_runner_requires_install_token_env(capsys) -> None:
    code = main(["--json", "--install-token-env", "MISSING_BYOC_TOKEN"])

    captured = capsys.readouterr()
    assert code == 2
    assert "MISSING_BYOC_TOKEN" in captured.err
    assert captured.out == ""


def test_run_byoc_agent_runner_rejects_unbounded_iterations(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main(["--json", "--iterations", "11"])

    captured = capsys.readouterr()
    assert code == 2
    assert "--iterations must be between 1 and 10" in captured.err
    assert captured.out == ""


def test_run_byoc_agent_runner_requires_bundle_for_local_digest_check(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main(["--json", "--verify-local-bundle-files"])

    captured = capsys.readouterr()
    assert code == 2
    assert "--verify-local-bundle-files requires --bootstrap-bundle" in captured.err
    assert captured.out == ""


def test_run_byoc_agent_runner_rejects_mock_desired_revision_for_live_url(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main(
        [
            "--json",
            "--control-plane-url",
            "https://control.example.com",
            "--mock-desired-revision",
            "2026.06.26-2",
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert "--mock-desired-revision is allowed only" in captured.err
    assert captured.out == ""
