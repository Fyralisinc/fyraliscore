from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.run_byoc_agent_runner import DEFAULT_INSTALL_TOKEN_ENV, main


INSTALL_TOKEN = "local-install-token-for-agent-runner-cli-tests"


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
