from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.run_byoc_agent_probe import DEFAULT_INSTALL_TOKEN_ENV, main


INSTALL_TOKEN = "local-install-token-for-agent-probe-cli-tests"


def test_run_byoc_agent_probe_json_output(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main(["--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    serialized = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["status"] == "pass"
    assert payload["control_plane_mode"] == "mock"
    assert payload["enrollment_status"] == "pass"
    assert payload["heartbeat_status"] == "pass"
    assert INSTALL_TOKEN not in serialized


def test_run_byoc_agent_probe_yaml_output(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)

    code = main([])

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert code == 0
    assert payload["required_checks_passed"] is True
    assert payload["checks"][0]["name"] == "manifest_schema"


def test_run_byoc_agent_probe_writes_output_file(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_INSTALL_TOKEN_ENV, INSTALL_TOKEN)
    output = tmp_path / "reports" / "byoc-agent-probe.json"

    code = main(["--json", "--output", str(output)])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_run_byoc_agent_probe_requires_install_token_env(capsys) -> None:
    code = main(["--json", "--install-token-env", "MISSING_BYOC_TOKEN"])

    captured = capsys.readouterr()
    assert code == 2
    assert "MISSING_BYOC_TOKEN" in captured.err
    assert captured.out == ""
