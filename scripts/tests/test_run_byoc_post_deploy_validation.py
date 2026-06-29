from __future__ import annotations

import json
from pathlib import Path

from scripts.run_byoc_post_deploy_validation import main


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "deploy/byoc/dataplane.example.yaml"
ENV_TEMPLATE = ROOT / ".env.production.example"


def test_run_byoc_post_deploy_validation_cli_markdown(capsys) -> None:
    code = main([
        "--manifest",
        str(MANIFEST),
        "--env-file",
        str(ENV_TEMPLATE),
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert "# BYOC Post-Deploy Validation" in captured.out
    assert "| manifest_contract | true | pass |" in captured.out


def test_run_byoc_post_deploy_validation_cli_json(capsys) -> None:
    code = main([
        "--manifest",
        str(MANIFEST),
        "--env-file",
        str(ENV_TEMPLATE),
        "--json",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["status"] == "pass"
    assert payload["required_checks_passed"] is True


def test_run_byoc_post_deploy_validation_cli_require_live_fails(capsys) -> None:
    code = main([
        "--manifest",
        str(MANIFEST),
        "--env-file",
        str(ENV_TEMPLATE),
        "--require-live",
        "--json",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "fail"
    assert any(check["name"] == "gateway_health" for check in payload["checks"])


def test_run_byoc_post_deploy_validation_cli_rejects_bad_worker_arg(
    capsys,
) -> None:
    code = main([
        "--manifest",
        str(MANIFEST),
        "--worker-health",
        "bad",
    ])

    captured = capsys.readouterr()
    assert code == 2
    assert "NAME=URL" in captured.err
