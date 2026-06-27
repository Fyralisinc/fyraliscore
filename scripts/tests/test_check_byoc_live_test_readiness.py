from __future__ import annotations

import json
from pathlib import Path

from scripts.check_byoc_live_test_readiness import main


ROOT = Path(__file__).resolve().parents[2]


def test_check_byoc_live_test_readiness_prints_manual_report_without_credentials(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr(
        "services.platform.runtime.byoc_live_test_readiness.shutil.which",
        lambda _: None,
    )

    code = main(["--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.live_test_readiness.v1"
    assert payload["status"] == "manual_required"
    assert payload["required_checks_passed"] is True
    assert payload["live_aws_ready"] is False
    assert payload["next_required_action"] == "configure_aws_access"
    assert "123456789012" not in rendered
    assert "arn:" not in rendered.lower()
    assert "aws_secret_access_key" not in rendered.lower()


def test_check_byoc_live_test_readiness_requires_aws_access_when_requested(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr(
        "services.platform.runtime.byoc_live_test_readiness.shutil.which",
        lambda _: None,
    )

    code = main(["--json", "--require-aws-access"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "fail"
    assert payload["required_checks_passed"] is False
    assert payload["live_aws_ready"] is False


def test_check_byoc_live_test_readiness_writes_output_file(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    output = tmp_path / "readiness.json"
    monkeypatch.setattr(
        "services.platform.runtime.byoc_live_test_readiness.shutil.which",
        lambda _: None,
    )

    code = main(["--json", "--output", str(output)])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_check_byoc_live_test_readiness_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["report"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.live_test_readiness.v1"
    )
