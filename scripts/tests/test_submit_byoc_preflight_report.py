from __future__ import annotations

import json
from pathlib import Path

from scripts.submit_byoc_preflight_report import (
    DEFAULT_SIGNING_SECRET_ENV,
    main,
)
from services.platform.runtime.byoc_preflight_bundle import (
    ByocPreflightBundleInputs,
    render_preflight_report_json,
    run_byoc_preflight_bundle,
)
from services.platform.runtime.byoc_preflight_intake import (
    ByocPreflightReportSubmissionRequest,
    validate_preflight_report_submission,
)


ROOT = Path(__file__).resolve().parents[2]
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"
IAC_PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
ENV_TEMPLATE = ROOT / ".env.production.example"
SIGNING_SECRET = "local-control-plane-preflight-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"
AGENT_ID = "agt_preflightsubmit01"
AGENT_VERSION = "2026.06.26-preflight-submit"


def _preflight_report(path: Path) -> Path:
    report = run_byoc_preflight_bundle(
        ByocPreflightBundleInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            iac_package_path=IAC_PACKAGE,
            bootstrap_bundle_path=BUNDLE,
            bootstrap_plan_path=PLAN,
            env_path=ENV_TEMPLATE,
            repo_root=ROOT,
        )
    )
    path.write_text(render_preflight_report_json(report), encoding="utf-8")
    return path


def test_submit_byoc_preflight_report_prints_signed_submission(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    report_path = _preflight_report(tmp_path / "preflight-report.json")
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(
        [
            "--preflight-report",
            str(report_path),
            "--agent-id",
            AGENT_ID,
            "--agent-version",
            AGENT_VERSION,
            "--key-ref",
            SIGNING_KEY_REF,
            "--nonce",
            "nonce-preflight-submit-test-001",
            "--submitted-at",
            "2026-06-26T13:00:00+00:00",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    request = ByocPreflightReportSubmissionRequest.model_validate(payload)
    assert code == 0
    assert request.schema_version == "fyralis.byoc.preflight_report_submission.v1"
    assert request.preflight_report.schema_version == "fyralis.byoc.preflight_bundle.v1"
    assert request.preflight_report.status == "pass"
    assert validate_preflight_report_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []
    assert SIGNING_SECRET not in rendered
    assert "ghcr.io" not in rendered
    assert "prepared_commands" not in rendered
    assert "postgresql://" not in rendered
    assert captured.err == ""


def test_submit_byoc_preflight_report_writes_output_file(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    report_path = _preflight_report(tmp_path / "preflight-report.json")
    output_path = tmp_path / "out" / "preflight-submission.json"
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(
        [
            "--preflight-report",
            str(report_path),
            "--agent-id",
            AGENT_ID,
            "--agent-version",
            AGENT_VERSION,
            "--key-ref",
            SIGNING_KEY_REF,
            "--nonce",
            "nonce-preflight-submit-test-002",
            "--output",
            str(output_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output_path.read_text(encoding="utf-8")) == json.loads(
        captured.out
    )


def test_submit_byoc_preflight_report_posts_signed_submission(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    report_path = _preflight_report(tmp_path / "preflight-report.json")
    posted: dict[str, object] = {}
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def read(self) -> bytes:
            return json.dumps(
                {
                    "schema_version": "fyralis.byoc.preflight_report_receipt.v1",
                    "status": "accepted",
                    "receipt_id": "pfrep_0123456789abcdef0123456789abcdef",
                    "stored_scope": "sanitized_metadata_only",
                }
            ).encode("utf-8")

    def _fake_urlopen(request, *, timeout):
        posted["url"] = request.full_url
        posted["timeout"] = timeout
        posted["body"] = json.loads(request.data.decode("utf-8"))
        return _Response()

    monkeypatch.setattr(
        "scripts.submit_byoc_preflight_report.urllib.request.urlopen",
        _fake_urlopen,
    )

    code = main(
        [
            "--preflight-report",
            str(report_path),
            "--agent-id",
            AGENT_ID,
            "--agent-version",
            AGENT_VERSION,
            "--key-ref",
            SIGNING_KEY_REF,
            "--nonce",
            "nonce-preflight-submit-test-003",
            "--submit-url",
            "https://control.example.com/byoc/control-plane/preflight-reports",
            "--timeout-seconds",
            "3",
        ]
    )

    captured = capsys.readouterr()
    receipt = json.loads(captured.out)
    posted_body = json.dumps(posted["body"], sort_keys=True)
    assert code == 0
    assert posted["url"] == (
        "https://control.example.com/byoc/control-plane/preflight-reports"
    )
    assert posted["timeout"] == 3.0
    assert receipt["receipt_id"] == "pfrep_0123456789abcdef0123456789abcdef"
    assert "ghcr.io" not in posted_body
    assert "prepared_commands" not in posted_body
    assert "postgresql://" not in posted_body


def test_submit_byoc_preflight_report_requires_signing_secret(
    tmp_path: Path,
    capsys,
) -> None:
    report_path = _preflight_report(tmp_path / "preflight-report.json")

    code = main(
        [
            "--preflight-report",
            str(report_path),
            "--agent-id",
            AGENT_ID,
            "--agent-version",
            AGENT_VERSION,
            "--key-ref",
            SIGNING_KEY_REF,
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert DEFAULT_SIGNING_SECRET_ENV in captured.err
    assert captured.out == ""


def test_submit_byoc_preflight_report_requires_identity_args(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    report_path = _preflight_report(tmp_path / "preflight-report.json")
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(["--preflight-report", str(report_path), "--key-ref", SIGNING_KEY_REF])

    captured = capsys.readouterr()
    assert code == 2
    assert "--agent-id is required" in captured.err
    assert "--agent-version is required" in captured.err
    assert captured.out == ""


def test_submit_byoc_preflight_report_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["submission_request"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.preflight_report_submission.v1"
    )
