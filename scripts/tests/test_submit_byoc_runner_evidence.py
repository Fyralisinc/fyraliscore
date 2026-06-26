from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from scripts.submit_byoc_runner_evidence import (
    DEFAULT_SIGNING_SECRET_ENV,
    main,
)
from services.platform.runtime.byoc_agent_runner import (
    ByocAgentRunnerInputs,
    render_agent_runner_report_json,
    run_byoc_agent_runner,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    ByocRunnerEvidenceSubmissionRequest,
    validate_runner_evidence_submission,
)


ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = ROOT / "deploy/byoc/dataplane.example.yaml"
BUNDLE_NEXT_PATH = ROOT / "deploy/byoc/bootstrap-bundle.next.example.yaml"
INSTALL_TOKEN = "local-install-token-for-runner-submit-tests"
SIGNING_SECRET = "local-control-plane-runner-evidence-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"


def _runner_report(path: Path) -> Path:
    report = asyncio.run(
        run_byoc_agent_runner(
            ByocAgentRunnerInputs(
                manifest_path=MANIFEST_PATH,
                install_token=INSTALL_TOKEN,
                agent_id="agt_runnersubmit01",
                agent_version="2026.06.26-submit",
                nonce_prefix="nonce-runner-submit",
                iterations=1,
                mock_desired_revision="2026.06.26-2",
                mock_config_epoch=3,
                bootstrap_bundle_path=BUNDLE_NEXT_PATH,
                verify_local_bundle_files=True,
                repo_root=ROOT,
                requested_at=datetime(2026, 6, 26, 12, 0, tzinfo=UTC),
                sent_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
            )
        )
    )
    path.write_text(render_agent_runner_report_json(report), encoding="utf-8")
    return path


def test_submit_byoc_runner_evidence_prints_signed_submission(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    report_path = _runner_report(tmp_path / "runner-report.json")
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(
        [
            "--runner-report",
            str(report_path),
            "--key-ref",
            SIGNING_KEY_REF,
            "--nonce",
            "nonce-runner-submit-test-001",
            "--submitted-at",
            "2026-06-26T12:45:00+00:00",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    request = ByocRunnerEvidenceSubmissionRequest.model_validate(payload)
    assert code == 0
    assert request.schema_version == "fyralis.byoc.runner_evidence_submission.v1"
    assert request.evidence.schema_version == "fyralis.byoc.runner_evidence_summary.v1"
    assert request.evidence.apply_plan_count == 1
    assert request.evidence.artifact_verification_count == 1
    assert validate_runner_evidence_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []
    assert SIGNING_SECRET not in rendered
    assert INSTALL_TOKEN not in rendered
    assert '"checks":' not in rendered
    assert '"iterations":' not in rendered
    assert "gateway_image" not in rendered
    assert captured.err == ""


def test_submit_byoc_runner_evidence_writes_output_file(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    report_path = _runner_report(tmp_path / "runner-report.json")
    output_path = tmp_path / "out" / "runner-submission.json"
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(
        [
            "--runner-report",
            str(report_path),
            "--key-ref",
            SIGNING_KEY_REF,
            "--nonce",
            "nonce-runner-submit-test-002",
            "--output",
            str(output_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output_path.read_text(encoding="utf-8")) == json.loads(
        captured.out
    )


def test_submit_byoc_runner_evidence_posts_signed_submission(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    report_path = _runner_report(tmp_path / "runner-report.json")
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
                    "schema_version": "fyralis.byoc.runner_evidence_receipt.v1",
                    "status": "accepted",
                    "receipt_id": "runev_0123456789abcdef0123456789abcdef",
                    "stored_scope": "sanitized_metadata_only",
                }
            ).encode("utf-8")

    def _fake_urlopen(request, *, timeout):
        posted["url"] = request.full_url
        posted["timeout"] = timeout
        posted["body"] = json.loads(request.data.decode("utf-8"))
        return _Response()

    monkeypatch.setattr(
        "scripts.submit_byoc_runner_evidence.urllib.request.urlopen",
        _fake_urlopen,
    )

    code = main(
        [
            "--runner-report",
            str(report_path),
            "--key-ref",
            SIGNING_KEY_REF,
            "--nonce",
            "nonce-runner-submit-test-003",
            "--submit-url",
            "https://control.example.com/byoc/control-plane/runner-evidence",
            "--timeout-seconds",
            "3",
        ]
    )

    captured = capsys.readouterr()
    receipt = json.loads(captured.out)
    posted_body = json.dumps(posted["body"], sort_keys=True)
    assert code == 0
    assert posted["url"] == (
        "https://control.example.com/byoc/control-plane/runner-evidence"
    )
    assert posted["timeout"] == 3.0
    assert receipt["receipt_id"] == "runev_0123456789abcdef0123456789abcdef"
    assert '"checks":' not in posted_body
    assert '"iterations":' not in posted_body
    assert "gateway_image" not in posted_body
    assert INSTALL_TOKEN not in posted_body


def test_submit_byoc_runner_evidence_requires_signing_secret(
    tmp_path: Path,
    capsys,
) -> None:
    report_path = _runner_report(tmp_path / "runner-report.json")

    code = main(
        [
            "--runner-report",
            str(report_path),
            "--key-ref",
            SIGNING_KEY_REF,
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert DEFAULT_SIGNING_SECRET_ENV in captured.err
    assert captured.out == ""


def test_submit_byoc_runner_evidence_requires_key_ref(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    report_path = _runner_report(tmp_path / "runner-report.json")
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(["--runner-report", str(report_path)])

    captured = capsys.readouterr()
    assert code == 2
    assert "--key-ref is required" in captured.err
    assert captured.out == ""


def test_submit_byoc_runner_evidence_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["summary"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.runner_evidence_summary.v1"
    )
