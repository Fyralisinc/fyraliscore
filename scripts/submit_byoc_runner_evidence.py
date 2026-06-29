#!/usr/bin/env python3
"""Sign and optionally submit sanitized BYOC runner evidence."""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_agent_runner import (
    ByocAgentRunnerApplyPlanEvidence,
    ByocAgentRunnerArtifactDigestEvidence,
    ByocAgentRunnerArtifactVerificationEvidence,
    ByocAgentRunnerCheck,
    ByocAgentRunnerIteration,
    ByocAgentRunnerReport,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    model_json_schema_bundle,
    runner_evidence_submission_payload,
    runner_evidence_summary_from_report,
    signed_runner_evidence_submission,
    validate_runner_evidence_submission,
)


DEFAULT_SIGNING_SECRET_ENV = "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runner-report",
        type=Path,
        help="Sanitized JSON/YAML report emitted by scripts/run_byoc_agent_runner.py.",
    )
    parser.add_argument(
        "--signing-secret-env",
        default=DEFAULT_SIGNING_SECRET_ENV,
        help="Environment variable containing local intake signing-key material.",
    )
    parser.add_argument(
        "--key-ref",
        help="Control-plane evidence intake signing key reference.",
    )
    parser.add_argument(
        "--nonce",
        help="Submission nonce. Generated when omitted.",
    )
    parser.add_argument(
        "--submitted-at",
        type=str,
        help="ISO timestamp to use when signing the submission.",
    )
    parser.add_argument(
        "--submit-url",
        help=(
            "Optional full URL for POST /byoc/control-plane/runner-evidence. "
            "Omit to print the signed submission without network access."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional file to write the signed submission JSON before submit.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the runner evidence intake schema bundle and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.schema:
        print(json.dumps(model_json_schema_bundle(), indent=2, sort_keys=True))
        return 0
    if args.runner_report is None:
        _print_errors("BYOC runner evidence submission failed", ["--runner-report is required"])
        return 2
    if not args.key_ref:
        _print_errors("BYOC runner evidence submission failed", ["--key-ref is required"])
        return 2
    if args.timeout_seconds <= 0:
        _print_errors(
            "BYOC runner evidence submission failed",
            ["--timeout-seconds must be positive"],
        )
        return 2
    signing_secret = os.environ.get(args.signing_secret_env, "")
    if not signing_secret.strip():
        _print_errors(
            "BYOC runner evidence submission failed",
            [f"{args.signing_secret_env} must contain signing-key material"],
        )
        return 2

    try:
        report = _load_runner_report(args.runner_report)
        summary = runner_evidence_summary_from_report(report)
        payload = runner_evidence_submission_payload(
            evidence=summary,
            nonce=args.nonce or _nonce(),
            submitted_at=_parse_submitted_at(args.submitted_at),
        )
        submission = signed_runner_evidence_submission(
            payload,
            signing_secret=signing_secret,
            key_ref=args.key_ref,
        )
        violations = validate_runner_evidence_submission(
            submission,
            signing_secret=signing_secret,
            expected_key_ref=args.key_ref,
        )
        if violations:
            _print_errors(
                "BYOC runner evidence submission contract violations",
                [violation.render() for violation in violations],
            )
            return 1
    except (ValidationError, TypeError, ValueError) as exc:
        _print_errors(
            "BYOC runner evidence submission failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "BYOC runner evidence submission failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    signed_json = json.dumps(
        submission.model_dump(mode="json"),
        indent=2,
        sort_keys=True,
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(signed_json + "\n", encoding="utf-8")

    if not args.submit_url:
        print(signed_json)
        return 0

    try:
        receipt = _post_json(
            args.submit_url,
            json.loads(signed_json),
            timeout_seconds=args.timeout_seconds,
        )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        _print_errors(
            "BYOC runner evidence intake rejected the submission",
            [f"HTTP {exc.code}: {body}"],
        )
        return 1
    except urllib.error.URLError as exc:
        _print_errors(
            "BYOC runner evidence intake was unreachable",
            [str(exc.reason)],
        )
        return 1

    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


def _load_runner_report(path: Path) -> ByocAgentRunnerReport:
    data = _load_mapping(path)
    checks = tuple(ByocAgentRunnerCheck(**item) for item in data.pop("checks"))
    iterations = tuple(
        ByocAgentRunnerIteration(**item) for item in data.pop("iterations")
    )
    apply_plans = tuple(
        ByocAgentRunnerApplyPlanEvidence(
            **{
                **item,
                "step_names": tuple(item.get("step_names", ())),
            }
        )
        for item in data.pop("apply_plans")
    )
    artifact_verifications = tuple(
        ByocAgentRunnerArtifactVerificationEvidence(
            **{
                **item,
                "required_artifact_roles": tuple(
                    item.get("required_artifact_roles", ())
                ),
                "artifacts": tuple(
                    ByocAgentRunnerArtifactDigestEvidence(**artifact)
                    for artifact in item.get("artifacts", ())
                ),
            }
        )
        for item in data.pop("artifact_verifications")
    )
    return ByocAgentRunnerReport(
        **data,
        checks=list(checks),
        iterations=list(iterations),
        apply_plans=list(apply_plans),
        artifact_verifications=list(artifact_verifications),
    )


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data = json.loads(raw)
    else:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
            raise RuntimeError("YAML input requires PyYAML") from exc
        data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError("runner report must be a JSON/YAML object")
    return dict(data)


def _post_json(
    url: str,
    payload: dict[str, Any],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        response_body = response.read().decode("utf-8")
    parsed = json.loads(response_body)
    if not isinstance(parsed, dict):
        raise ValueError("runner evidence intake response must be a JSON object")
    return parsed


def _parse_submitted_at(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _nonce() -> str:
    return "nonce-" + secrets.token_urlsafe(24)


def _print_errors(title: str, errors: list[str]) -> None:
    print(f"{title}:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
