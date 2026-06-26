#!/usr/bin/env python3
"""Sign and optionally submit a sanitized BYOC preflight report."""
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

from services.platform.runtime.byoc_preflight_bundle import (
    ByocPreflightBundleReport,
)
from services.platform.runtime.byoc_preflight_intake import (
    model_json_schema_bundle,
    preflight_report_submission_payload,
    signed_preflight_report_submission,
    validate_preflight_report_submission,
)


DEFAULT_SIGNING_SECRET_ENV = "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--preflight-report",
        type=Path,
        help="Sanitized JSON/YAML report emitted by scripts/run_byoc_preflight_bundle.py.",
    )
    parser.add_argument(
        "--agent-id",
        required=False,
        help="BYOC data-plane agent id submitting the preflight evidence.",
    )
    parser.add_argument(
        "--agent-version",
        required=False,
        help="BYOC data-plane agent or tooling version.",
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
            "Optional full URL for POST /byoc/control-plane/preflight-reports. "
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
        help="Print the preflight report intake schema bundle and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.schema:
        print(json.dumps(model_json_schema_bundle(), indent=2, sort_keys=True))
        return 0
    missing = [
        name
        for name, value in (
            ("--preflight-report", args.preflight_report),
            ("--agent-id", args.agent_id),
            ("--agent-version", args.agent_version),
            ("--key-ref", args.key_ref),
        )
        if not value
    ]
    if missing:
        _print_errors(
            "BYOC preflight report submission failed",
            [f"{name} is required" for name in missing],
        )
        return 2
    if args.timeout_seconds <= 0:
        _print_errors(
            "BYOC preflight report submission failed",
            ["--timeout-seconds must be positive"],
        )
        return 2
    signing_secret = os.environ.get(args.signing_secret_env, "")
    if not signing_secret.strip():
        _print_errors(
            "BYOC preflight report submission failed",
            [f"{args.signing_secret_env} must contain signing-key material"],
        )
        return 2

    try:
        report = _load_preflight_report(args.preflight_report)
        payload = preflight_report_submission_payload(
            preflight_report=report,
            agent_id=args.agent_id,
            agent_version=args.agent_version,
            nonce=args.nonce or _nonce(),
            submitted_at=_parse_submitted_at(args.submitted_at),
        )
        submission = signed_preflight_report_submission(
            payload,
            signing_secret=signing_secret,
            key_ref=args.key_ref,
        )
        violations = validate_preflight_report_submission(
            submission,
            signing_secret=signing_secret,
            expected_key_ref=args.key_ref,
        )
        if violations:
            _print_errors(
                "BYOC preflight report submission contract violations",
                [violation.render() for violation in violations],
            )
            return 1
    except (ValidationError, TypeError, ValueError) as exc:
        _print_errors(
            "BYOC preflight report submission failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "BYOC preflight report submission failed",
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
            "BYOC preflight report intake rejected the submission",
            [f"HTTP {exc.code}: {body}"],
        )
        return 1
    except urllib.error.URLError as exc:
        _print_errors(
            "BYOC preflight report intake was unreachable",
            [str(exc.reason)],
        )
        return 1

    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


def _load_preflight_report(path: Path) -> ByocPreflightBundleReport:
    data = _load_mapping(path)
    return ByocPreflightBundleReport.model_validate(data)


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
        raise ValueError("preflight report must be a JSON/YAML object")
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
        raise ValueError("preflight report intake response must be a JSON object")
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
