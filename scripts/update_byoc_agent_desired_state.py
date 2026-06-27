#!/usr/bin/env python3
"""Sign and optionally submit a BYOC agent desired-state update."""
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

from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentDesiredStateUpdatePayload,
    ByocAgentDesiredStateUpdateReceipt,
    ByocAgentDesiredStateUpdateRequest,
    desired_state_update_payload,
    signed_desired_state_update_request,
    validate_desired_state_update_request,
)


DEFAULT_SIGNING_SECRET_ENV = "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-id", help="BYOC deployment id.")
    parser.add_argument("--customer-id", help="BYOC customer id.")
    parser.add_argument("--agent-id", help="Enrolled BYOC data-plane agent id.")
    parser.add_argument("--desired-revision", help="Desired artifact/config revision.")
    parser.add_argument("--config-epoch", type=int, help="Monotonic config epoch.")
    parser.add_argument(
        "--evidence-package-required",
        action="store_true",
        help="Require the agent to produce a sanitized evidence package.",
    )
    parser.add_argument(
        "--reason-code",
        help="Bounded operator/automation reason code for the update.",
    )
    parser.add_argument(
        "--requested-by",
        help="Bounded operator/automation id requesting the update.",
    )
    parser.add_argument(
        "--signing-secret-env",
        default=DEFAULT_SIGNING_SECRET_ENV,
        help="Environment variable containing local intake signing-key material.",
    )
    parser.add_argument(
        "--key-ref",
        help="Control-plane intake signing key reference.",
    )
    parser.add_argument("--nonce", help="Update nonce. Generated when omitted.")
    parser.add_argument(
        "--requested-at",
        type=str,
        help="ISO timestamp to use when signing the update.",
    )
    parser.add_argument(
        "--submit-url",
        help=(
            "Optional full URL for POST /byoc/control-plane/agent-desired-state. "
            "Omit to print the signed update without network access."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional file to write the signed update JSON before submit.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the desired-state update schema bundle and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.schema:
        print(json.dumps(_schema_bundle(), indent=2, sort_keys=True))
        return 0

    missing = [
        name
        for name, value in (
            ("--deployment-id", args.deployment_id),
            ("--customer-id", args.customer_id),
            ("--agent-id", args.agent_id),
            ("--desired-revision", args.desired_revision),
            ("--config-epoch", args.config_epoch),
            ("--reason-code", args.reason_code),
            ("--requested-by", args.requested_by),
            ("--key-ref", args.key_ref),
        )
        if value is None or value == ""
    ]
    if missing:
        _print_errors(
            "BYOC agent desired-state update failed",
            [f"{name} is required" for name in missing],
        )
        return 2
    if args.timeout_seconds <= 0:
        _print_errors(
            "BYOC agent desired-state update failed",
            ["--timeout-seconds must be positive"],
        )
        return 2
    signing_secret = os.environ.get(args.signing_secret_env, "")
    if not signing_secret.strip():
        _print_errors(
            "BYOC agent desired-state update failed",
            [f"{args.signing_secret_env} must contain signing-key material"],
        )
        return 2

    try:
        payload = desired_state_update_payload(
            deployment_id=args.deployment_id,
            customer_id=args.customer_id,
            agent_id=args.agent_id,
            desired_revision=args.desired_revision,
            config_epoch=args.config_epoch,
            evidence_package_required=args.evidence_package_required,
            reason_code=args.reason_code,
            requested_by=args.requested_by,
            nonce=args.nonce or _nonce(),
            requested_at=_parse_requested_at(args.requested_at),
        )
        update = signed_desired_state_update_request(
            payload,
            signing_secret=signing_secret,
            key_ref=args.key_ref,
        )
        violations = validate_desired_state_update_request(
            update,
            signing_secret=signing_secret,
            expected_key_ref=args.key_ref,
        )
        if violations:
            _print_errors(
                "BYOC agent desired-state update contract violations",
                [violation.render() for violation in violations],
            )
            return 1
    except (ValidationError, TypeError, ValueError) as exc:
        _print_errors(
            "BYOC agent desired-state update failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "BYOC agent desired-state update failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    signed_json = json.dumps(update.model_dump(mode="json"), indent=2, sort_keys=True)
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
            "BYOC agent desired-state intake rejected the update",
            [f"HTTP {exc.code}: {body}"],
        )
        return 1
    except urllib.error.URLError as exc:
        _print_errors(
            "BYOC agent desired-state intake was unreachable",
            [str(exc.reason)],
        )
        return 1

    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


def _schema_bundle() -> dict[str, Any]:
    return {
        "payload": ByocAgentDesiredStateUpdatePayload.model_json_schema(),
        "request": ByocAgentDesiredStateUpdateRequest.model_json_schema(),
        "receipt": ByocAgentDesiredStateUpdateReceipt.model_json_schema(),
    }


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
        raise ValueError("desired-state intake response must be a JSON object")
    return parsed


def _parse_requested_at(raw: str | None) -> datetime | None:
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
