#!/usr/bin/env python3
"""Collect, sign, and optionally submit sanitized BYOC product health."""
from __future__ import annotations

import argparse
import asyncio
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

from services.platform.runtime.byoc_product_health import (
    model_json_schema_bundle,
    signed_product_health_snapshot,
    validate_product_health_snapshot_submission,
)
from services.platform.runtime.byoc_product_health_collector import (
    ByocProductHealthCollectorIdentity,
    collect_product_health_snapshot,
)


DEFAULT_DATABASE_URL_ENV = "DATABASE_URL"
DEFAULT_SIGNING_SECRET_ENV = "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-id", help="BYOC deployment id.")
    parser.add_argument("--customer-id", help="BYOC customer id.")
    parser.add_argument("--agent-id", help="BYOC data-plane agent id.")
    parser.add_argument("--agent-version", help="BYOC data-plane agent version.")
    parser.add_argument(
        "--artifact-revision",
        help="Deployed BYOC artifact revision or image tag.",
    )
    parser.add_argument(
        "--tenant-id",
        help=(
            "Optional Fyralis tenant UUID. Omit only for single-tenant/customer "
            "databases where all rows belong to the BYOC deployment."
        ),
    )
    parser.add_argument(
        "--database-url",
        help="Postgres DSN. Defaults to the environment variable named by --database-url-env.",
    )
    parser.add_argument(
        "--database-url-env",
        default=DEFAULT_DATABASE_URL_ENV,
        help="Environment variable containing the Postgres DSN.",
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
        help="Snapshot nonce. Generated when omitted.",
    )
    parser.add_argument(
        "--collected-at",
        type=str,
        help="ISO timestamp to use as the collection time.",
    )
    parser.add_argument(
        "--submit-url",
        help=(
            "Optional full URL for POST /byoc/control-plane/product-health-snapshots. "
            "Omit to print the signed snapshot without network access."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional file to write the JSON snapshot before submit.",
    )
    parser.add_argument(
        "--unsigned",
        action="store_true",
        help="Emit the unsigned snapshot payload for local inspection only.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the product-health schema bundle and exit.",
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
            ("--deployment-id", args.deployment_id),
            ("--customer-id", args.customer_id),
            ("--agent-id", args.agent_id),
            ("--agent-version", args.agent_version),
            ("--artifact-revision", args.artifact_revision),
        )
        if not value
    ]
    if not args.unsigned and not args.key_ref:
        missing.append("--key-ref")
    if missing:
        _print_errors(
            "BYOC product-health collection failed",
            [f"{name} is required" for name in missing],
        )
        return 2
    if args.timeout_seconds <= 0:
        _print_errors(
            "BYOC product-health collection failed",
            ["--timeout-seconds must be positive"],
        )
        return 2
    if args.unsigned and args.submit_url:
        _print_errors(
            "BYOC product-health collection failed",
            ["--unsigned cannot be used with --submit-url"],
        )
        return 2

    database_url = args.database_url or os.environ.get(args.database_url_env, "")
    if not database_url.strip():
        _print_errors(
            "BYOC product-health collection failed",
            [f"{args.database_url_env} must contain a Postgres DSN"],
        )
        return 2

    signing_secret = ""
    if not args.unsigned:
        signing_secret = os.environ.get(args.signing_secret_env, "")
        if not signing_secret.strip():
            _print_errors(
                "BYOC product-health collection failed",
                [f"{args.signing_secret_env} must contain signing-key material"],
            )
            return 2

    try:
        payload = asyncio.run(_collect_snapshot(args, database_url=database_url))
        if args.unsigned:
            output_json = json.dumps(
                payload.model_dump(mode="json"),
                indent=2,
                sort_keys=True,
            )
        else:
            submission = signed_product_health_snapshot(
                payload,
                signing_secret=signing_secret,
                key_ref=args.key_ref,
            )
            violations = validate_product_health_snapshot_submission(
                submission,
                signing_secret=signing_secret,
                expected_key_ref=args.key_ref,
            )
            if violations:
                _print_errors(
                    "BYOC product-health snapshot contract violations",
                    [violation.render() for violation in violations],
                )
                return 1
            output_json = json.dumps(
                submission.model_dump(mode="json"),
                indent=2,
                sort_keys=True,
            )
    except (ValidationError, TypeError, ValueError) as exc:
        _print_errors(
            "BYOC product-health collection failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1
    except ImportError as exc:
        _print_errors(
            "BYOC product-health collection failed",
            [f"missing optional dependency: {exc.name or 'asyncpg'}"],
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "BYOC product-health collection failed",
            [type(exc).__name__],
        )
        return 1

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output_json + "\n", encoding="utf-8")

    if not args.submit_url:
        print(output_json)
        return 0

    try:
        receipt = _post_json(
            args.submit_url,
            json.loads(output_json),
            timeout_seconds=args.timeout_seconds,
        )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        _print_errors(
            "BYOC product-health intake rejected the snapshot",
            [f"HTTP {exc.code}: {body}"],
        )
        return 1
    except urllib.error.URLError as exc:
        _print_errors(
            "BYOC product-health intake was unreachable",
            [str(exc.reason)],
        )
        return 1

    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


async def _collect_snapshot(
    args: argparse.Namespace,
    *,
    database_url: str,
):
    try:
        import asyncpg
    except ImportError:
        raise

    connection = await asyncpg.connect(dsn=database_url, statement_cache_size=0)
    try:
        return await collect_product_health_snapshot(
            connection,
            identity=ByocProductHealthCollectorIdentity(
                deployment_id=args.deployment_id,
                customer_id=args.customer_id,
                agent_id=args.agent_id,
                agent_version=args.agent_version,
                artifact_revision=args.artifact_revision,
                tenant_id=args.tenant_id,
            ),
            nonce=args.nonce or _nonce(),
            collected_at=_parse_timestamp(args.collected_at),
        )
    finally:
        await connection.close()


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
        raise ValueError("product-health intake response must be a JSON object")
    return parsed


def _parse_timestamp(raw: str | None) -> datetime | None:
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
