#!/usr/bin/env python3
"""Sign and optionally run a BYOC sanitized deployment overview read."""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_control_plane_intake import (
    signed_evidence_receipt_read_headers,
)
from services.platform.runtime.byoc_deployment_overview import (
    ByocDeploymentOverview,
    ByocDeploymentOverviewQuery,
)


DEFAULT_SIGNING_SECRET_ENV = "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY"
DEPLOYMENT_OVERVIEW_PATH = "/byoc/control-plane/deployment-overview"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-id", help="BYOC deployment id.")
    parser.add_argument("--customer-id", help="Optional BYOC customer id bound.")
    parser.add_argument(
        "--signing-secret-env",
        default=DEFAULT_SIGNING_SECRET_ENV,
        help="Environment variable containing local read signing-key material.",
    )
    parser.add_argument("--key-ref", help="Control-plane receipt/read key reference.")
    parser.add_argument("--nonce", help="Read nonce. Generated when omitted.")
    parser.add_argument(
        "--timestamp",
        type=str,
        help="ISO timestamp to use when signing the read.",
    )
    parser.add_argument(
        "--overview-url",
        help=(
            "Optional full URL for GET /byoc/control-plane/deployment-overview. "
            "Omit to print a signed GET request without network access."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the deployment overview query/response schema bundle and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.schema:
        print(json.dumps(_schema_bundle(), indent=2, sort_keys=True))
        return 0
    if not args.key_ref:
        _print_errors("BYOC deployment overview read failed", ["--key-ref is required"])
        return 2
    if args.timeout_seconds <= 0:
        _print_errors(
            "BYOC deployment overview read failed",
            ["--timeout-seconds must be positive"],
        )
        return 2
    signing_secret = os.environ.get(args.signing_secret_env, "")
    if not signing_secret.strip():
        _print_errors(
            "BYOC deployment overview read failed",
            [f"{args.signing_secret_env} must contain signing-key material"],
        )
        return 2

    try:
        query = ByocDeploymentOverviewQuery(
            deployment_id=args.deployment_id,
            customer_id=args.customer_id,
        )
        query_string = _query_string(query)
        headers = signed_evidence_receipt_read_headers(
            method="GET",
            path=DEPLOYMENT_OVERVIEW_PATH,
            query=query_string,
            signing_secret=signing_secret,
            key_ref=args.key_ref,
            nonce=args.nonce or _nonce(),
            timestamp=_parse_timestamp(args.timestamp),
        )
    except (ValidationError, TypeError, ValueError) as exc:
        _print_errors(
            "BYOC deployment overview read failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "BYOC deployment overview read failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    if not args.overview_url:
        print(
            json.dumps(
                {
                    "method": "GET",
                    "path": DEPLOYMENT_OVERVIEW_PATH,
                    "query": query_string,
                    "headers": headers,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    try:
        url = _url_with_query(args.overview_url, query_string)
        overview = _get_json(url, headers, timeout_seconds=args.timeout_seconds)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        _print_errors(
            "BYOC deployment overview read was rejected",
            [f"HTTP {exc.code}: {body}"],
        )
        return 1
    except urllib.error.URLError as exc:
        _print_errors(
            "BYOC deployment overview read was unreachable",
            [str(exc.reason)],
        )
        return 1
    except ValueError as exc:
        _print_errors("BYOC deployment overview read failed", [str(exc)])
        return 1

    print(json.dumps(overview, indent=2, sort_keys=True))
    return 0


def _schema_bundle() -> dict[str, Any]:
    return {
        "query": ByocDeploymentOverviewQuery.model_json_schema(),
        "overview": ByocDeploymentOverview.model_json_schema(),
    }


def _query_string(query: ByocDeploymentOverviewQuery) -> str:
    params: list[tuple[str, str]] = [("deployment_id", query.deployment_id)]
    if query.customer_id is not None:
        params.append(("customer_id", query.customer_id))
    return urllib.parse.urlencode(params)


def _url_with_query(url: str, query: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.query:
        raise ValueError("--overview-url must not include a query string")
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment)
    )


def _get_json(
    url: str,
    headers: dict[str, str],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            **headers,
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        response_body = response.read().decode("utf-8")
    parsed = json.loads(response_body)
    if not isinstance(parsed, dict):
        raise ValueError("deployment overview response must be a JSON object")
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
