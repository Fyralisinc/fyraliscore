#!/usr/bin/env python3
"""Validate a Fyralis BYOC data-plane deployment manifest."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    byoc_manifest_json_schema,
    effective_runtime_processes,
    load_byoc_manifest,
    render_validation_errors,
    validate_byoc_manifest_contract,
)


def _render_json_result(
    *,
    valid: bool,
    schema_errors: list[str],
    contract_errors: list[str],
    manifest: ByocDataPlaneManifest | None = None,
) -> str:
    payload: dict[str, object] = {
        "valid": valid,
        "schema_errors": schema_errors,
        "contract_errors": contract_errors,
    }
    if manifest is not None:
        payload["deployment_id"] = manifest.deployment_id
        payload["customer_id"] = manifest.customer_id
        payload["effective_runtime_processes"] = [
            process.name for process in effective_runtime_processes(manifest)
        ]
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest",
        nargs="?",
        type=Path,
        help="JSON or YAML BYOC data-plane manifest to validate.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable validation results.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the manifest contract and exit.",
    )
    parser.add_argument(
        "--effective-processes",
        action="store_true",
        help="Print the production runtime processes enabled by the manifest.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(argv or sys.argv[1:]))

    if args.schema:
        print(json.dumps(byoc_manifest_json_schema(), indent=2, sort_keys=True))
        return 0

    if args.manifest is None:
        print("manifest path is required unless --schema is used", file=sys.stderr)
        return 2

    try:
        manifest = load_byoc_manifest(args.manifest)
    except ValidationError as exc:
        schema_errors = render_validation_errors(exc)
        if args.json:
            sys.stdout.write(
                _render_json_result(
                    valid=False,
                    schema_errors=schema_errors,
                    contract_errors=[],
                )
            )
        else:
            print("BYOC data-plane manifest schema violations:", file=sys.stderr)
            for error in schema_errors:
                print(f"  {error}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        message = f"{type(exc).__name__}: {exc}"
        if args.json:
            sys.stdout.write(
                _render_json_result(
                    valid=False,
                    schema_errors=[message],
                    contract_errors=[],
                )
            )
        else:
            print(f"Failed to load BYOC data-plane manifest: {message}", file=sys.stderr)
        return 1

    violations = validate_byoc_manifest_contract(manifest)
    contract_errors = [violation.render() for violation in violations]
    if violations:
        if args.json:
            sys.stdout.write(
                _render_json_result(
                    valid=False,
                    schema_errors=[],
                    contract_errors=contract_errors,
                    manifest=manifest,
                )
            )
        else:
            print("BYOC data-plane manifest contract violations:", file=sys.stderr)
            for error in contract_errors:
                print(f"  {error}", file=sys.stderr)
        return 1

    if args.effective_processes:
        for process in effective_runtime_processes(manifest):
            print(process.name)
        return 0

    if args.json:
        sys.stdout.write(
            _render_json_result(
                valid=True,
                schema_errors=[],
                contract_errors=[],
                manifest=manifest,
            )
        )
    else:
        print("BYOC data-plane manifest passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
