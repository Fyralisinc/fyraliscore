#!/usr/bin/env python3
"""Validate Fyralis BYOC cloud/IAM permission contracts."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_permissions import (
    ByocAwsIamTemplateSkeleton,
    ByocPermissionsManifest,
    byoc_aws_iam_template_json_schema,
    byoc_permissions_json_schema,
    load_byoc_aws_iam_template,
    load_byoc_permissions_manifest,
    render_validation_errors,
    validate_aws_iam_template_contract,
    validate_permissions_manifest_contract,
)


def _render_json_result(
    *,
    valid: bool,
    schema_errors: list[str],
    contract_errors: list[str],
    manifest: ByocPermissionsManifest | None = None,
    aws_template: ByocAwsIamTemplateSkeleton | None = None,
) -> str:
    payload: dict[str, object] = {
        "valid": valid,
        "schema_errors": schema_errors,
        "contract_errors": contract_errors,
    }
    if manifest is not None:
        payload["deployment_id"] = manifest.deployment_id
        payload["customer_id"] = manifest.customer_id
        payload["cloud_provider"] = manifest.cloud_provider
        payload["roles"] = [role.name for role in manifest.roles]
    if aws_template is not None:
        payload["aws_template_roles"] = [role.name for role in aws_template.roles]
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest",
        nargs="?",
        type=Path,
        help="JSON or YAML BYOC permissions manifest to validate.",
    )
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        help="Optional BYOC data-plane manifest to verify deployment identity.",
    )
    parser.add_argument(
        "--aws-template",
        type=Path,
        help="Optional AWS IAM skeleton template to verify against the manifest.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable validation results.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the permissions manifest and exit.",
    )
    parser.add_argument(
        "--aws-template-schema",
        action="store_true",
        help="Print the JSON schema for the AWS IAM skeleton and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(argv or sys.argv[1:]))

    if args.schema:
        print(json.dumps(byoc_permissions_json_schema(), indent=2, sort_keys=True))
        return 0
    if args.aws_template_schema:
        print(
            json.dumps(
                byoc_aws_iam_template_json_schema(),
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.manifest is None:
        print("manifest path is required unless --schema is used", file=sys.stderr)
        return 2

    schema_errors: list[str] = []
    contract_errors: list[str] = []
    manifest: ByocPermissionsManifest | None = None
    aws_template: ByocAwsIamTemplateSkeleton | None = None

    try:
        manifest = load_byoc_permissions_manifest(args.manifest)
    except ValidationError as exc:
        schema_errors.extend(render_validation_errors(exc))
    except Exception as exc:  # noqa: BLE001
        schema_errors.append(f"{type(exc).__name__}: {exc}")

    dataplane_manifest = None
    if manifest is not None and args.dataplane_manifest is not None:
        try:
            dataplane_manifest = load_byoc_manifest(args.dataplane_manifest)
        except ValidationError as exc:
            schema_errors.extend(
                f"dataplane.{error}" for error in render_validation_errors(exc)
            )
        except Exception as exc:  # noqa: BLE001
            schema_errors.append(f"dataplane.{type(exc).__name__}: {exc}")

    if manifest is not None and args.aws_template is not None:
        try:
            aws_template = load_byoc_aws_iam_template(args.aws_template)
        except ValidationError as exc:
            schema_errors.extend(
                f"aws_template.{error}" for error in render_validation_errors(exc)
            )
        except Exception as exc:  # noqa: BLE001
            schema_errors.append(f"aws_template.{type(exc).__name__}: {exc}")

    if schema_errors:
        if args.json:
            sys.stdout.write(
                _render_json_result(
                    valid=False,
                    schema_errors=schema_errors,
                    contract_errors=[],
                    manifest=manifest,
                    aws_template=aws_template,
                )
            )
        else:
            print("BYOC permissions schema violations:", file=sys.stderr)
            for error in schema_errors:
                print(f"  {error}", file=sys.stderr)
        return 1

    assert manifest is not None
    contract_errors.extend(
        violation.render()
        for violation in validate_permissions_manifest_contract(
            manifest,
            dataplane_manifest=dataplane_manifest,
        )
    )
    if aws_template is not None:
        contract_errors.extend(
            violation.render()
            for violation in validate_aws_iam_template_contract(
                aws_template,
                permissions_manifest=manifest,
            )
        )

    if contract_errors:
        if args.json:
            sys.stdout.write(
                _render_json_result(
                    valid=False,
                    schema_errors=[],
                    contract_errors=contract_errors,
                    manifest=manifest,
                    aws_template=aws_template,
                )
            )
        else:
            print("BYOC permissions contract violations:", file=sys.stderr)
            for error in contract_errors:
                print(f"  {error}", file=sys.stderr)
        return 1

    if args.json:
        sys.stdout.write(
            _render_json_result(
                valid=True,
                schema_errors=[],
                contract_errors=[],
                manifest=manifest,
                aws_template=aws_template,
            )
        )
    else:
        print("BYOC permissions manifest passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
