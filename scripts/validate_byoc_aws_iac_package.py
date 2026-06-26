#!/usr/bin/env python3
"""Validate Fyralis BYOC AWS IaC package scaffolds."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_aws_iac_package import (
    ByocAwsIacPackage,
    byoc_aws_iac_package_json_schema,
    load_byoc_aws_iac_package,
    render_validation_errors,
    validate_aws_iac_package_contract,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_permissions import (
    load_byoc_aws_iam_template,
    load_byoc_permissions_manifest,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "package",
        nargs="?",
        type=Path,
        help="JSON or YAML BYOC AWS IaC package manifest to validate.",
    )
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        help="Optional BYOC data-plane manifest to verify deployment identity.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        help="Optional BYOC permissions manifest to verify IAM/tag requirements.",
    )
    parser.add_argument(
        "--iam-template",
        type=Path,
        help="Optional AWS IAM skeleton to verify package identity.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used to inspect Terraform scaffold files.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable validation results.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the AWS IaC package contract and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(argv or sys.argv[1:]))

    if args.schema:
        print(json.dumps(byoc_aws_iac_package_json_schema(), indent=2, sort_keys=True))
        return 0
    if args.package is None:
        print("package path is required unless --schema is used", file=sys.stderr)
        return 2

    schema_errors: list[str] = []
    contract_errors: list[str] = []
    package: ByocAwsIacPackage | None = None
    dataplane = None
    permissions = None
    iam_template = None

    try:
        package = load_byoc_aws_iac_package(args.package)
    except ValidationError as exc:
        schema_errors.extend(render_validation_errors(exc))
    except Exception as exc:  # noqa: BLE001
        schema_errors.append(f"{type(exc).__name__}: {exc}")

    if package is not None and args.dataplane_manifest is not None:
        try:
            dataplane = load_byoc_manifest(args.dataplane_manifest)
        except ValidationError as exc:
            schema_errors.extend(
                f"dataplane.{error}" for error in render_validation_errors(exc)
            )
        except Exception as exc:  # noqa: BLE001
            schema_errors.append(f"dataplane.{type(exc).__name__}: {exc}")

    if package is not None and args.permissions_manifest is not None:
        try:
            permissions = load_byoc_permissions_manifest(args.permissions_manifest)
        except ValidationError as exc:
            schema_errors.extend(
                f"permissions.{error}" for error in render_validation_errors(exc)
            )
        except Exception as exc:  # noqa: BLE001
            schema_errors.append(f"permissions.{type(exc).__name__}: {exc}")

    if package is not None and args.iam_template is not None:
        try:
            iam_template = load_byoc_aws_iam_template(args.iam_template)
        except ValidationError as exc:
            schema_errors.extend(
                f"iam_template.{error}" for error in render_validation_errors(exc)
            )
        except Exception as exc:  # noqa: BLE001
            schema_errors.append(f"iam_template.{type(exc).__name__}: {exc}")

    if schema_errors:
        return _render_result(
            args,
            valid=False,
            schema_errors=schema_errors,
            contract_errors=[],
            package=package,
        )

    assert package is not None
    contract_errors.extend(
        violation.render()
        for violation in validate_aws_iac_package_contract(
            package,
            dataplane_manifest=dataplane,
            permissions_manifest=permissions,
            iam_template=iam_template,
            repo_root=args.repo_root.resolve(),
        )
    )
    if contract_errors:
        return _render_result(
            args,
            valid=False,
            schema_errors=[],
            contract_errors=contract_errors,
            package=package,
        )

    return _render_result(
        args,
        valid=True,
        schema_errors=[],
        contract_errors=[],
        package=package,
    )


def _render_result(
    args: argparse.Namespace,
    *,
    valid: bool,
    schema_errors: list[str],
    contract_errors: list[str],
    package: ByocAwsIacPackage | None,
) -> int:
    if args.json:
        payload: dict[str, object] = {
            "valid": valid,
            "schema_errors": schema_errors,
            "contract_errors": contract_errors,
        }
        if package is not None:
            payload.update(
                {
                    "deployment_id": package.deployment_id,
                    "customer_id": package.customer_id,
                    "cloud_provider": package.cloud_provider,
                    "terraform_root_module": package.terraform.root_module_path,
                    "terraform_files": [
                        file.path for file in package.terraform.files
                    ],
                }
            )
        sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    elif not valid:
        title = (
            "BYOC AWS IaC package schema violations"
            if schema_errors
            else "BYOC AWS IaC package contract violations"
        )
        print(f"{title}:", file=sys.stderr)
        for error in schema_errors + contract_errors:
            print(f"  {error}", file=sys.stderr)
    else:
        print("BYOC AWS IaC package passed.")
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
