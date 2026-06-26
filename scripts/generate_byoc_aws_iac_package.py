#!/usr/bin/env python3
"""Generate or check a Fyralis BYOC AWS IaC scaffold package."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_aws_iac_package import (
    ByocAwsIacPackage,
    ByocAwsIacPackageViolation,
    byoc_aws_iac_package_json_schema,
    generate_aws_iac_package,
    load_byoc_aws_iac_package,
    render_terraform_scaffold,
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
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest to consume.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=Path("deploy/byoc/permissions.example.yaml"),
        help="BYOC permissions manifest to consume.",
    )
    parser.add_argument(
        "--iam-template",
        type=Path,
        default=Path("deploy/byoc/aws/iam.bootstrap.template.yaml"),
        help="AWS IAM skeleton template to consume.",
    )
    parser.add_argument(
        "--bootstrap-bundle",
        type=Path,
        default=Path("deploy/byoc/bootstrap-bundle.example.yaml"),
        help="BYOC bootstrap bundle path referenced by the scaffold package.",
    )
    parser.add_argument(
        "--terraform-root",
        type=Path,
        default=Path("deploy/byoc/aws/terraform"),
        help="Terraform scaffold root module path to render.",
    )
    parser.add_argument(
        "--check-package",
        type=Path,
        help="Existing AWS IaC package manifest to validate and compare.",
    )
    parser.add_argument(
        "--package-output",
        type=Path,
        default=Path("deploy/byoc/aws/iac-package.example.yaml"),
        help="Package manifest path to write when --write is supplied.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used to inspect or write Terraform scaffold files.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the package manifest and Terraform scaffold files.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the AWS IaC package contract and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))

    if args.schema:
        print(json.dumps(byoc_aws_iac_package_json_schema(), indent=2, sort_keys=True))
        return 0

    try:
        dataplane = load_byoc_manifest(args.dataplane_manifest)
        permissions = load_byoc_permissions_manifest(args.permissions_manifest)
        iam_template = load_byoc_aws_iam_template(args.iam_template)
        generated = generate_aws_iac_package(
            dataplane_manifest=dataplane,
            permissions_manifest=permissions,
            iam_template=iam_template,
            source_paths={
                "dataplane_manifest": args.dataplane_manifest,
                "permissions_manifest": args.permissions_manifest,
                "iam_skeleton": args.iam_template,
                "bootstrap_bundle": args.bootstrap_bundle,
            },
            terraform_root_path=args.terraform_root,
        )
        rendered_terraform = render_terraform_scaffold(
            generated,
            iam_template=iam_template,
        )
    except ValidationError as exc:
        _print_errors(
            "BYOC AWS IaC package schema violations",
            render_validation_errors(exc),
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "Failed to build BYOC AWS IaC package",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    if args.check_package is not None:
        return _check_existing_package(
            existing_path=args.check_package,
            generated=generated,
            rendered_terraform=rendered_terraform,
            dataplane=dataplane,
            permissions=permissions,
            iam_template=iam_template,
            repo_root=args.repo_root.resolve(),
        )

    if args.write:
        _write_package_and_terraform(
            package_path=args.package_output,
            package=generated,
            rendered_terraform=rendered_terraform,
            repo_root=args.repo_root.resolve(),
        )
        print("BYOC AWS IaC package written.")
        return 0

    payload = generated.model_dump(mode="json", exclude_none=True)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
            _print_errors("Failed to render YAML", [str(exc)])
            return 1
        sys.stdout.write(yaml.safe_dump(payload, sort_keys=False, width=1_000_000))
    return 0


def _check_existing_package(
    *,
    existing_path: Path,
    generated: ByocAwsIacPackage,
    rendered_terraform: dict[str, str],
    dataplane,
    permissions,
    iam_template,
    repo_root: Path,
) -> int:
    existing = load_byoc_aws_iac_package(existing_path)
    violations = validate_aws_iac_package_contract(
        existing,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        iam_template=iam_template,
        repo_root=repo_root,
    )
    if existing.model_dump(mode="json") != generated.model_dump(mode="json"):
        violations.append(
            ByocAwsIacPackageViolation(
                path="<root>",
                code="generated_iac_package_drift",
                message="checked-in AWS IaC package does not match generated output",
            )
        )
    for rel_path, expected in rendered_terraform.items():
        path = repo_root / rel_path
        if not path.exists():
            violations.append(
                ByocAwsIacPackageViolation(
                    path=rel_path,
                    code="generated_terraform_file_missing",
                    message="generated Terraform scaffold file is missing",
                )
            )
            continue
        actual = path.read_text(encoding="utf-8")
        if actual != expected:
            violations.append(
                ByocAwsIacPackageViolation(
                    path=rel_path,
                    code="generated_terraform_file_drift",
                    message="checked-in Terraform scaffold does not match generated output",
                )
            )
    if violations:
        _print_errors(
            "BYOC AWS IaC package contract violations",
            [violation.render() for violation in violations],
        )
        return 1
    print("BYOC AWS IaC package passed.")
    return 0


def _write_package_and_terraform(
    *,
    package_path: Path,
    package: ByocAwsIacPackage,
    rendered_terraform: dict[str, str],
    repo_root: Path,
) -> None:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    package_path.parent.mkdir(parents=True, exist_ok=True)
    package_path.write_text(
        yaml.safe_dump(
            package.model_dump(mode="json", exclude_none=True),
            sort_keys=False,
            width=1_000_000,
        ),
        encoding="utf-8",
    )
    for rel_path, content in rendered_terraform.items():
        path = repo_root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _print_errors(title: str, errors: list[str]) -> None:
    print(f"{title}:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
