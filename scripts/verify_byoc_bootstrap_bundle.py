#!/usr/bin/env python3
"""Verify a Fyralis BYOC bootstrap bundle contract."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_bootstrap_bundle import (
    ByocBootstrapBundleManifest,
    byoc_bootstrap_bundle_json_schema,
    load_byoc_bootstrap_bundle,
    render_validation_errors,
    validate_bootstrap_bundle_contract,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


def _render_json_result(
    *,
    valid: bool,
    schema_errors: list[str],
    contract_errors: list[str],
    bundle: ByocBootstrapBundleManifest | None = None,
) -> str:
    payload: dict[str, object] = {
        "valid": valid,
        "schema_errors": schema_errors,
        "contract_errors": contract_errors,
    }
    if bundle is not None:
        payload["deployment_id"] = bundle.deployment_id
        payload["customer_id"] = bundle.customer_id
        payload["artifact_revision"] = bundle.artifact_revision
        payload["artifacts"] = [artifact.role for artifact in bundle.artifacts]
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bundle",
        nargs="?",
        type=Path,
        help="JSON or YAML BYOC bootstrap bundle manifest to verify.",
    )
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        help="Optional BYOC data-plane manifest to verify deployment identity.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        help="Optional BYOC permissions manifest to verify deployment identity.",
    )
    parser.add_argument(
        "--verify-local-files",
        action="store_true",
        help="Hash any local_path artifacts and compare them to declared digests.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used for --verify-local-files.",
    )
    parser.add_argument(
        "--print-cosign-commands",
        action="store_true",
        help="Print offline cosign verify-blob commands for bundle artifacts.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable verification results.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the bundle contract and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(argv or sys.argv[1:]))

    if args.schema:
        print(
            json.dumps(
                byoc_bootstrap_bundle_json_schema(),
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.bundle is None:
        print("bundle path is required unless --schema is used", file=sys.stderr)
        return 2

    schema_errors: list[str] = []
    bundle: ByocBootstrapBundleManifest | None = None
    try:
        bundle = load_byoc_bootstrap_bundle(args.bundle)
    except ValidationError as exc:
        schema_errors.extend(render_validation_errors(exc))
    except Exception as exc:  # noqa: BLE001
        schema_errors.append(f"{type(exc).__name__}: {exc}")

    dataplane_manifest = None
    if bundle is not None and args.dataplane_manifest is not None:
        try:
            dataplane_manifest = load_byoc_manifest(args.dataplane_manifest)
        except ValidationError as exc:
            schema_errors.extend(
                f"dataplane.{error}" for error in render_validation_errors(exc)
            )
        except Exception as exc:  # noqa: BLE001
            schema_errors.append(f"dataplane.{type(exc).__name__}: {exc}")

    permissions_manifest = None
    if bundle is not None and args.permissions_manifest is not None:
        try:
            permissions_manifest = load_byoc_permissions_manifest(
                args.permissions_manifest
            )
        except ValidationError as exc:
            schema_errors.extend(
                f"permissions.{error}" for error in render_validation_errors(exc)
            )
        except Exception as exc:  # noqa: BLE001
            schema_errors.append(f"permissions.{type(exc).__name__}: {exc}")

    if schema_errors:
        if args.json:
            sys.stdout.write(
                _render_json_result(
                    valid=False,
                    schema_errors=schema_errors,
                    contract_errors=[],
                    bundle=bundle,
                )
            )
        else:
            print("BYOC bootstrap bundle schema violations:", file=sys.stderr)
            for error in schema_errors:
                print(f"  {error}", file=sys.stderr)
        return 1

    assert bundle is not None
    contract_errors = [
        violation.render()
        for violation in validate_bootstrap_bundle_contract(
            bundle,
            dataplane_manifest=dataplane_manifest,
            permissions_manifest=permissions_manifest,
            verify_local_files=args.verify_local_files,
            repo_root=args.repo_root.resolve(),
        )
    ]
    if contract_errors:
        if args.json:
            sys.stdout.write(
                _render_json_result(
                    valid=False,
                    schema_errors=[],
                    contract_errors=contract_errors,
                    bundle=bundle,
                )
            )
        else:
            print("BYOC bootstrap bundle contract violations:", file=sys.stderr)
            for error in contract_errors:
                print(f"  {error}", file=sys.stderr)
        return 1

    if args.json:
        sys.stdout.write(
            _render_json_result(
                valid=True,
                schema_errors=[],
                contract_errors=[],
                bundle=bundle,
            )
        )
    elif args.print_cosign_commands:
        for command in _cosign_commands(bundle):
            print(command)
    else:
        print("BYOC bootstrap bundle passed.")
    return 0


def _cosign_commands(bundle: ByocBootstrapBundleManifest) -> list[str]:
    commands: list[str] = []
    for artifact in bundle.artifacts:
        identity = (
            f"--certificate-identity {artifact.signature.certificate_identity!r} "
            f"--certificate-oidc-issuer {artifact.signature.oidc_issuer!r} "
            f"--bundle {artifact.signature.bundle_ref!r}"
        )
        if artifact.local_path is not None:
            commands.append(
                f"cosign verify-blob {identity} {artifact.local_path!r}"
            )
        else:
            commands.append(f"cosign verify {identity} {artifact.ref!r}")
    return commands


if __name__ == "__main__":
    raise SystemExit(main())
