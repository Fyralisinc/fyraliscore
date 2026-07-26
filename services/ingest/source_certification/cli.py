"""Command-line release gate for the 27-source certification bundle."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.evaluator import (
    release_manifest,
    sign_manifest,
)
from services.ingest.source_certification.io import load_certification_input
from services.ingest.source_certification.models import (
    CertificationInput,
    CertificationInvariantError,
)
from services.ingest.source_certification.promotion import (
    PromotionManifestError,
    validate_commit_sha,
    validate_promotion_manifest_file,
)
from services.ingest.source_contract.catalog import source_definition


_REPO_ROOT = Path(__file__).resolve().parents[3]


def _inventory() -> dict[str, object]:
    sources = []
    for source_id, spec in SOURCE_CERTIFICATION_CATALOG.items():
        source = source_definition(source_id)
        missing = [
            item.behavior_id for item in spec.evidence if not item.verified
        ]
        surface = next(
            (
                item
                for item in spec.evidence
                if item.behavior_id == "used_api_surface"
            ),
            None,
        )
        sources.append(
            {
                "source_id": source_id,
                "spec_version": spec.spec_version,
                "spec_hash": spec.declaration_hash(),
                "provider_api_version": spec.provider_api_version,
                "evidence_pack_id": spec.evidence_pack_id,
                "evidence_pack_version": spec.evidence_pack_version,
                "evidence_pack_sha256": spec.evidence_pack_sha256,
                "required_scenarios": list(spec.required_scenarios),
                "required_canary_operations": list(
                    spec.canary.required_operations
                ),
                "evidence_verified": not missing,
                "unverified_evidence": missing,
                "used_surface_schema_sha256": (
                    surface.schema_sha256 if surface else None
                ),
                "provider_transport_enforced": (
                    source.provider_transport_enforced
                ),
                "operation_policy_ids": list(source.operation_policy_ids),
            }
        )
    verified = sum(
        bool(item["evidence_verified"])
        and item["used_surface_schema_sha256"] is not None
        for item in sources
    )
    transport_enforced = sum(
        bool(item["provider_transport_enforced"]) for item in sources
    )
    return {
        "state": (
            "ready"
            if verified == len(sources) and transport_enforced == len(sources)
            else "blocked"
        ),
        "required_sources": len(sources),
        "evidence_ready_sources": verified,
        "provider_transport_enforced_sources": transport_enforced,
        "sources": sources,
    }


def _strict_ratchet_clean() -> bool:
    result = subprocess.run(
        [
            sys.executable,
            str(_REPO_ROOT / "scripts/check_source_architecture_ratchet.py"),
            "--no-baseline",
        ],
        cwd=_REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _load_inputs(input_dir: Path) -> dict[str, CertificationInput]:
    inputs: dict[str, CertificationInput] = {}
    for source_id in SOURCE_CERTIFICATION_CATALOG:
        path = input_dir / f"{source_id}.json"
        if path.is_file():
            inputs[source_id] = load_certification_input(path)
    return inputs


def _write_json(value: object, output: str) -> None:
    rendered = json.dumps(value, sort_keys=True, indent=2, default=str) + "\n"
    if output == "-":
        sys.stdout.write(rendered)
        return
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(rendered, encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed Fyralis source certification gate",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser(
        "inventory",
        help="print the declared 27-source evidence inventory",
    )
    inventory.add_argument("--output", default="-")
    inventory.add_argument(
        "--require-ready",
        action="store_true",
        help="return non-zero until all evidence packs are verified and pinned",
    )

    manifest = subparsers.add_parser(
        "manifest",
        help="evaluate <source>.json artifacts and the strict legacy ratchet",
    )
    manifest.add_argument("--input-dir", required=True, type=Path)
    manifest.add_argument("--output", default="-")
    manifest.add_argument(
        "--signing-key-env",
        help="optional environment variable containing the manifest HMAC key",
    )
    manifest.add_argument(
        "--commit-sha",
        help=(
            "full commit SHA certified by this manifest; required for promotion"
        ),
    )

    verify = subparsers.add_parser(
        "verify-manifest",
        help="validate a signed manifest for one exact production target SHA",
    )
    verify.add_argument("--manifest", required=True, type=Path)
    verify.add_argument("--target-sha", required=True)
    verify.add_argument("--signing-key-env", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inventory":
            result = _inventory()
            _write_json(result, args.output)
            return int(args.require_ready and result["state"] != "ready")

        if args.command == "verify-manifest":
            value = os.environ.get(args.signing_key_env)
            if not value:
                raise CertificationInvariantError(
                    f"signing key environment variable "
                    f"{args.signing_key_env!r} is empty or unset"
                )
            result = validate_promotion_manifest_file(
                args.manifest,
                target_sha=args.target_sha,
                signing_key=value.encode("utf-8"),
            )
            _write_json(result, "-")
            return 0

        inputs = _load_inputs(args.input_dir)
        manifest = release_manifest(
            inputs,
            legacy_ratchet_clean=_strict_ratchet_clean(),
        )
        if args.commit_sha:
            manifest["commit_sha"] = validate_commit_sha(args.commit_sha)
        if args.signing_key_env:
            value = os.environ.get(args.signing_key_env)
            if not value:
                raise CertificationInvariantError(
                    f"signing key environment variable "
                    f"{args.signing_key_env!r} is empty or unset"
                )
            manifest["signature"] = sign_manifest(
                manifest,
                value.encode("utf-8"),
            )
        _write_json(manifest, args.output)
        return int(manifest["state"] != "passed")
    except (CertificationInvariantError, PromotionManifestError) as exc:
        print(f"source certification input error: {exc}", file=sys.stderr)
        return 2


__all__ = ["main"]
