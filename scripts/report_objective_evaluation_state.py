#!/usr/bin/env python3
"""Compile the continuous InvariantProofMatrix from explicit run evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.proof import (
    InvariantEvidenceManifest,
    aggregate_invariant_evidence_manifests,
    compile_invariant_proof_matrix,
    render_evidence_aggregation_markdown,
    render_invariant_proof_markdown,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "architecture" / "registry.yaml"


def _load_manifest(path: Path) -> InvariantEvidenceManifest:
    raw = yaml.safe_load(path.read_text())
    return InvariantEvidenceManifest.model_validate(raw)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument(
        "--evidence",
        type=Path,
        action="append",
        help=(
            "versioned YAML/JSON InvariantEvidenceManifest; repeat for compatible "
            "component manifests; omitted means no observed evidence"
        ),
    )
    parser.add_argument("--run-id", default="unobserved-current-state")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument(
        "--require-freeze-ready",
        action="store_true",
        help="return nonzero unless all 42 rows are substantiated without incidents",
    )
    args = parser.parse_args()

    registry = load_architecture_registry(args.registry)
    evidence = ()
    run_id = args.run_id
    bundle = None
    if args.evidence:
        manifests = tuple(_load_manifest(path) for path in args.evidence)
        incompatible = tuple(
            path
            for path, manifest in zip(args.evidence, manifests, strict=True)
            if manifest.architecture_digest != registry.digest
        )
        if incompatible:
            raise SystemExit(
                "evidence architecture_digest does not match the current registry; "
                "replay or explicitly migrate: "
                + ", ".join(str(path) for path in incompatible)
            )
        try:
            bundle = aggregate_invariant_evidence_manifests(manifests)
        except ValueError as exc:
            raise SystemExit(f"unsafe or incompatible evidence aggregation: {exc}") from exc
        evidence = bundle.evidence
        run_id = bundle.run_id

    report = compile_invariant_proof_matrix(
        registry,
        run_id=run_id,
        evidence=evidence,
    )
    if args.format == "json":
        payload = report.model_dump(mode="json")
        payload["production_freeze_ready"] = report.production_freeze_ready
        payload["evidence_aggregation"] = (
            bundle.model_dump(mode="json") if bundle else None
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        if bundle:
            print(render_evidence_aggregation_markdown(bundle))
            print()
        print(render_invariant_proof_markdown(report))
    return int(args.require_freeze_ready and not report.production_freeze_ready)


if __name__ == "__main__":
    raise SystemExit(main())
