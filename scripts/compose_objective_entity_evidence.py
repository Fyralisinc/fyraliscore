#!/usr/bin/env python3
"""Compose SHA-bound entity evidence for large-company evaluation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.entity_evidence_composer import (  # noqa: E402
    compose_objective_entity_evidence,
    load_bound_json,
    write_atomic_json,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v3-report", type=Path, required=True)
    parser.add_argument("--v3-sha256", required=True)
    parser.add_argument("--company-physics", type=Path, required=True)
    parser.add_argument("--company-physics-sha256", required=True)
    parser.add_argument("--company-physics-adversarial", type=Path, required=True)
    parser.add_argument("--company-physics-adversarial-sha256", required=True)
    parser.add_argument("--boundary-type", type=Path)
    parser.add_argument("--boundary-type-sha256")
    parser.add_argument("--boundary-type-closure", type=Path)
    parser.add_argument("--boundary-type-closure-sha256")
    parser.add_argument("--broad-extraction", type=Path)
    parser.add_argument("--broad-extraction-sha256")
    parser.add_argument("--broad-extraction-receipt", type=Path)
    parser.add_argument("--broad-extraction-receipt-sha256")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    v3 = load_bound_json(args.v3_report, expected_sha256=args.v3_sha256)
    vertical = load_bound_json(
        args.company_physics, expected_sha256=args.company_physics_sha256
    )
    adversarial = load_bound_json(
        args.company_physics_adversarial,
        expected_sha256=args.company_physics_adversarial_sha256,
    )
    boundary_type = (
        load_bound_json(args.boundary_type, expected_sha256=args.boundary_type_sha256)
        if args.boundary_type and args.boundary_type_sha256 else None
    )
    if bool(args.boundary_type) != bool(args.boundary_type_sha256):
        raise SystemExit("boundary type path and SHA must be supplied together")
    closure = (
        load_bound_json(args.boundary_type_closure,
            expected_sha256=args.boundary_type_closure_sha256)
        if args.boundary_type_closure and args.boundary_type_closure_sha256 else None
    )
    if bool(args.boundary_type_closure) != bool(args.boundary_type_closure_sha256):
        raise SystemExit("boundary type closure path and SHA must be supplied together")
    broad = (
        load_bound_json(args.broad_extraction,
            expected_sha256=args.broad_extraction_sha256)
        if args.broad_extraction and args.broad_extraction_sha256 else None
    )
    broad_receipt = (
        load_bound_json(args.broad_extraction_receipt,
            expected_sha256=args.broad_extraction_receipt_sha256)
        if args.broad_extraction_receipt and args.broad_extraction_receipt_sha256
        else None
    )
    if not all((args.broad_extraction, args.broad_extraction_sha256,
                args.broad_extraction_receipt,
                args.broad_extraction_receipt_sha256)) and any((
                    args.broad_extraction, args.broad_extraction_sha256,
                    args.broad_extraction_receipt,
                    args.broad_extraction_receipt_sha256)):
        raise SystemExit("broad extraction report/receipt paths and SHAs must all be supplied")
    result = compose_objective_entity_evidence(
        v3=v3, vertical=vertical,
        v3_artifact_sha256=args.v3_sha256,
        vertical_artifact_sha256=args.company_physics_sha256,
        adversarial=adversarial,
        adversarial_artifact_sha256=args.company_physics_adversarial_sha256,
        boundary_type=boundary_type,
        boundary_type_artifact_sha256=args.boundary_type_sha256,
        boundary_type_closure=closure,
        boundary_type_closure_artifact_sha256=args.boundary_type_closure_sha256,
        broad_extraction=broad,
        broad_extraction_artifact_sha256=args.broad_extraction_sha256,
        broad_extraction_receipt=broad_receipt,
        broad_extraction_receipt_sha256=args.broad_extraction_receipt_sha256,
    )
    write_atomic_json(args.output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
