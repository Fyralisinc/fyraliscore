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
    result = compose_objective_entity_evidence(
        v3=v3, vertical=vertical,
        v3_artifact_sha256=args.v3_sha256,
        vertical_artifact_sha256=args.company_physics_sha256,
        adversarial=adversarial,
        adversarial_artifact_sha256=args.company_physics_adversarial_sha256,
        boundary_type=boundary_type,
        boundary_type_artifact_sha256=args.boundary_type_sha256,
    )
    write_atomic_json(args.output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
