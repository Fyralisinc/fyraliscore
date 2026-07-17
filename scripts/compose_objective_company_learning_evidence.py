#!/usr/bin/env python3
"""Compose available objective company-learning artifacts; preserve unknowns."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from lib.evaluation.company_learning_evidence_composer import (
    BoundArtifact,
    compose_objective_company_learning_evidence,
)
from lib.evaluation.entity_evidence_composer import write_atomic_json


def _load(path: Path | None) -> BoundArtifact | None:
    if path is None:
        return None
    raw = path.read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"artifact root must be an object: {path}")
    return BoundArtifact(payload=payload, artifact_sha256=hashlib.sha256(raw).hexdigest())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retrieval-evolution", type=Path)
    parser.add_argument("--retrieval-evolution-postfix", type=Path)
    parser.add_argument("--company-model-ablation", type=Path)
    parser.add_argument("--company-model-ablation-legacy", type=Path)
    parser.add_argument("--company-model-ablation-active-failure", type=Path)
    parser.add_argument("--company-model-ablation-active-predecessor", type=Path)
    parser.add_argument("--feedback-learning", type=Path)
    parser.add_argument("--feedback-quality", type=Path)
    parser.add_argument("--source-equivalence", type=Path)
    parser.add_argument("--correction-homeostasis", type=Path)
    parser.add_argument("--joined-runtime", type=Path)
    parser.add_argument("--single-model-synthesis", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compose_objective_company_learning_evidence(
        retrieval_evolution=_load(args.retrieval_evolution),
        retrieval_evolution_postfix=_load(args.retrieval_evolution_postfix),
        company_model_ablation=_load(args.company_model_ablation),
        company_model_ablation_legacy=_load(args.company_model_ablation_legacy),
        company_model_ablation_active_failure=_load(
            args.company_model_ablation_active_failure
        ),
        company_model_ablation_active_predecessor=_load(
            args.company_model_ablation_active_predecessor
        ),
        feedback_learning=_load(args.feedback_learning),
        feedback_quality=_load(args.feedback_quality),
        source_equivalence=_load(args.source_equivalence),
        correction_homeostasis=_load(args.correction_homeostasis),
        joined_runtime=_load(args.joined_runtime),
        single_model_synthesis=_load(args.single_model_synthesis),
    )
    write_atomic_json(args.output, result)
    print(json.dumps({
        "output": str(args.output.resolve()), "verdict": result["verdict"],
        "coverage": result["evidence_coverage"],
        "coverage_adjusted_score": result["coverage_adjusted_score"],
        "composition_sha256": result["composition_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
