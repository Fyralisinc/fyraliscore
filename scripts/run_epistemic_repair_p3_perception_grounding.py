#!/usr/bin/env python3
"""Run the sealed provider-free P3 perception and grounding evaluator."""

from __future__ import annotations

import argparse
from pathlib import Path

from lib.evaluation.epistemic_repair.p3_runner import (
    ARTIFACT_NAME,
    P3PerceptionRuntime,
    run_p3_perception_grounding,
    write_p3_artifact,
    write_p3_artifact_schema,
)
from services.domain.entity_grounding.episode import (
    ContextObservationInput,
    GroundingCandidateInput,
    build_adjudicated_grounding_decision,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import (
    prepare_entity_mention_detection,
)


ROOT = Path(__file__).resolve().parents[1]


def _production_runtime() -> P3PerceptionRuntime:
    return P3PerceptionRuntime(
        context_observation_type=ContextObservationInput,
        grounding_candidate_type=GroundingCandidateInput,
        prepare_context_selection=prepare_context_selection,
        prepare_entity_mention_detection=prepare_entity_mention_detection,
        build_grounding_episode=build_grounding_episode,
        build_adjudicated_grounding_decision=build_adjudicated_grounding_decision,
        candidate_id_for_ref=candidate_id_for_ref,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Execute the sealed 120-signal P3 evaluator. This starts from "
            "normalized signal fixtures and does not run ingestion listeners."
        )
    )
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=ROOT,
        help="repository root used to digest the exercised production sources",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(ARTIFACT_NAME),
        help="JSON artifact path",
    )
    parser.add_argument(
        "--schema-output",
        type=Path,
        help="optional path for the P3 artifact JSON Schema",
    )
    args = parser.parse_args()
    report = run_p3_perception_grounding(
        repository_root=args.repository_root.resolve(),
        runtime=_production_runtime(),
    )
    write_p3_artifact(report, args.output)
    if args.schema_output is not None:
        write_p3_artifact_schema(args.schema_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
