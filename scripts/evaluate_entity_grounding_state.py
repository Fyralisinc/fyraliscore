#!/usr/bin/env python3
"""Measure one tenant/time-bounded entity-grounding population."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import asyncpg

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.entity_grounding import (
    GroundingEvaluationScope,
    build_entity_grounding_invariant_evidence,
    evaluate_entity_grounding_state,
    render_entity_grounding_markdown,
)
from lib.evaluation.proof import (
    InvariantEvidenceManifest,
    compile_invariant_proof_matrix,
    render_invariant_proof_markdown,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "architecture/registry.yaml"


def _time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO-8601 time: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("time must include an offset or Z")
    return parsed


async def _run(args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL or --dsn is required")
    registry = load_architecture_registry(args.registry)
    scope = GroundingEvaluationScope(
        tenant_id=args.tenant_id,
        observation_start=args.start,
        observation_end=args.end,
        run_id=args.run_id,
    )
    conn = await asyncpg.connect(dsn)
    try:
        state = await evaluate_entity_grounding_state(
            conn,
            scope=scope,
            artifact_refs=tuple(args.artifact_ref),
        )
    finally:
        await conn.close()
    evidence = build_entity_grounding_invariant_evidence(
        state,
        registry=registry,
        executed_scenario_ids=frozenset(args.executed_scenario),
    )
    manifest = InvariantEvidenceManifest(
        manifest_version="entity-grounding-evidence-v1",
        run_id=args.run_id,
        architecture_digest=registry.digest,
        system_version=args.system_version,
        created_at=datetime.now(timezone.utc).isoformat(),
        experiment_manifest_ref=args.experiment_manifest_ref,
        evidence=evidence,
        artifact_refs=tuple(args.artifact_ref),
    )
    proof = compile_invariant_proof_matrix(
        registry,
        run_id=args.run_id,
        evidence=evidence,
    )
    if args.format == "json":
        print(
            json.dumps(
                {
                    "grounding_state": state.model_dump(mode="json"),
                    "evidence_manifest": manifest.model_dump(mode="json"),
                    "invariant_proof_report": proof.model_dump(mode="json"),
                    "production_freeze_ready": proof.production_freeze_ready,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(render_entity_grounding_markdown(state))
        print(render_invariant_proof_markdown(proof))
    if args.require_no_incidents and state.incident_counts:
        return 2
    if args.require_complete_fates:
        required_rates = [
            state.work_population_coverage,
            state.mention_detection_population_coverage,
            state.mention_context_continuity_rate,
            state.mention_protocol_closure_rate,
        ]
        if state.explicit_anchor_count:
            required_rates.append(state.explicit_anchor_reconstructability_rate)
        if state.detected_mention_count:
            required_rates.append(
                state.detected_mention_to_candidate_continuity_rate
            )
        if state.rejected_not_anchored_count:
            required_rates.append(state.rejected_not_anchored_correctness_rate)
        if state.terminal_trace_required_count:
            required_rates.append(state.terminal_trace_coverage)
        if state.candidate_request_count:
            required_rates.append(state.candidate_request_fate_coverage)
        if any(value is None or value < 1.0 for value in required_rates):
            return 3
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", help="Postgres DSN; defaults to DATABASE_URL")
    parser.add_argument("--tenant-id", type=UUID, required=True)
    parser.add_argument("--start", type=_time, required=True)
    parser.add_argument("--end", type=_time, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--system-version", required=True)
    parser.add_argument("--experiment-manifest-ref", required=True)
    parser.add_argument("--artifact-ref", action="append", required=True)
    parser.add_argument("--executed-scenario", action="append", default=[])
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--require-no-incidents", action="store_true")
    parser.add_argument("--require-complete-fates", action="store_true")
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
