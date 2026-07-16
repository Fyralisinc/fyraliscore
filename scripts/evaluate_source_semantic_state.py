#!/usr/bin/env python3
"""Measure one tenant/time-bounded source-semantic admission population."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol
from uuid import UUID

import asyncpg

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.proof import (
    InvariantEvidenceManifest,
    compile_invariant_proof_matrix,
    render_invariant_proof_markdown,
)
from lib.evaluation.source_semantics import (
    SourceSemanticEvaluationScope,
    build_source_semantic_invariant_evidence,
    evaluate_source_semantic_state,
    render_source_semantic_markdown,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "architecture/registry.yaml"


class _CoreCoverageState(Protocol):
    eligible_grounding_interpretation_coverage: float | None
    source_coordinate_reconstructability_rate: float | None
    interpretation_structural_closure_rate: float | None
    grounding_continuity_exactness_rate: float | None
    explicit_admission_fate_coverage: float | None
    epistemic_consumer_admission_continuity_rate: float | None
    applied_decision_model_coverage: float | None
    one_model_cardinality_rate: float | None
    model_source_provenance_rate: float | None
    model_scope_referent_rate: float | None
    model_grounding_dependency_rate: float | None
    model_dependency_closure_rate: float | None
    non_admitted_no_model_safety_rate: float | None
    supported_report_admission_precision: float | None
    supported_report_admission_recall: float | None


def _time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO-8601 time: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("time must include an offset or Z")
    return parsed


def _observed_core_coverage_is_complete(state: _CoreCoverageState) -> bool:
    """Require perfect observed fates without converting unknown rates to 1.0.

    The compiled proof report, not this component-local gate, records missing
    exposure and scenario evidence as insufficient system proof.
    """

    rates = (
        state.eligible_grounding_interpretation_coverage,
        state.source_coordinate_reconstructability_rate,
        state.interpretation_structural_closure_rate,
        state.grounding_continuity_exactness_rate,
        state.explicit_admission_fate_coverage,
        state.epistemic_consumer_admission_continuity_rate,
        state.applied_decision_model_coverage,
        state.one_model_cardinality_rate,
        state.model_source_provenance_rate,
        state.model_scope_referent_rate,
        state.model_grounding_dependency_rate,
        state.model_dependency_closure_rate,
        state.non_admitted_no_model_safety_rate,
        state.supported_report_admission_precision,
        state.supported_report_admission_recall,
    )
    return all(rate == 1.0 for rate in rates if rate is not None)


async def _run(args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL or --dsn is required")
    registry = load_architecture_registry(args.registry)
    scope = SourceSemanticEvaluationScope(
        tenant_id=args.tenant_id,
        start=args.start,
        end=args.end,
        run_id=args.run_id,
    )
    conn = await asyncpg.connect(dsn)
    try:
        state = await evaluate_source_semantic_state(
            conn,
            scope=scope,
            artifact_refs=tuple(args.artifact_ref),
        )
    finally:
        await conn.close()
    evidence = build_source_semantic_invariant_evidence(
        state,
        registry=registry,
        executed_scenario_ids=frozenset(args.executed_scenario),
    )
    manifest = InvariantEvidenceManifest(
        manifest_version="source-semantic-evidence-v1",
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
                    "source_semantic_state": state.model_dump(mode="json"),
                    "evidence_manifest": manifest.model_dump(mode="json"),
                    "invariant_proof_report": proof.model_dump(mode="json"),
                    "production_freeze_ready": proof.production_freeze_ready,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(render_source_semantic_markdown(state))
        print(render_invariant_proof_markdown(proof))
    if args.require_no_incidents and state.incident_counts:
        return 2
    if (
        args.require_complete_fates
        and not _observed_core_coverage_is_complete(state)
    ):
        return 3
    return 0


def _parser() -> argparse.ArgumentParser:
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
    return parser


def main() -> int:
    return asyncio.run(_run(_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
