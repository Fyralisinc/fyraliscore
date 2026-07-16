#!/usr/bin/env python3
"""Build a readable correction-assurance artifact from runtime evidence."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import asyncpg

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.correction_assurance import (
    CorrectionAssuranceArtifact,
    CorrectionRuntimeEvidence,
    build_correction_assurance,
    render_correction_assurance_markdown,
)
from lib.evaluation.correction_propagation import (
    CorrectionPropagationAudit,
    CorrectionPropagationScope,
    evaluate_correction_propagation,
)


ARTIFACT_NAME = "correction_assurance.json"
MARKDOWN_NAME = "correction_assurance.md"


def run_correction_assurance(
    *,
    output_dir: Path,
    run_id: str,
    system_version: str,
    runtime_evidence: CorrectionRuntimeEvidence,
    audit: CorrectionPropagationAudit | None = None,
    created_at: datetime | None = None,
) -> CorrectionAssuranceArtifact:
    """Build and persist the canonical artifact for suite integration."""

    output_dir.mkdir(parents=True, exist_ok=True)
    artifact = build_correction_assurance(
        run_id=run_id,
        system_version=system_version,
        created_at=created_at or datetime.now(timezone.utc),
        runtime_evidence=runtime_evidence,
        audit=audit,
        artifact_refs=(
            *runtime_evidence.artifact_refs,
            f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",
        ),
    )
    (output_dir / ARTIFACT_NAME).write_text(
        json.dumps(artifact.artifact_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / MARKDOWN_NAME).write_text(
        render_correction_assurance_markdown(artifact),
        encoding="utf-8",
    )
    return artifact


async def run_correction_assurance_for_scope(
    conn: asyncpg.Connection,
    *,
    output_dir: Path,
    run_id: str,
    system_version: str,
    tenant_id: UUID,
    predecessor_grounding_trace_id: UUID,
    runtime_evidence: CorrectionRuntimeEvidence,
    observed_at: datetime | None = None,
) -> CorrectionAssuranceArtifact:
    """Attach the existing read-only dependency census before persistence."""

    measured_at = observed_at or datetime.now(timezone.utc)
    audit = await evaluate_correction_propagation(
        conn,
        scope=CorrectionPropagationScope(
            tenant_id=tenant_id,
            predecessor_grounding_trace_id=predecessor_grounding_trace_id,
            run_id=f"{run_id}:audit",
            observed_at=measured_at,
        ),
        artifact_refs=(
            *runtime_evidence.artifact_refs,
            f"correction-audit:{tenant_id}:{predecessor_grounding_trace_id}",
        ),
    )
    return run_correction_assurance(
        output_dir=output_dir,
        run_id=run_id,
        system_version=system_version,
        runtime_evidence=runtime_evidence,
        audit=audit,
        created_at=measured_at,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    evidence_payload = _read_json(args.runtime_evidence)
    runtime_evidence = CorrectionRuntimeEvidence.model_validate(
        evidence_payload
    )
    audit = (
        CorrectionPropagationAudit.model_validate(_read_json(args.audit))
        if args.audit is not None
        else None
    )
    artifact = run_correction_assurance(
        output_dir=args.output_dir,
        run_id=args.run_id,
        system_version=args.system_version,
        runtime_evidence=runtime_evidence,
        audit=audit,
    )
    print(f"artifact={args.output_dir / ARTIFACT_NAME}")
    print(
        "status={status} converged={converged} discovery={discovery} "
        "residual_unsafe={residual}".format(
            status=artifact.status,
            converged=artifact.metrics.converged,
            discovery=artifact.metrics.dependency_discovery_rate,
            residual=artifact.metrics.residual_unsafe_debt_count,
        )
    )
    return 0 if artifact.status == "working" else 2


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected one JSON object in {path}")
    return payload


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser = argparse.ArgumentParser(
        description=(
            "Build correction assurance from a runtime-evidence JSON contract "
            "and an optional read-only correction-propagation audit."
        )
    )
    parser.add_argument("--runtime-evidence", type=Path, required=True)
    parser.add_argument("--audit", type=Path)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / f"correction-assurance-{timestamp}",
    )
    parser.add_argument(
        "--run-id",
        default=f"correction-assurance-{timestamp}",
    )
    parser.add_argument("--system-version", default="local-working-tree")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARTIFACT_NAME",
    "MARKDOWN_NAME",
    "run_correction_assurance",
    "run_correction_assurance_for_scope",
]
