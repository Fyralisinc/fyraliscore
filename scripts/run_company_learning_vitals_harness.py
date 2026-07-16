#!/usr/bin/env python3
"""Run the real paired learning experiment and render joined Company Vitals."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg

from lib.shared.migrations import apply_migrations_dir
from scripts.company_vitals import (
    VitalsArtifacts,
    collect_db_trace_for_report_dir,
    write_vitals_artifacts,
)
from scripts.run_company_learning_pair_harness import run_pair_experiment


ROOT = Path(__file__).resolve().parents[1]


async def run_joined_company_learning_vitals(
    *,
    database_url: str,
    report_dir: Path,
    run_id: str,
    system_version: str,
    llm_call_cost_usd: float = 0.001,
) -> VitalsArtifacts:
    """Produce one report joining DB E3 evidence and paired E4 evidence."""

    pool = await asyncpg.create_pool(
        database_url,
        min_size=1,
        max_size=3,
        init=_install_json_codec,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, ROOT / "db" / "migrations")
        payload = await run_pair_experiment(
            pool=pool,
            output_dir=report_dir,
            run_id=run_id,
            system_version=system_version,
            llm_call_cost_usd=llm_call_cost_usd,
        )
    finally:
        await pool.close()

    selected = payload["report"]["pairs"][0]["adaptive"]
    write_company_learning_report_shell(
        report_dir,
        run_id=run_id,
        system_version=system_version,
        tenant_id=str(selected["tenant_id"]),
        observation_ids=(
            str(selected["lineage"]["training_observation_id"]),
            str(selected["lineage"]["recurrence_observation_id"]),
        ),
    )
    db_trace = await collect_db_trace_for_report_dir(
        report_dir,
        database_url=database_url,
    )
    return write_vitals_artifacts(report_dir, db_trace=db_trace)


def write_company_learning_report_shell(
    report_dir: Path,
    *,
    run_id: str,
    system_version: str,
    tenant_id: str,
    observation_ids: tuple[str, ...],
) -> None:
    """Write the standard report artifacts consumed by Company Vitals."""

    report_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        report_dir / "run_config.json",
        {"system_version": system_version},
    )
    _write_json(
        report_dir / "run_summary.json",
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "signal_count": len(observation_ids),
            "observation_count": len(observation_ids),
            "vitals_measurement_profile": "company_learning_only",
        },
    )
    _write_json(
        report_dir / "benchmark_summary.json",
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "status": "diagnostic_only",
            "required_run_failures": [],
            "company_intelligence_scorecard": {
                "overall_score": None,
                "interpretation": (
                    "Focused autonomous company-learning proof; general "
                    "product vitals were not measured."
                ),
                "proof_gaps": [],
                "dimensions": {},
                "product_value_evals": {
                    "overall_score": None,
                    "proof_gaps": [],
                    "evals": {},
                },
            },
        },
    )
    planned = [
        {
            "index": index,
            "storyline_id": "corrective-memory",
            "sequence": f"joined_{index}",
            "family": "entity_grounding",
            "content": (
                "NBI training clarification"
                if index == 0
                else "NBI held-out recurrence"
            ),
        }
        for index, _ in enumerate(observation_ids)
    ]
    manifest = [
        {
            "index": index,
            "channel": "slack:message",
            "family": "entity_grounding",
            "observation_id": observation_id,
        }
        for index, observation_id in enumerate(observation_ids)
    ]
    _write_jsonl(report_dir / "planned_signals.jsonl", planned)
    _write_jsonl(report_dir / "signal_manifest.jsonl", manifest)
    _write_jsonl(report_dir / "models.jsonl", [])
    _write_jsonl(report_dir / "model_edges.jsonl", [])


async def _install_json_codec(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda value: (
            json.dumps(value) if not isinstance(value, str) else value
        ),
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=lambda value: (
            json.dumps(value) if not isinstance(value, str) else value
        ),
        decoder=json.loads,
        schema="pg_catalog",
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    database_url = args.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL or --database-url is required", file=sys.stderr)
        return 2
    result = asyncio.run(
        run_joined_company_learning_vitals(
            database_url=database_url,
            report_dir=args.report_dir,
            run_id=args.run_id,
            system_version=args.system_version,
            llm_call_cost_usd=args.llm_call_cost_usd,
        )
    )
    experiment = (
        result.scorecard.get("company_physics", {})
        .get("experiments", {})
        .get("corrective_memory_recurrence", {})
    )
    failures = _working_version_failures(result)
    print(f"report_dir={result.report_dir}")
    print(f"vitals_dir={result.output_dir}")
    print(f"scorecard={result.output_dir / 'vitals_scorecard.json'}")
    evidence_bundle_path = (
        result.output_dir / "company_learning_evidence_bundle.json"
    )
    if evidence_bundle_path.is_file():
        print(f"evidence_bundle={evidence_bundle_path}")
    print(
        (
            "working_version_status={status} company_physics_status={physics} "
            "overall_score={score} adaptive_lift={lift}"
        ).format(
            status="fail" if failures else "ok",
            physics=result.scorecard.get("company_physics", {}).get("status"),
            score=result.scorecard.get("overall_score"),
            lift=experiment.get("metrics", {}).get(
                "adaptive_minus_frozen_correctness"
            ),
        )
    )
    for failure in failures:
        print(f"working-version failure: {failure}", file=sys.stderr)
    return 2 if failures else 0


def _working_version_failures(result: VitalsArtifacts) -> list[str]:
    required_paths = {
        "paired experiment": (
            result.report_dir / "company_learning_scenario_evidence.json"
        ),
        "company-learning evaluation": (
            result.output_dir / "company_learning_evaluation.json"
        ),
        "company-learning evidence bundle": (
            result.output_dir / "company_learning_evidence_bundle.json"
        ),
        "Vitals scorecard": result.output_dir / "vitals_scorecard.json",
    }
    failures = [
        f"{name} artifact is missing: {path}"
        for name, path in required_paths.items()
        if not path.is_file()
    ]
    scorecard = result.scorecard
    failures.extend(str(item) for item in scorecard.get("hard_failures", []))
    company_physics = scorecard.get("company_physics", {})
    if company_physics.get("status") in {
        None,
        "not_observed",
        "unavailable",
    } or not company_physics.get("scope"):
        failures.append("DB-backed Company Physics evaluation is unavailable")
    failures.extend(
        str(item) for item in company_physics.get("hard_failures", [])
    )
    experiment = (
        company_physics.get("experiments", {})
        .get("corrective_memory_recurrence", {})
    )
    if experiment.get("available") is not True:
        failures.append("typed corrective-memory experiment is unavailable")
    if experiment.get("status") != "observed":
        failures.append(
            "corrective-memory experiment did not reach observed status"
        )
    if int(experiment.get("hard_safety_incident_count") or 0) > 0:
        failures.append(
            "corrective-memory experiment recorded hard-safety incidents"
        )
    lift = experiment.get("metrics", {}).get(
        "adaptive_minus_frozen_correctness"
    )
    if not isinstance(lift, (int, float)):
        failures.append("adaptive-versus-frozen correctness lift is missing")
    return sorted(set(failures))


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser = argparse.ArgumentParser(
        description=(
            "Run the paired corrective-memory experiment, collect its live "
            "DB-backed company-learning slice, and render joined Company Vitals."
        )
    )
    parser.add_argument("--database-url", default=None)
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=Path("reports") / f"company-learning-vitals-{timestamp}",
    )
    parser.add_argument(
        "--run-id",
        default=f"company-learning-vitals-{timestamp}",
    )
    parser.add_argument("--system-version", default="local-working-tree")
    parser.add_argument("--llm-call-cost-usd", type=float, default=0.001)
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
