#!/usr/bin/env python3
"""Score one tenant's persisted entity pipeline against evaluator-owned gold."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from lib.evaluation.entity_pipeline_gold import (
    GoldEntityPipelineCase,
    evaluate_persisted_entity_pipeline,
)


def load_gold_manifest_bundle(
    path: Path,
) -> tuple[list[GoldEntityPipelineCase], dict[str, str], dict[str, str]]:
    payload: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("gold manifest must be one JSON object")
    schema_version = payload.get("schema_version")
    if schema_version not in {
        "gold-entity-pipeline-corpus-v1",
        "gold-entity-pipeline-corpus-v2",
    }:
        raise ValueError("unsupported gold entity pipeline corpus schema")
    raw_cases = payload.get("cases")
    raw_labels = payload.get("canonical_gold_labels")
    if not isinstance(raw_cases, list) or not isinstance(raw_labels, dict):
        raise ValueError("gold manifest requires cases and canonical_gold_labels")
    cases = [GoldEntityPipelineCase.model_validate(item) for item in raw_cases]
    labels = {str(key): str(value) for key, value in raw_labels.items()}
    topology_labels: dict[str, str] = {}
    if schema_version == "gold-entity-pipeline-corpus-v2":
        population_version = payload.get("topology_population_version")
        raw_topology_labels = payload.get("topology_model_gold_labels")
        if not isinstance(population_version, str) or not population_version.strip():
            raise ValueError("v2 manifest requires topology_population_version")
        if not isinstance(raw_topology_labels, dict):
            raise ValueError("v2 manifest requires topology_model_gold_labels")
        topology_labels = {
            str(model_id): str(gold_label)
            for model_id, gold_label in raw_topology_labels.items()
        }
    if any(case.expected_relations for case in cases) and schema_version != (
        "gold-entity-pipeline-corpus-v2"
    ):
        raise ValueError("relation expectations require a versioned v2 topology population")
    return cases, labels, topology_labels


def load_gold_manifest(path: Path) -> tuple[list[GoldEntityPipelineCase], dict[str, str]]:
    """Backward-compatible loader for extraction/link-only callers."""

    cases, labels, _ = load_gold_manifest_bundle(path)
    return cases, labels


async def _run(args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL or --dsn is required")
    cases, labels, topology_labels = load_gold_manifest_bundle(args.gold_manifest)
    conn = await asyncpg.connect(dsn)
    try:
        report = await evaluate_persisted_entity_pipeline(
            conn,
            tenant_id=args.tenant_id,
            gold_cases=cases,
            canonical_gold_labels=labels,
            topology_model_gold_labels=topology_labels,
            ks=tuple(args.k),
        )
    finally:
        await conn.close()
    rendered = json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate persisted batched entity grounding against a sealed gold manifest"
        )
    )
    parser.add_argument("--dsn", help="Postgres DSN; defaults to DATABASE_URL")
    parser.add_argument("--tenant-id", type=UUID, required=True)
    parser.add_argument("--gold-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("-k", action="append", type=int, default=[])
    args = parser.parse_args()
    if not args.k:
        args.k = [1, 3, 5]
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
