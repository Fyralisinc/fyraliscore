#!/usr/bin/env python3
"""Mutable development proof for strict single-Model synthesis."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg  # noqa: E402

from lib.evaluation.company_model_ablation import evaluate_single_model_synthesis  # noqa: E402
from lib.shared.migrations import apply_migrations_dir  # noqa: E402
from scripts.run_bounded_company_model_ablation_db import (  # noqa: E402
    REPO_ROOT,
    _run_frozen,
    _run_learned,
)
from scripts.single_model_synthesis_provider import (  # noqa: E402
    SingleModelSynthesisProvider,
)
from services.domain.models.repo import pgvector_pool_init  # noqa: E402

SUBJECTS = ("alpha", "beta", "gamma")
FACETS = {s: tuple(f"{s}_facet_{i}" for i in range(1, 7)) for s in SUBJECTS}
BATCHES = tuple(
    tuple((subject, facet) for facet in FACETS[subject][offset:offset + 3])
    for subject in SUBJECTS
    for offset in (0, 3)
)
MANIFEST = {"schema_version": "company-model-synthesis-manifest-v1",
    "experiment_id": "single-model-synthesis-development-v1",
    "hidden_patterns": [{"thesis_id": s, "required_facets": list(FACETS[s])}
        for s in SUBJECTS]}


async def _rows(pool, tenant_ids):
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT id,tenant_id,"natural",supporting_model_ids,created_at FROM models WHERE tenant_id=ANY($1::uuid[]) AND status=\'active\' ORDER BY created_at,id', tenant_ids)
    return [dict(row) for row in rows]


def _arm(name, rows, *, frozen):
    models, prior, required = [], set(), {}
    for subject in SUBJECTS:
        subject_rows = [r for r in rows if (
            str(r["natural"]).startswith(f"{subject} evidence facets:")
            or str(r["natural"]).startswith(
                f"cross batch synthesized pattern for {subject}; integrated evidence:"
            )
        )]
        complete = []
        for row in subject_rows:
            facets = {x.strip() for x in str(row["natural"]).partition(":")[2].split(",")}
            lineage = {str(x) for x in row["supporting_model_ids"] or []}
            models.append({"model_id": str(row["id"]), "thesis_id": subject,
                "facets": sorted(facets), "evidence_model_ids": sorted(lineage),
                "persisted": True})
            if set(FACETS[subject]) <= facets:
                complete.append(row)
            else:
                prior.add(str(row["id"]))
        required[subject] = sorted(str(r["id"]) for r in subject_rows
            if complete and r["created_at"] < complete[-1]["created_at"]
            and str(r["natural"]).startswith(f"{subject} evidence facets:"))
        if frozen:
            required[subject] = []
    return {"schema_version": "company-model-synthesis-arm-v1", "arm": name,
        "prior_model_ids": sorted(prior), "required_lineage_by_thesis": required,
        "models": models}


async def run(dsn: str, output: Path):
    os.environ["THINK_COMPILED_BATCH_MEMORY_REASONING"] = "1"
    os.environ["INQUIRY_LLM_QUESTION_PLANNING_ENABLED"] = "0"
    conn = await asyncpg.connect(dsn)
    try:
        await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
    finally:
        await conn.close()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, init=pgvector_pool_init)
    try:
        kw = {"consume_model_summaries": True, "batch_definitions": BATCHES,
            "provider_factory": SingleModelSynthesisProvider}
        _, lm, _ = await _run_learned(pool, **kw)
        _, fm, _ = await _run_frozen(pool, **kw)
        learned_rows = await _rows(pool, list({r["tenant_id"] for r in lm}))
        frozen_rows = await _rows(pool, list({r["tenant_id"] for r in fm}))
        learned, frozen = _arm("learned_memory", learned_rows, frozen=False), _arm("frozen_memory", frozen_rows, frozen=True)
        evaluation = evaluate_single_model_synthesis(manifest=MANIFEST, learned=learned, frozen=frozen)
        artifact = {"evidence_class": "development_only", "manifest": MANIFEST,
            "learned_arm": learned, "frozen_arm": frozen, "evaluation": evaluation}
        output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
        return artifact
    finally:
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(run(args.dsn, args.output))
    print(json.dumps(result["evaluation"], sort_keys=True))
