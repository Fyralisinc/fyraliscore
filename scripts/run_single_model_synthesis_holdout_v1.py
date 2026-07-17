#!/usr/bin/env python3
"""Execute the frozen strict single-Model synthesis holdout exactly once."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
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
    REPO_ROOT, _run_frozen, _run_learned,
)
from scripts.single_model_synthesis_provider import SingleModelSynthesisProvider  # noqa: E402
from services.domain.models.repo import pgvector_pool_init  # noqa: E402
from tests.evaluation.single_model_synthesis_holdout_v1 import (  # noqa: E402
    BATCHES_V1, CORPUS_DIGEST_V1, FACETS_V1, MANIFEST_DIGEST_V1, MANIFEST_V1,
    SUBJECTS_V1,
)


async def _rows(pool, tenant_ids):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT id,tenant_id,"natural",supporting_model_ids,created_at '
            "FROM models WHERE tenant_id=ANY($1::uuid[]) AND status='active' "
            "ORDER BY created_at,id", tenant_ids)
    return [dict(row) for row in rows]


def _arm(name, rows, *, frozen):
    models, prior, required = [], set(), {}
    for subject in SUBJECTS_V1:
        prefix = f"{subject} evidence facets:"
        synthesis = f"cross batch synthesized pattern for {subject}; integrated evidence:"
        subject_rows = [r for r in rows if str(r["natural"]).startswith((prefix, synthesis))]
        complete = []
        for row in subject_rows:
            facets = {x.strip() for x in str(row["natural"]).partition(":")[2].split(",")}
            lineage = {str(x) for x in row["supporting_model_ids"] or []}
            models.append({"model_id": str(row["id"]), "thesis_id": subject,
                "facets": sorted(facets), "evidence_model_ids": sorted(lineage),
                "persisted": True})
            if set(FACETS_V1[subject]) <= facets:
                complete.append(row)
            else:
                prior.add(str(row["id"]))
        required[subject] = sorted(str(r["id"]) for r in subject_rows
            if complete and r["created_at"] < complete[-1]["created_at"]
            and str(r["natural"]).startswith(prefix))
        if frozen:
            required[subject] = []
    return {"schema_version": "company-model-synthesis-arm-v1", "arm": name,
        "prior_model_ids": sorted(prior), "required_lineage_by_thesis": required,
        "models": models}


async def run_once(dsn, output, receipt):
    if output.exists() or receipt.exists():
        raise RuntimeError("single-Model synthesis holdout v1 is one-shot")
    os.environ["THINK_COMPILED_BATCH_MEMORY_REASONING"] = "1"
    os.environ["INQUIRY_LLM_QUESTION_PLANNING_ENABLED"] = "0"
    meta = {"schema_version": "single-model-synthesis-holdout-receipt-v1",
        "status": "running", "started_at": datetime.now(timezone.utc).isoformat(),
        "run_attempts": 1, "manifest_digest": MANIFEST_DIGEST_V1,
        "corpus_digest": CORPUS_DIGEST_V1}
    receipt.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    try:
        conn = await asyncpg.connect(dsn)
        try:
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
        finally:
            await conn.close()
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, init=pgvector_pool_init)
        try:
            kw = {"consume_model_summaries": True, "batch_definitions": BATCHES_V1,
                "provider_factory": SingleModelSynthesisProvider}
            _, learned_models, _ = await _run_learned(pool, **kw)
            _, frozen_models, _ = await _run_frozen(pool, **kw)
            learned_rows = await _rows(pool, list({r["tenant_id"] for r in learned_models}))
            frozen_rows = await _rows(pool, list({r["tenant_id"] for r in frozen_models}))
            learned = _arm("learned_memory", learned_rows, frozen=False)
            frozen = _arm("frozen_memory", frozen_rows, frozen=True)
            evaluation = evaluate_single_model_synthesis(
                manifest=MANIFEST_V1, learned=learned, frozen=frozen)
            artifact = {"schema_version": "single-model-synthesis-holdout-v1-artifact-v1",
                "sealing": {"manifest_digest": MANIFEST_DIGEST_V1,
                    "corpus_digest": CORPUS_DIGEST_V1, "one_shot": True,
                    "active_batch_memory_enabled": True, "cross_batch_required": True},
                "manifest": MANIFEST_V1, "learned_arm": learned,
                "frozen_arm": frozen, "evaluation": evaluation}
            output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
        finally:
            await pool.close()
        meta.update({"status": "completed", "completed_at": datetime.now(timezone.utc).isoformat(),
            "artifact_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
            "verdict": evaluation["verdict"], "continuous_score": evaluation["continuous_score"]})
        return artifact
    except Exception as exc:
        meta.update({"status": "failed", "completed_at": datetime.now(timezone.utc).isoformat(),
            "error_type": type(exc).__name__, "error_message": str(exc)})
        raise
    finally:
        receipt.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(run_once(args.dsn, args.output, args.receipt))
    print(json.dumps(result["evaluation"], sort_keys=True))


if __name__ == "__main__":
    main()
