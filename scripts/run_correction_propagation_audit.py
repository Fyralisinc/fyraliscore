#!/usr/bin/env python3
"""Run a read-only correction-propagation dependency census."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg

from lib.evaluation.correction_propagation import (
    CorrectionPropagationScope,
    evaluate_correction_propagation,
    render_correction_propagation_markdown,
)


async def run_audit(
    *,
    database_url: str,
    tenant_id: UUID,
    grounding_trace_id: UUID,
    run_id: str,
    output_dir: Path,
) -> int:
    conn = await asyncpg.connect(database_url)
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            audit = await evaluate_correction_propagation(
                conn,
                scope=CorrectionPropagationScope(
                    tenant_id=tenant_id,
                    predecessor_grounding_trace_id=grounding_trace_id,
                    run_id=run_id,
                    observed_at=datetime.now(timezone.utc),
                ),
                artifact_refs=(
                    f"correction-propagation-audit:{output_dir.resolve()}",
                ),
            )
    finally:
        await conn.close()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "correction_propagation_audit.json"
    markdown_path = output_dir / "correction_propagation_audit.md"
    json_path.write_text(
        json.dumps(audit.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_correction_propagation_markdown(audit),
        encoding="utf-8",
    )
    print(f"audit_json={json_path}")
    print(f"audit_markdown={markdown_path}")
    print(
        "correction_found={found} discovered={discovered} "
        "unsafe_readable={unsafe} residual_debt={debt} converged={converged}".format(
            found=audit.correction_found,
            discovered=audit.discovered_dependency_count,
            unsafe=audit.unsafe_readable_count,
            debt=audit.residual_repair_debt_count,
            converged=audit.converged,
        )
    )
    integrity_failure = (
        not audit.correction_found
        or audit.source_immutable is not True
        or audit.cross_tenant_reference_count > 0
        or audit.cross_tenant_change_count > 0
    )
    return 2 if integrity_failure else 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    database_url = args.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL or --database-url is required", file=sys.stderr)
        return 2
    return asyncio.run(
        run_audit(
            database_url=database_url,
            tenant_id=args.tenant_id,
            grounding_trace_id=args.grounding_trace_id,
            run_id=args.run_id,
            output_dir=args.output_dir,
        )
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser = argparse.ArgumentParser(
        description=(
            "Audit tenant-scoped downstream state rooted at one adjudicated "
            "entity-grounding correction. This command performs no repair writes."
        )
    )
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--tenant-id", type=UUID, required=True)
    parser.add_argument("--grounding-trace-id", type=UUID, required=True)
    parser.add_argument(
        "--run-id",
        default=f"correction-propagation-audit-{timestamp}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / f"correction-propagation-audit-{timestamp}",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
