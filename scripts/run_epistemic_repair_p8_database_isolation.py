#!/usr/bin/env python3
"""Prove one literal fresh-template-cloned P8 database cell lifecycle."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from services.evaluation.epistemic_repair.p8_database_isolation import (
    prove_existing_template_cells,
    prove_fresh_database_cell,
)
from lib.evaluation.epistemic_repair.p8_population import ScaleCell


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    if args.template_database:
        proof = await prove_existing_template_cells(
            args.database_url, template_name=args.template_database,
            migrations_dir=ROOT / "db" / "migrations",
            cells=(ScaleCell("p8-isolation-proof-1", 10, 3, 1),
                   ScaleCell("p8-isolation-proof-2", 10, 3, 2)),
        )
        ready = proof.all_database_oids_distinct and proof.all_cell_databases_dropped
        migration_count = proof.template_identity.schema_migration_count
    else:
        proof = await prove_fresh_database_cell(
            args.database_url, migrations_dir=ROOT / "db" / "migrations",
            cell=ScaleCell("p8-isolation-proof", 10, 3, 2),
        )
        ready = proof.identities_distinct and proof.template_dropped and proof.cell_database_dropped
        migration_count = proof.template_identity.schema_migration_count
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(asdict(proof), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"database_isolation_proven={str(ready).lower()} template_migrations={migration_count} output={args.output}")
    return 0 if ready else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/p8-database-isolation-proof.json"))
    parser.add_argument("--template-database")
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
