#!/usr/bin/env python3
"""Apply Fyralis core database migrations with the shared Python runner."""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.shared.migrations import apply_migrations_dir  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    parser.add_argument(
        "--migrations-dir",
        type=pathlib.Path,
        default=_REPO_ROOT / "db" / "migrations",
        help="Directory containing core *.sql migrations.",
    )
    return parser


async def _amain(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.dsn:
        print("DATABASE_URL or --dsn is required", file=sys.stderr)
        return 2
    conn = await asyncpg.connect(args.dsn)
    try:
        applied = await apply_migrations_dir(conn, args.migrations_dir)
    finally:
        await conn.close()
    print(f"Core migrations complete. Applied: {len(applied)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
