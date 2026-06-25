#!/usr/bin/env python3
"""Manage ingestion DLQ replay and quarantine from an operator shell.

Examples:

  python scripts/manage_ingestion_dlq.py list \
    --tenant 00000000-0000-0000-0000-000000000001 \
    --operator-actor 00000000-0000-0000-0000-000000000002

  python scripts/manage_ingestion_dlq.py replay \
    --tenant 00000000-0000-0000-0000-000000000001 \
    --operator-actor 00000000-0000-0000-0000-000000000002 \
    --failure-id 00000000-0000-0000-0000-000000000003 \
    --ingress-kind webhook \
    --reason "parser fix deployed"
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import sys

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.ingest.ingestion.dlq.operator import (  # noqa: E402
    IngestionDLQOperatorError,
    build_parser,
    run_command,
)


async def _main_async(argv: list[str] | None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.dsn:
        print(
            json.dumps({"ok": False, "error": "DATABASE_URL is not set"}),
            file=sys.stderr,
        )
        return 2
    conn = await asyncpg.connect(dsn=args.dsn)
    try:
        await _register_codecs(conn)
        result = await run_command(args, conn=conn)
    finally:
        await conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return asyncio.run(_main_async(argv))
    except IngestionDLQOperatorError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
