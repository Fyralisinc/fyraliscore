#!/usr/bin/env python3
"""scripts/sandbox_inspect.py — read-only validation oracle for the
real-API ingestion sandbox.

Answers the questions the per-source validation needs: did OAuth install
land, did backfill plan + drain shards, did observations appear with the
right source_channel, are they deduped (live twin vs backfilled), did
embeddings fill, and is anything in the DLQ?

Run inside the sandbox stack so it shares DATABASE_URL:

    docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \\
        exec gateway python scripts/sandbox_inspect.py

Optional: --tenant <uuid> (default $COMPANY_OS_TENANT_ID, else all tenants).
Read-only: issues SELECTs only.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
from uuid import UUID

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import asyncpg

from services.gateway.db_bootstrap import _register_codecs


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sandbox_inspect")
    p.add_argument("--tenant", default=os.environ.get("COMPANY_OS_TENANT_ID"))
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    return p.parse_args()


def _hdr(title: str) -> None:
    print(f"\n=== {title} ===")


async def _run(args: argparse.Namespace) -> int:
    if not args.dsn:
        print("ERROR: no DSN — pass --dsn or set DATABASE_URL", file=sys.stderr)
        return 1
    tid = UUID(args.tenant) if args.tenant else None
    # `where` clauses parameterised on tenant (None = all tenants).
    twhere = "WHERE tenant_id = $1" if tid else ""
    targs = [tid] if tid else []

    pool = await asyncpg.create_pool(dsn=args.dsn, min_size=1, max_size=2, init=_register_codecs)
    try:
        print(f"sandbox inspect — tenant={tid or 'ALL'}")

        _hdr("provider_installations (OAuth install landed?)")
        rows = await pool.fetch(
            f"""SELECT provider, installation_id, enabled,
                       (secret_ref IS NOT NULL) AS has_secret
                  FROM provider_installations {twhere}
                 ORDER BY provider, installation_id""",
            *targs,
        )
        if not rows:
            print("  (none) — no installs yet; run the OAuth install flow")
        for r in rows:
            flag = "" if r["has_secret"] else "  <-- secret_ref NULL (seed it for github!)"
            print(f"  {r['provider']:<8} install={r['installation_id']:<22} "
                  f"enabled={r['enabled']} secret={r['has_secret']}{flag}")

        _hdr("onboarding_triggers (install → backfill kickoff)")
        rows = await pool.fetch(
            f"""SELECT source, trigger_kind, count(*) n
                  FROM onboarding_triggers {twhere}
                 GROUP BY source, trigger_kind ORDER BY source""",
            *targs,
        )
        for r in rows:
            print(f"  {r['source']:<8} {r['trigger_kind']:<14} n={r['n']}")
        if not rows:
            print("  (none)")

        _hdr("onboarding_runs (backfill state machine)")
        rows = await pool.fetch(
            f"""SELECT status, count(*) n FROM onboarding_runs {twhere}
                 GROUP BY status ORDER BY status""",
            *targs,
        )
        for r in rows:
            print(f"  status={r['status']:<16} n={r['n']}")
        if not rows:
            print("  (none)")

        _hdr("onboarding_shards (fetch units; state + progress)")
        rows = await pool.fetch(
            f"""SELECT source, state, count(*) n,
                       coalesce(sum(pages_fetched),0) pages,
                       coalesce(sum(observations_seen),0) seen
                  FROM onboarding_shards {twhere}
                 GROUP BY source, state ORDER BY source, state""",
            *targs,
        )
        for r in rows:
            print(f"  {r['source']:<8} state={r['state']:<22} n={r['n']:<4} "
                  f"pages={r['pages']:<5} obs_seen={r['seen']}")
        if not rows:
            print("  (none)")

        _hdr("observations by source_channel (dedup + embedding)")
        rows = await pool.fetch(
            f"""SELECT source_channel,
                       count(*) total,
                       count(DISTINCT external_id) distinct_ext,
                       count(*) FILTER (WHERE embedding IS NOT NULL) embedded,
                       count(*) FILTER (WHERE embedding_pending) pending
                  FROM observations {twhere}
                 GROUP BY source_channel ORDER BY source_channel""",
            *targs,
        )
        if not rows:
            print("  (none) — no observations ingested yet")
        for r in rows:
            dup = "" if r["total"] == r["distinct_ext"] else \
                f"  <-- {r['total'] - r['distinct_ext']} duplicate external_id(s)!"
            print(f"  {r['source_channel']:<22} total={r['total']:<5} "
                  f"distinct_ext={r['distinct_ext']:<5} embedded={r['embedded']:<5} "
                  f"pending={r['pending']}{dup}")

        _hdr("ingestion_failures (DLQ — unresolved)")
        rows = await pool.fetch(
            f"""SELECT source, failure_kind, count(*) n
                  FROM ingestion_failures
                 WHERE resolved_at IS NULL {("AND tenant_id = $1" if tid else "")}
                 GROUP BY source, failure_kind ORDER BY n DESC""",
            *targs,
        )
        if not rows:
            print("  (none) — clean")
        for r in rows:
            print(f"  {r['source']:<8} {r['failure_kind']:<28} n={r['n']}")

        print("\nInterpretation:")
        print("  - install present + has_secret=True ⇒ webhook verification can pass")
        print("  - observations total == distinct_ext ⇒ no dup external_ids (live deduped vs backfill)")
        print("  - embedded climbing toward total, pending → 0 ⇒ embedding path healthy")
        print("  - ingestion_failures non-empty ⇒ inspect: signature? schema? (see dlq_writer logs)")
        return 0
    finally:
        await pool.close()


def main() -> None:
    raise SystemExit(asyncio.run(_run(_parse_args())))


if __name__ == "__main__":
    main()
