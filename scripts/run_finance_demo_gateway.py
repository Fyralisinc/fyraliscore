#!/usr/bin/env python3
"""Isolated finance-console gateway (Mercury + QuickBooks).

A self-contained testing target for the /debug/finance UI. Mounts ONLY the
finance router on :8010 against an ISOLATED throwaway database (`finance_demo`)
on the existing Postgres cluster. No Kafka, no S3, no secrets, and NO changes to
the shared dev stack (the live `company_os` DB and its workers are untouched).

    python scripts/run_finance_demo_gateway.py

Then point a Vite dev server at it:

    cd ui && VITE_GATEWAY_TARGET=http://localhost:8010 npm run dev

Env:
    FINANCE_DEMO_ADMIN_DSN  admin DSN (default localhost:5434/company_os)
    FINANCE_DEMO_DB         throwaway db name (default finance_demo)
    FINANCE_DEMO_PORT       listen port (default 8010)
"""
from __future__ import annotations

import contextlib
import os
import pathlib
import sys
from uuid import UUID

import asyncpg
import uvicorn
from fastapi import FastAPI, Request

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_PORT_STR = os.environ.get("FINANCE_DEMO_PORT", "8010")
# Set BEFORE importing the service modules so anything that reads env at import
# time sees them. Pinning GATEWAY_SELF_PORT to our own port keeps the finance
# router's live/emit self-call local (this minimal app has no /webhooks route,
# so it 404s and falls back to inline ingest) — never touching the :8000 stack.
os.environ.setdefault("GATEWAY_SELF_PORT", _PORT_STR)
os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("FYRALIS_ENV", "test")

from lib.shared.migrations import apply_migrations_dir  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.domain.observations.partitions import ensure_partitions  # noqa: E402
from services.domain.actors.repo import ActorRepo  # noqa: E402
from services.domain.entity_aliases.repo import EntityAliasRepo  # noqa: E402
from services.app.gateway.finance_router import build_finance_router  # noqa: E402

ADMIN_DSN = os.environ.get(
    "FINANCE_DEMO_ADMIN_DSN",
    "postgresql://company_os:company_os@localhost:5434/company_os",
)
DB_NAME = os.environ.get("FINANCE_DEMO_DB", "finance_demo")
PORT = int(_PORT_STR)

# Well-known tenants seeded so the UI works out of the box. The first matches
# the UI's DEFAULT_TENANT_ID; add your own here if you change the tenant field.
SEED_TENANTS = [
    ("00000000-0000-0000-0000-000000000001", "finance-demo"),
    ("00000000-0000-0000-0000-000000000002", "finance-demo-2"),
]


def _demo_dsn() -> str:
    base = ADMIN_DSN.rsplit("/", 1)[0]
    return f"{base}/{DB_NAME}"


async def _ensure_database() -> str:
    """CREATE DATABASE finance_demo if absent; migrate it if finance tables
    are missing. Idempotent — safe to restart."""
    dsn = _demo_dsn()
    admin = await asyncpg.connect(ADMIN_DSN)
    try:
        exists = await admin.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", DB_NAME
        )
        if not exists:
            await admin.execute(f'CREATE DATABASE "{DB_NAME}"')
            print(f"[finance-demo] created isolated database '{DB_NAME}'", flush=True)
        else:
            print(f"[finance-demo] reusing isolated database '{DB_NAME}'", flush=True)
    finally:
        await admin.close()

    conn = await asyncpg.connect(dsn)
    try:
        present = await conn.fetchval(
            "SELECT to_regclass('public.quickbooks_installations')"
        )
        if present is None:
            print("[finance-demo] applying migrations ...", flush=True)
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
            print("[finance-demo] migrations applied", flush=True)
        else:
            print("[finance-demo] schema already present", flush=True)
    finally:
        await conn.close()
    return dsn


class _Deps:
    """The minimal slice of gateway deps the finance router reads."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self.actor_repo = ActorRepo(pool)
        self.alias_repo = EntityAliasRepo(pool)
        self.embedder = None


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI):
    dsn = await _ensure_database()
    pool = await asyncpg.create_pool(
        dsn=dsn, min_size=1, max_size=8, init=_register_codecs
    )
    await ensure_partitions(pool, months_ahead=3)
    async with pool.acquire() as conn:
        for tid, name in SEED_TENANTS:
            await conn.execute(
                "INSERT INTO tenants (id, name) VALUES ($1, $2) "
                "ON CONFLICT DO NOTHING",
                UUID(tid), name,
            )
    app.state.deps = _Deps(pool)
    print(
        f"[finance-demo] READY  http://127.0.0.1:{PORT}  (db={DB_NAME}, "
        f"default tenant={SEED_TENANTS[0][0]})",
        flush=True,
    )
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(title="finance-demo-gateway", lifespan=_lifespan)
app.include_router(build_finance_router())


@app.get("/healthz")
async def healthz() -> dict[str, object]:
    return {"ok": True, "db": DB_NAME}


@app.get("/stats")
@app.get("/debug/stats")
async def stats(request: Request) -> dict[str, object]:
    """Minimal stub so the inspector chrome (DebugLayout) renders cleanly.
    Only `observations` is real (the finance rows in this demo DB)."""
    deps = getattr(request.app.state, "deps", None)
    obs = 0
    if deps is not None:
        try:
            obs = await deps.pool.fetchval("SELECT count(*) FROM observations") or 0
        except Exception:  # noqa: BLE001
            obs = 0
    return {
        "stats": {
            "observations": obs,
            "active_models": 0,
            "commitments": 0,
            "think_runs": 0,
            "trigger_queue_depth": 0,
            "artifacts": 0,
        }
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
