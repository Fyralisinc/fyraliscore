"""Self-contained runner for WhatsApp LIVE ingestion.

Stands up JUST enough of the stack to see live ingestion working end-to-end:
a Postgres pool + the actor/entity repos + the WhatsApp webhook router — and
nothing else (no Kafka, no S3, no Ollama, no full gateway startup). Inbound
messages flow through the REAL inline `ingest()` path into the `observations`
table, exactly as they would inside the production gateway.

Run:
    export DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os
    export WHATSAPP_VERIFY_TOKEN=my-verify-token          # for Meta's GET handshake
    # optional: export WHATSAPP_ALLOW_UNSIGNED=1          # skip HMAC for local poking
    python scripts/whatsapp_live_server.py                # serves on :8000

Then open  http://localhost:8000/debug/whatsapp  (the live viewer),
register an installation, and POST a (signed) webhook — see scripts/whatsapp_simulate.py.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from services.app.gateway.db_bootstrap import close_gateway_pool, create_gateway_pool
from services.app.gateway.deps import attach_gateway_deps
from services.app.gateway.rate_limit import RateLimiter
from services.app.gateway.whatsapp_router import build_whatsapp_router
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo


_DEFAULT_DSN = "postgresql://company_os:company_os@localhost:5434/company_os"


@asynccontextmanager
async def _lifespan(app: FastAPI):
    dsn = os.environ.get("DATABASE_URL") or _DEFAULT_DSN
    pool = await create_gateway_pool(dsn)
    # embedder=None → ingest() persists with embedding_pending=True (no Ollama needed).
    attach_gateway_deps(
        app,
        pool=pool,
        actor_repo=ActorRepo(pool),
        alias_repo=EntityAliasRepo(pool),
        embedder=None,
        rate_limiter=RateLimiter(),
    )
    _port = os.environ.get("WHATSAPP_PORT", "8000")
    print(f"[whatsapp-live] connected to {dsn}")
    print(f"[whatsapp-live] viewer:   http://localhost:{_port}/debug/whatsapp")
    print(f"[whatsapp-live] webhook:  POST/GET http://localhost:{_port}/integrations/whatsapp/webhook")
    try:
        yield
    finally:
        await close_gateway_pool(pool)


def build_app() -> FastAPI:
    app = FastAPI(title="WhatsApp Live Ingestion", lifespan=_lifespan)
    app.include_router(build_whatsapp_router(debug_endpoints_enabled=True))
    return app


app = build_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.environ.get("WHATSAPP_HOST", "0.0.0.0"),
        port=int(os.environ.get("WHATSAPP_PORT", "8000")),
        log_level="info",
    )
