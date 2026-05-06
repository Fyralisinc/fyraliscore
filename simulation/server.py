"""simulation/server.py — FastAPI app for the simulated Slack UI.

Hosts:
- Static files under /simulation/slack_ui/* (served from
  simulation/slack_ui/ on disk).
- POST /simulation/inject — accepts a JSON message from the UI
  composer, builds a SyntheticSignal, and routes it through
  services.synthetic.core.inject().
- GET /simulation/personas — the persona registry.
- GET /simulation/channels — the fixed channel list.
- GET /simulation/messages?channel=... — the last 20 messages in a
  channel. Reads directly from the observations table so it reflects
  what actually landed in the substrate (not a UI-local cache).

Run as standalone (owns its own pool + lifespan):
    COMPANY_OS_ENV=dev DATABASE_URL=... \\
      uvicorn simulation.server:app --port 8765

Run mounted inside the gateway (shares the gateway's pool):
    See `services/gateway/main.py::_configure_ceo_view`, which calls
    `build_sim_router(...)` with the gateway's deps and includes the
    returned APIRouter. `GATEWAY_MOUNT_SIM=1` (default on in dev/test)
    opts in.

Week 5 stabilization note:
    The standalone `app` used to be the only way to run the SIM surface;
    mounting it in-process double-created a pool (the sub-app owned its
    own `_lifespan`). Week 5 splits the routes into `build_sim_router`
    so the gateway can mount them using the gateway's pool without a
    second lifespan. The standalone `app` is preserved via a local app
    factory (`_build_standalone_app`) that wires its own pool + lifespan.
"""
from __future__ import annotations

import json
import os
import pathlib
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# Env guard fires at import.
import services.synthetic  # noqa: F401
from lib.embeddings.ollama import OllamaClient
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.db_bootstrap import _register_codecs
from services.synthetic.core import SyntheticSignal, inject

from simulation.personas import (
    get_persona,
    load_personas_cached,
    voice_hints_for,
)
from simulation.workers._common import (
    _resolve_run_id,
    _resolve_tenant_id,
    ensure_personas_seeded,
)


HERE = pathlib.Path(__file__).parent
STATIC_DIR = HERE / "slack_ui"


# ------------------------------------------------------------------
# Channels — simulated Slack workspace layout.
#
# These are the channels the composer can target. They become
# source_channel="slack:<handle>" via the ingestion bypass. Rachin
# can extend the list by editing this file; no DB-side registration
# is required because the bypass passthrough handler binds channels
# on first use.
# ------------------------------------------------------------------
SIM_CHANNELS: list[dict[str, str]] = [
    {"handle": "leadership", "description": "exec + founder only"},
    {"handle": "eng", "description": "engineering-wide"},
    {"handle": "payments", "description": "payments team"},
    {"handle": "revenue", "description": "sales + CS"},
    {"handle": "customer-acme", "description": "Acme shared channel"},
    {"handle": "design", "description": "design team"},
    {"handle": "random", "description": "watercooler"},
    {"handle": "journal", "description": "Rachin's private journal"},
]


class InjectRequest(BaseModel):
    persona: str = Field(..., description="Persona handle or UUID.")
    channel: str = Field(..., description="Slack channel handle, e.g. 'eng'.")
    content_text: str = Field(..., min_length=1, max_length=8000)
    occurred_at: Optional[str] = Field(
        None, description="ISO-8601 UTC, 'now', or relative like '-3h'."
    )
    scenario_id: Optional[str] = None
    external_id: Optional[str] = None
    tenant_id: Optional[str] = Field(None, description="Override tenant UUID (for demo sessions).")


class InjectResponse(BaseModel):
    observation_id: str
    deduped: bool
    tenant_id: str
    run_id: str
    channel: str
    occurred_at: str


# ---------------------------------------------------------------------
# SimDeps — container for the SIM surface's runtime dependencies.
# Lives on the owning app's state under `app.state.sim_deps` when the
# standalone app is used; when the router is mounted, the caller
# passes an already-built `SimDeps` to `build_sim_router`.
# ---------------------------------------------------------------------


@dataclass
class SimDeps:
    pool: asyncpg.Pool
    tenant_id: UUID
    run_id: str
    embedder: OllamaClient | None
    actor_repo: ActorRepo
    alias_repo: EntityAliasRepo


# ---------------------------------------------------------------------
# Router factory — owns the route bodies, closes over a SimDeps.
# ---------------------------------------------------------------------


def build_sim_router(deps: SimDeps) -> APIRouter:
    """Build a FastAPI APIRouter that implements the SIM surface against
    the supplied `deps`. The returned router has no lifespan of its own;
    the caller is responsible for pool/embedder lifecycle.

    Used by both:
    - the standalone app factory below (owns a fresh pool + lifespan)
    - the gateway's `_configure_ceo_view` (shares the gateway pool)
    """
    router = APIRouter()

    @router.get("/simulation/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "tenant_id": str(deps.tenant_id),
            "run_id": deps.run_id,
            "channel_count": len(SIM_CHANNELS),
            "persona_count": len(load_personas_cached()),
        }

    @router.get("/simulation/personas")
    async def personas_list() -> dict[str, Any]:
        out = []
        for p in load_personas_cached():
            out.append(
                {
                    "id": str(p.id),
                    "name": p.name,
                    "role": p.role,
                    "title": p.title,
                    "slack_handle": p.slack_handle,
                    "github_handle": p.github_handle,
                    "email": p.email,
                    "typical_channels": list(p.typical_channels),
                    "voice_style_notes": p.voice_style_notes,
                    "voice_hints": voice_hints_for(p.id),
                }
            )
        return {"personas": out}

    @router.get("/simulation/channels")
    async def channels_list() -> dict[str, Any]:
        return {"channels": SIM_CHANNELS}

    @router.post("/simulation/inject", response_model=InjectResponse)
    async def do_inject(req: InjectRequest) -> InjectResponse:
        try:
            persona = get_persona(req.persona)
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        channel_handles = {c["handle"] for c in SIM_CHANNELS}
        if req.channel not in channel_handles:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"unknown channel {req.channel!r}; "
                    f"known: {sorted(channel_handles)}"
                ),
            )
        occurred_at = _parse_occurred_at(req.occurred_at)

        source_channel = f"slack:{req.channel}"
        external_id = req.external_id or (
            f"sim-slack-{req.channel}-{persona.slack_handle}-"
            f"{occurred_at.isoformat()}-"
            f"{hash(req.content_text) & 0xFFFFFFFF:08x}"
        )
        entities_hint = _extract_entities_hint(req.content_text)
        signal = SyntheticSignal(
            source_channel=source_channel,
            source_actor_ref=persona.slack_ref,
            content_text=req.content_text,
            content={
                "event_kind": "message",
                "channel_name": req.channel,
                "author_slack_handle": persona.slack_handle,
                "ts": occurred_at.isoformat(),
            },
            occurred_at=occurred_at,
            external_id=external_id,
            entities_hint=entities_hint,
            scenario_id=req.scenario_id,
            run_id=deps.run_id,
        )
        effective_tenant = UUID(req.tenant_id) if req.tenant_id else deps.tenant_id
        result = await inject(
            signal,
            effective_tenant,
            pool=deps.pool,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
        )
        return InjectResponse(
            observation_id=str(result.observation.id),
            deduped=result.deduped,
            tenant_id=str(effective_tenant),
            run_id=deps.run_id,
            channel=source_channel,
            occurred_at=occurred_at.isoformat(),
        )

    @router.get("/simulation/messages")
    async def messages(
        channel: str = Query(..., description="Slack channel handle."),
        limit: int = Query(20, ge=1, le=200),
    ) -> dict[str, Any]:
        source_channel = f"slack:{channel}"
        rows = await deps.pool.fetch(
            """
            SELECT id, occurred_at, source_actor_ref, actor_id,
                   content_text, content
            FROM observations
            WHERE tenant_id = $1
              AND source_channel = $2
              AND kind = 'signal'
              AND content->>'synthetic' = 'true'
            ORDER BY occurred_at DESC
            LIMIT $3
            """,
            deps.tenant_id,
            source_channel,
            limit,
        )
        out = []
        for r in rows:
            content = r["content"]
            if isinstance(content, (bytes, str)):
                try:
                    content = (
                        json.loads(content)
                        if isinstance(content, str)
                        else json.loads(content.decode())
                    )
                except Exception:
                    content = {}
            handle = (
                (content or {}).get("author_slack_handle")
                or (r["source_actor_ref"] or "").split(":", 1)[-1]
            )
            out.append(
                {
                    "observation_id": str(r["id"]),
                    "occurred_at": r["occurred_at"].isoformat(),
                    "author_handle": handle,
                    "author_actor_id": (
                        str(r["actor_id"]) if r["actor_id"] else None
                    ),
                    "content_text": r["content_text"],
                    "scenario_id": (content or {}).get("scenario_id"),
                    "run_id": (content or {}).get("run_id"),
                }
            )
        out.reverse()  # UI likes ascending-time
        return {"channel": channel, "messages": out}

    return router


# ---------------------------------------------------------------------
# Helpers shared by both surfaces.
# ---------------------------------------------------------------------


def _parse_occurred_at(raw: Optional[str]) -> datetime:
    # Imported lazily to avoid a cycle with workers._common at app
    # import time.
    from simulation.workers._common import parse_occurred_at

    dt = parse_occurred_at(raw)
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt


def _extract_entities_hint(text: str) -> list[dict[str, str]]:
    """Lightweight @mention / #channel / link-domain extractor.

    Matches the plan's "lightweight heuristic" for Phase 2 — the
    real entity resolver handles the long tail; this helper just
    surfaces obvious structural references so Think has them in
    content.entities_mentioned from the first ingest.
    """
    import re

    hints: list[dict[str, str]] = []
    for m in re.finditer(r"@([A-Za-z][A-Za-z0-9_\-]{0,30})", text):
        hints.append({"type": "actor", "handle": m.group(1)})
    for m in re.finditer(r"#([A-Za-z][A-Za-z0-9_\-]{0,30})", text):
        hints.append({"type": "channel", "handle": m.group(1)})
    for m in re.finditer(r"https?://([A-Za-z0-9.\-]+)", text):
        hints.append({"type": "link", "domain": m.group(1)})
    return hints


# ---------------------------------------------------------------------
# Standalone app factory — preserves the existing
# `uvicorn simulation.server:app` invocation.
# ---------------------------------------------------------------------


def _build_standalone_app() -> FastAPI:
    """Construct the standalone SIM app: owns its own pool + lifespan,
    then mounts the router returned by `build_sim_router(deps)`.

    Used by module-level `app = _build_standalone_app()` below so the
    existing `uvicorn simulation.server:app` command keeps working.
    """

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError(
                "DATABASE_URL must be set to run the simulation UI"
            )
        pool = await asyncpg.create_pool(
            dsn, min_size=1, max_size=8, init=_register_codecs
        )
        tenant_id = _resolve_tenant_id(None)
        run_id = _resolve_run_id(None)
        embedder = OllamaClient()
        await ensure_personas_seeded(pool, tenant_id)
        deps = SimDeps(
            pool=pool,
            tenant_id=tenant_id,
            run_id=run_id,
            embedder=embedder,
            actor_repo=ActorRepo(pool),
            alias_repo=EntityAliasRepo(pool),
        )
        app.state.sim_deps = deps
        # Attach the router now that deps exist. This is safe because
        # FastAPI re-resolves the route table on first request.
        app.include_router(build_sim_router(deps))
        # Also re-mount the static files last (JSON routes take priority).
        _mount_static_if_present(app)
        try:
            yield
        finally:
            try:
                await embedder.close()
            except Exception:
                pass
            await pool.close()

    app = FastAPI(
        title="Company OS — Simulation Harness",
        version="0.1.0",
        lifespan=_lifespan,
    )
    return app


def _mount_static_if_present(app: FastAPI) -> None:
    """Mount slack_ui static files on `/` if the directory exists.

    Idempotent: mounting twice would raise, so we guard on an existing
    route with name 'slack_ui_static'.
    """
    if not STATIC_DIR.exists():
        return
    # Avoid double-mount when called from both the factory and the test.
    for r in app.routes:
        if getattr(r, "name", None) == "slack_ui_static":
            return
    app.mount(
        "/",
        StaticFiles(directory=str(STATIC_DIR), html=True),
        name="slack_ui_static",
    )


# Module-level app for `uvicorn simulation.server:app`.
app = _build_standalone_app()


__all__ = [
    "SIM_CHANNELS",
    "SimDeps",
    "InjectRequest",
    "InjectResponse",
    "app",
    "build_sim_router",
]
