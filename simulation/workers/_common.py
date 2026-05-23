"""Shared worker bootstrap.

Every simulation/workers/*.py CLI runs through `with_context()` to
- import services.synthetic (triggers the production guard),
- open an asyncpg pool + an OllamaClient,
- ensure the scenario actors + identity mappings exist,
- print a human-readable summary of what was emitted.

The bootstrap is deliberately chatty: this tool is run by Rachin from
a terminal and he wants to see what happened without grepping logs.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from contextlib import asynccontextmanager


# Allow `python simulation/workers/X.py` style invocation: prepend the
# repo root to sys.path so the `simulation` package resolves. Calling
# workers via `python -m simulation.workers.X` already works without
# this, but it's the less-typed path and matches the docstrings.
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional
from uuid import UUID, uuid4

import asyncpg

# The env guard fires as a side-effect of importing services.synthetic
# — we import it eagerly so every worker fails at the top of the
# module instead of mid-CLI.
import services.synthetic  # noqa: F401
from lib.embeddings.ollama import OllamaClient
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.db_bootstrap import _register_codecs
from services.synthetic.core import SyntheticSignal, inject

from simulation.personas import load_personas_cached


DEFAULT_TENANT_ENV = "SIMULATION_TENANT_ID"
DEFAULT_RUN_ENV = "SIMULATION_RUN_ID"


@dataclass
class WorkerContext:
    pool: asyncpg.Pool
    tenant_id: UUID
    run_id: str
    actor_repo: ActorRepo
    alias_repo: EntityAliasRepo
    embedder: OllamaClient


def _resolve_tenant_id(explicit: Optional[str]) -> UUID:
    """Pick the tenant UUID for this run.

    Priority: explicit CLI arg > $SIMULATION_TENANT_ID env > deterministic
    fallback. The deterministic fallback is a fixed UUID so repeated
    dev-loop runs accumulate in the same tenant (important for
    reset/inspect workflows).
    """
    if explicit:
        return UUID(explicit)
    env = os.environ.get(DEFAULT_TENANT_ENV)
    if env:
        return UUID(env)
    # Deterministic dev tenant. If two developers ever share a Postgres
    # the collision is fine — reset.py purges by run_id + tenant, and
    # this value is easy to spot in logs.
    return UUID("00000000-0000-7000-8000-000000000dd1")


def _resolve_run_id(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    env = os.environ.get(DEFAULT_RUN_ENV)
    if env:
        return env
    # One ephemeral run per process — workers called back-to-back in a
    # shell get distinct ids unless the caller pins one.
    return f"sim-{uuid4()}"


async def ensure_personas_seeded(
    pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    """INSERT every persona as an actor + actor_identity_mapping rows.

    Idempotent via ON CONFLICT. Safe to call from every worker — the
    no-op cost is negligible and it means a cold machine without
    running the reset script first still produces a valid substrate.
    """
    personas = load_personas_cached()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for p in personas:
                await conn.execute(
                    """
                    INSERT INTO actors
                        (id, tenant_id, type, display_name, email,
                         status, metadata, created_at)
                    VALUES ($1, $2, $3, $4, $5, 'active', $6::jsonb, now())
                    ON CONFLICT (id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        email = EXCLUDED.email
                    """,
                    p.id,
                    tenant_id,
                    "human_internal",
                    p.name,
                    p.email,
                    json.dumps(
                        {
                            "role": p.role,
                            "title": p.title,
                            "synthetic_persona": True,
                        }
                    ),
                )
                refs = [
                    p.slack_ref,
                    p.github_ref,
                    p.email_ref,
                    # calendar / linear share the email ref namespace
                    # — actor_identity_mappings' PK is
                    # (source_channel, source_actor_ref), so aliasing
                    # by email for those surfaces keeps resolution
                    # deterministic without polluting the schema.
                    f"calendar:{p.email}" if p.email else None,
                    f"linear:{p.slack_handle}" if p.slack_handle else None,
                ]
                for ref in refs:
                    if not ref:
                        continue
                    channel, _, external_ref = ref.partition(":")
                    await conn.execute(
                        """
                        INSERT INTO actor_identity_mappings
                            (actor_id, source_channel, source_actor_ref,
                             confidence, created_at)
                        VALUES ($1, $2, $3, 1.0, now())
                        ON CONFLICT (source_channel, source_actor_ref)
                        DO NOTHING
                        """,
                        p.id,
                        channel,
                        external_ref,
                    )


async def _build_pool() -> asyncpg.Pool:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL must be set for simulation workers "
            "(point at the dev/staging Postgres)."
        )
    return await asyncpg.create_pool(
        dsn, min_size=1, max_size=8, init=_register_codecs
    )


@asynccontextmanager
async def with_context(
    tenant_id_arg: Optional[str] = None,
    run_id_arg: Optional[str] = None,
) -> AsyncIterator[WorkerContext]:
    """Manage worker lifecycle: pool, repos, embedder, actor seeding."""
    pool = await _build_pool()
    tenant_id = _resolve_tenant_id(tenant_id_arg)
    run_id = _resolve_run_id(run_id_arg)
    embedder = OllamaClient()
    try:
        await ensure_personas_seeded(pool, tenant_id)
        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        yield WorkerContext(
            pool=pool,
            tenant_id=tenant_id,
            run_id=run_id,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
        )
    finally:
        try:
            await embedder.close()
        except Exception:
            pass
        try:
            await pool.close()
        except Exception:
            pass


async def emit_signal(
    ctx: WorkerContext,
    *,
    source_channel: str,
    source_actor_ref: Optional[str],
    content_text: str,
    content: dict[str, Any],
    occurred_at: Optional[datetime],
    external_id: Optional[str],
    scenario_id: Optional[str] = None,
    entities_hint: Optional[list[dict[str, Any]]] = None,
) -> UUID:
    signal = SyntheticSignal(
        source_channel=source_channel,
        source_actor_ref=source_actor_ref,
        content_text=content_text,
        content=content,
        occurred_at=occurred_at or datetime.now(timezone.utc),
        external_id=external_id,
        entities_hint=entities_hint or [],
        scenario_id=scenario_id,
        run_id=ctx.run_id,
    )
    result = await inject(
        signal,
        ctx.tenant_id,
        pool=ctx.pool,
        actor_repo=ctx.actor_repo,
        alias_repo=ctx.alias_repo,
        embedder=ctx.embedder,
    )
    return result.observation.id


def parse_occurred_at(s: Optional[str]) -> Optional[datetime]:
    """Accept ISO-8601, 'now', '-3h', '+2d' (relative to now)."""
    if not s or s == "now":
        return datetime.now(timezone.utc)
    s = s.strip()
    if s.startswith(("+", "-")) and s[-1] in "smhd":
        sign = 1 if s.startswith("+") else -1
        unit = s[-1]
        try:
            amount = int(s[1:-1])
        except ValueError as exc:
            raise ValueError(f"cannot parse relative time {s!r}") from exc
        seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        from datetime import timedelta

        return datetime.now(timezone.utc) + timedelta(seconds=sign * amount * seconds)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tenant", dest="tenant_id", default=None, help="Tenant UUID (default: $SIMULATION_TENANT_ID)."
    )
    parser.add_argument(
        "--run-id", dest="run_id", default=None, help="Run id for this event. Default: generated."
    )
    parser.add_argument(
        "--scenario", dest="scenario_id", default=None, help="Scenario id tag (stored in content.scenario_id)."
    )
    parser.add_argument(
        "--occurred-at", dest="occurred_at", default="now",
        help="When the event happened (ISO-8601, 'now', or relative like -3h).",
    )


def run(coro) -> Any:
    """Tiny helper so each worker main can stay a single coroutine."""
    return asyncio.run(coro)


def print_emitted(observation_id: UUID, summary: str) -> None:
    sys.stdout.write(f"emitted observation {observation_id} — {summary}\n")
