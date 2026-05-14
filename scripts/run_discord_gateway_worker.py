"""Launcher for services.integrations.discord.gateway.worker — one process.

Mirrors the shape of `scripts/run_think_worker.py` and
`scripts/run_post_commit_worker.py`. Loads env, builds deps, runs the
worker until SIGTERM / SIGINT or fatal Discord close.

Exit codes:
  0 = clean shutdown
  1 = fatal Discord close (auth/intents misconfigured)
  2 = configuration error at startup
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import time

import asyncpg
import structlog

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.embeddings.ollama import OllamaClient  # noqa: E402
from lib.shared.secrets import build_secret_store  # noqa: E402
from services.actors.repo import ActorRepo  # noqa: E402
from services.entity_aliases.repo import EntityAliasRepo  # noqa: E402
from services.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.integrations.discord.gateway.dispatch import DispatchDeps  # noqa: E402
from services.integrations.discord.gateway.worker import GatewayWorker  # noqa: E402
from services.webhooks.tenant_resolver import (  # noqa: E402
    InstallationCache,
    TenantResolverDeps,
    build_tenant_resolver,
    default_metrics,
)


async def _main() -> int:
    log = structlog.get_logger("scripts.run_discord_gateway_worker")
    try:
        dsn = os.environ["DATABASE_URL"]
        bot_token = os.environ["DISCORD_BOT_TOKEN"]
    except KeyError as exc:
        log.error("discord_gateway_missing_env", var=str(exc))
        return 2

    application_id = os.environ.get("DISCORD_CLIENT_ID")

    pool = await asyncpg.create_pool(
        dsn=dsn, min_size=2, max_size=4, init=_register_codecs,
    )
    try:
        secret_store = build_secret_store(pool)
        # Reuse the same resolver shape the HTTP gateway uses (IN-07).
        tenant_resolver = build_tenant_resolver(
            TenantResolverDeps(
                pool=pool,
                cache=InstallationCache(),
                clock=time.monotonic,
                metrics=default_metrics(),
            )
        )
        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        try:
            embedder = OllamaClient()
        except Exception:  # noqa: BLE001
            embedder = None

        deps = DispatchDeps(
            pool=pool,
            tenant_resolver=tenant_resolver,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            application_id=application_id,
        )
        worker = GatewayWorker(bot_token=bot_token, deps=deps)
        log.info(
            "discord_gateway_worker_booting",
            has_application_id=bool(application_id),
        )
        return await worker.run_forever()
    finally:
        await pool.close()
        if "secret_store" in locals() and hasattr(secret_store, "aclose"):
            try:
                await secret_store.aclose()
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
