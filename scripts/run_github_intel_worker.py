#!/usr/bin/env python
"""scripts/run_github_intel_worker.py — GitHub Intelligence ordered worker.

Long-running loop: for each tenant with `github_intel.enabled`, sweep new
github:webhook observations into the queue and drain them (authoritative FSM
state + github_signal_enrichment + code reindex triggers). Also drains the
code_intel reindex triggers when CODE_INTEL_REINDEX_ROOT is set (dogfood).

Env:
  DATABASE_URL                  Postgres DSN
  GITHUB_INTEL_TENANTS          comma-separated tenant UUIDs (or COMPANY_OS_TENANT_ID)
  GITHUB_INTEL_POLL_SECONDS     loop interval (default 3.0)
  CODE_INTEL_REINDEX_ROOT       optional local working-copy path for reindex
"""
from __future__ import annotations

import asyncio
import os
import signal
from uuid import UUID

from services.ingestion.workflows.runtime import make_workflow_pool
from services.github_intel.config import GITHUB_INTEL_ENABLED
from services.github_intel.worker import drain, enqueue_new_github_observations


def _tenants() -> list[UUID]:
    raw = os.environ.get("GITHUB_INTEL_TENANTS") or os.environ.get("COMPANY_OS_TENANT_ID", "")
    return [UUID(t.strip()) for t in raw.split(",") if t.strip()]


async def main() -> None:
    dsn = os.environ["DATABASE_URL"]
    poll = float(os.environ.get("GITHUB_INTEL_POLL_SECONDS", "3.0"))
    reindex_root = os.environ.get("CODE_INTEL_REINDEX_ROOT")
    pool = await make_workflow_pool(dsn, min_size=1, max_size=5)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    from services.ingestion.feature_flags.client import TenantFlags
    flags = TenantFlags(pool)
    try:
        while not stop.is_set():
            for tenant in _tenants():
                if not await flags.get_bool(tenant, GITHUB_INTEL_ENABLED, default=False):
                    continue
                await enqueue_new_github_observations(pool, tenant)
                await drain(pool, tenant, worker_id="github_intel_worker")
                if reindex_root:
                    from services.code_intel.reindex import drain_reindex_triggers
                    await drain_reindex_triggers(pool, tenant, root_path=reindex_root)
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll)
            except asyncio.TimeoutError:
                pass
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
