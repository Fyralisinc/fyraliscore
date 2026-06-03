#!/usr/bin/env python
"""Drain an existing company-scale Think queue with real worker processes.

This is intentionally a thin harness around the production ThinkWorker. It is
used when a large E2E run has already materialized a tenant and enqueued T1
triggers, but the queue needs to be drained with multiple OS processes so the
Codex app-server transport is not serialized behind one process-local lock.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time
from pathlib import Path
from uuid import UUID

import asyncpg
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_1000_signal_model_layer_probe import (  # noqa: E402
    _build_cached_provider,
    _register_codecs,
)
from services.reasoning.think.worker import ThinkWorker, WorkerConfig  # noqa: E402


load_dotenv(REPO_ROOT / ".env", override=False)


async def _pending_count(pool: asyncpg.Pool, tenant_id: UUID) -> int:
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NULL
            """,
            tenant_id,
        )
    return int(value or 0)


async def _stale_locked_workers(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    worker_prefix: str,
    stale_lock_seconds: int,
) -> list[str]:
    if stale_lock_seconds <= 0:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT locked_by
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NULL
              AND locked_by IS NOT NULL
              AND locked_by LIKE $2
              AND locked_at < now() - ($3 || ' seconds')::interval
            GROUP BY locked_by
            ORDER BY locked_by
            """,
            tenant_id,
            f"{worker_prefix}-%",
            str(stale_lock_seconds),
        )
    return [str(row["locked_by"]) for row in rows if row["locked_by"]]


async def _release_worker_locks(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    worker_ids: list[str],
    *,
    stale_lock_seconds: int,
) -> int:
    if not worker_ids:
        return 0
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE think_trigger_queue
            SET locked_by = NULL,
                locked_at = NULL
            WHERE tenant_id = $1
              AND completed_at IS NULL
              AND locked_by = ANY($2::text[])
              AND locked_at < now() - ($3 || ' seconds')::interval
            """,
            tenant_id,
            worker_ids,
            str(stale_lock_seconds),
        )
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


def _worker_index(worker_id: str, worker_prefix: str) -> int | None:
    prefix = f"{worker_prefix}-"
    if not worker_id.startswith(prefix):
        return None
    try:
        return int(worker_id[len(prefix):]) - 1
    except ValueError:
        return None


async def _terminate_process_group(
    child: asyncio.subprocess.Process,
    *,
    timeout_s: float = 5.0,
) -> None:
    if child.returncode is not None:
        return
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(child.wait(), timeout=timeout_s)
        return
    except asyncio.TimeoutError:
        pass
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    await child.wait()


async def _run_worker(args: argparse.Namespace) -> int:
    tenant_id = UUID(args.tenant_id)
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set")

    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=args.pool_max_size,
        init=_register_codecs,
    )
    provider = _build_cached_provider()
    cfg = WorkerConfig.from_env()
    cfg.worker_id = args.worker_id
    cfg.tenant_filter = tenant_id
    cfg.poll_interval_s = args.poll_interval
    cfg.poll_batch = 1
    cfg.max_concurrency_per_tenant = 1
    cfg.backpressure_limit = args.backpressure_limit
    worker = ThinkWorker(pool=pool, config=cfg, llm_provider=provider)

    async def _noop_promote() -> None:
        return None

    worker._promote_reeval_rows = _noop_promote  # type: ignore[assignment]
    task = asyncio.create_task(worker.run())
    try:
        while await _pending_count(pool, tenant_id) > 0:
            await asyncio.sleep(args.poll_interval)
    finally:
        await worker.stop()
        try:
            await asyncio.wait_for(task, timeout=30)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        await pool.close()
    return 0


async def _run_supervisor(args: argparse.Namespace) -> int:
    tenant_id = UUID(args.tenant_id)
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set")

    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=2,
        init=_register_codecs,
    )
    started = time.monotonic()
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("LLM_CACHE_DISABLE", "1")

    async def _spawn_child(index: int) -> asyncio.subprocess.Process:
        worker_id = f"{args.worker_prefix}-{index + 1}"
        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "worker",
            "--tenant-id",
            str(tenant_id),
            "--worker-id",
            worker_id,
            "--pool-max-size",
            str(args.child_pool_max_size),
            "--poll-interval",
            str(args.child_poll_interval),
            "--backpressure-limit",
            str(args.backpressure_limit),
        ]
        return await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(REPO_ROOT),
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )

    children = [await _spawn_child(index) for index in range(args.workers)]

    try:
        last_pending: int | None = None
        deadline = started + args.timeout
        while True:
            stale_workers = await _stale_locked_workers(
                pool,
                tenant_id,
                worker_prefix=args.worker_prefix,
                stale_lock_seconds=args.stale_lock_seconds,
            )
            restarted: list[str] = []
            for worker_id in stale_workers:
                index = _worker_index(worker_id, args.worker_prefix)
                if index is None or index < 0 or index >= len(children):
                    continue
                await _terminate_process_group(children[index])
                unlocked = await _release_worker_locks(
                    pool,
                    tenant_id,
                    [worker_id],
                    stale_lock_seconds=args.stale_lock_seconds,
                )
                children[index] = await _spawn_child(index)
                restarted.append(f"{worker_id}:{unlocked}")
            if restarted:
                print(
                    "stale_worker_restarted "
                    f"workers={','.join(restarted)} "
                    f"stale_lock_seconds={args.stale_lock_seconds}",
                    flush=True,
                )
            for index, child in enumerate(children):
                if child.returncode is not None:
                    print(
                        f"worker_restarting index={index + 1} returncode={child.returncode}",
                        flush=True,
                    )
                    children[index] = await _spawn_child(index)
            pending = await _pending_count(pool, tenant_id)
            if pending != last_pending or args.progress_every <= 0:
                async with pool.acquire() as conn:
                    runs = await conn.fetchrow(
                        """
                        SELECT COUNT(*) FILTER (WHERE status = 'success') AS success,
                               COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                               COUNT(*) AS total
                        FROM think_runs
                        WHERE tenant_id = $1
                        """,
                        tenant_id,
                    )
                elapsed = time.monotonic() - started
                print(
                    "progress "
                    f"pending={pending} "
                    f"runs={int(runs['total'] or 0)} "
                    f"success={int(runs['success'] or 0)} "
                    f"failed={int(runs['failed'] or 0)} "
                    f"elapsed_s={elapsed:.1f}",
                    flush=True,
                )
                last_pending = pending
            if pending == 0:
                return 0
            if time.monotonic() >= deadline:
                print(
                    f"timeout pending={pending} elapsed_s={time.monotonic() - started:.1f}",
                    flush=True,
                )
                return 124
            await asyncio.sleep(args.progress_every)
    finally:
        for child in children:
            if child.returncode is None:
                child.send_signal(signal.SIGTERM)
        await asyncio.gather(*(child.wait() for child in children), return_exceptions=True)
        await pool.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)

    supervisor = sub.add_parser("supervisor")
    supervisor.add_argument("--tenant-id", required=True)
    supervisor.add_argument("--workers", type=int, default=8)
    supervisor.add_argument("--timeout", type=int, default=172800)
    supervisor.add_argument("--progress-every", type=float, default=30.0)
    supervisor.add_argument("--worker-prefix", default=f"company-e2e-drain-{os.getpid()}")
    supervisor.add_argument("--child-pool-max-size", type=int, default=2)
    supervisor.add_argument("--child-poll-interval", type=float, default=0.05)
    supervisor.add_argument("--backpressure-limit", type=int, default=1_000_000)
    supervisor.add_argument("--stale-lock-seconds", type=int, default=0)

    worker = sub.add_parser("worker")
    worker.add_argument("--tenant-id", required=True)
    worker.add_argument("--worker-id", required=True)
    worker.add_argument("--pool-max-size", type=int, default=2)
    worker.add_argument("--poll-interval", type=float, default=0.05)
    worker.add_argument("--backpressure-limit", type=int, default=1_000_000)

    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    if args.mode == "worker":
        return await _run_worker(args)
    return await _run_supervisor(args)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
