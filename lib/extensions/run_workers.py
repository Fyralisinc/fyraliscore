"""lib.extensions.run_workers — discover + supervise extension background workers.

The host-side counterpart to the ``company_os.workers`` contribution point. An
installed extension declares a :class:`~lib.extensions.host_api.v1.BackgroundWorker`
(see that module); this supervisor discovers every such worker, gates it on its
owning manifest being host-API compatible, and runs it under one process with a
shared shutdown signal and a single ``/healthz`` + ``/metrics`` endpoint — the
generic replacement for hardcoding a ``command:`` per worker in compose.

Run it as the container command::

    python -m lib.extensions.run_workers

Discovery mirrors :mod:`lib.extensions.manifest` exactly (cached once per
process, failure-isolated — a bad extension can never break startup). The
supervisor mirrors the core sweepers' ``run_forever`` prime directive: a worker
that raises is logged and retried next cycle; it can never take down siblings or
the process.

Import floor: this module lives under ``lib.extensions`` and therefore must not
import ``services`` (import-linter contract "extension host API hides
internals"). All plumbing is built from ``lib.*`` primitives + the stdlib +
asyncpg; in particular it does NOT register the app's asyncpg codecs (those live
under ``services``) — contributed workers handle JSON columns explicitly, the
same way the core ingestion workers do on the data path.

Env:
  * ``DATABASE_URL`` (required) — the asyncpg DSN.
  * ``INGESTION_HEALTH_PORT`` — enables ``/healthz`` + ``/metrics`` (shared with
    every other worker container via the compose ``x-app-env`` anchor).
  * ``EXTENSION_WORKERS_ONCE`` — run exactly one pass of each worker, then exit
    (used by the e2e demo + tests).
  * ``EXTENSION_WORKERS_POOL_MAX`` — pool max_size (default 4).
"""
from __future__ import annotations

import asyncio
import importlib.metadata as importlib_metadata
import logging
import os
import signal

import asyncpg

from lib.extensions.host_api.v1 import BackgroundWorker
from lib.extensions.registry import active_manifests
from lib.observability.health import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from lib.observability.metrics import counter, render_default
from lib.observability.pools import register_pool, unregister_pool

log = logging.getLogger("extensions.run_workers")

_ENTRY_POINT_GROUP = "company_os.workers"
_POOL_NAME = "extension_workers"

# Bounded-cardinality metrics (worker name is an extension-controlled enum;
# acceptable as a label per the observability cardinality rule).
_passes = counter(
    "extension_worker_passes_total",
    "Background-worker passes the supervisor has run.",
    ("worker", "outcome"),
)

_discovered: list[BackgroundWorker] | None = None


def discover_workers() -> list[BackgroundWorker]:
    """Resolve every contributed background worker once per process (cached).

    Each ``company_os.workers`` entry point resolves to a ``BackgroundWorker``,
    a list/tuple of them, or a zero-arg callable returning either. Failure is
    isolated: a discovery error or one bad entry point is logged and skipped, it
    never raises (a broken extension must not stop the supervisor booting).
    """
    global _discovered
    if _discovered is not None:
        return _discovered
    found: list[BackgroundWorker] = []
    try:
        entry_points = importlib_metadata.entry_points(group=_ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - discovery must never block startup
        log.warning("background_worker_discovery_failed", exc_info=True)
        _discovered = found
        return found
    for ep in entry_points:
        try:
            obj = ep.load()
            resolved = obj() if callable(obj) and not isinstance(obj, BackgroundWorker) else obj
            items = resolved if isinstance(resolved, (list, tuple)) else [resolved]
            for it in items:
                if not isinstance(it, BackgroundWorker):
                    log.error("background_worker_bad_type source=%s", ep.name)
                    continue
                found.append(it)
                log.info(
                    "background_worker_discovered source=%s name=%s manifest=%s mode=%s",
                    ep.name, it.name, it.manifest_id, it.mode,
                )
        except Exception:  # noqa: BLE001 - one bad extension must not break others
            log.error("background_worker_load_failed source=%s", ep.name, exc_info=True)
    _discovered = found
    return found


def active_workers() -> list[BackgroundWorker]:
    """Discovered workers whose owning manifest is host-API compatible.

    A worker with ``manifest_id=None`` (in-repo first-party) is always included.
    A worker whose ``manifest_id`` is not in the active manifest set is skipped
    and logged — the SemVer-pin discipline (ADR-0004 §A.4) applied to workers.
    """
    active_ids = {m.id for m in active_manifests()}
    out: list[BackgroundWorker] = []
    for w in discover_workers():
        if w.manifest_id is None or w.manifest_id in active_ids:
            out.append(w)
        else:
            log.warning(
                "background_worker_skipped name=%s manifest=%s reason=manifest_inactive_or_incompatible",
                w.name, w.manifest_id,
            )
    return out


async def _supervise(
    worker: BackgroundWorker,
    pool: asyncpg.Pool,
    shutdown: asyncio.Event,
    heartbeat: Heartbeat,
    *,
    once: bool,
) -> None:
    """Run one worker forever (or once), swallowing per-pass errors.

    The prime directive (mirrors topology_sweeper.run_forever): a pass that
    raises is logged and the loop continues/retries, so one worker's failure can
    never kill its siblings or the process. A ``"forever"`` worker owns its own
    loop and is expected to return only when ``shutdown`` is set; if it instead
    *crashes*, it is re-invoked after a backoff rather than silently abandoned
    (which would leave the worker dead while ``/healthz`` stays green).
    """
    # A "forever" worker has no bounded single pass, so it can't be run "once".
    if once and worker.mode == "forever":
        log.info("background_worker_skipped name=%s reason=forever_in_once_mode", worker.name)
        return

    backoff = 1.0
    while not shutdown.is_set():
        heartbeat.touch()
        crashed = False
        try:
            await worker.run(pool=pool, shutdown=shutdown)
            _passes.inc(worker=worker.name, outcome="ok")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - prime directive: never kill siblings
            crashed = True
            _passes.inc(worker=worker.name, outcome="error")
            log.exception("background_worker_pass_failed name=%s", worker.name)
        if once:
            return
        if worker.mode == "forever":
            # Clean return ⇒ it honored shutdown ⇒ done. A crash ⇒ re-invoke
            # after an (interruptible) exponential backoff.
            if not crashed or shutdown.is_set():
                return
            delay = backoff
            backoff = min(backoff * 2, 60.0)
        else:
            delay = worker.interval_s
            backoff = 1.0
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=max(delay, 0.1))
        except asyncio.TimeoutError:
            pass


def _install_signal_handlers(shutdown: asyncio.Event) -> None:
    """Bind SIGTERM/SIGINT to set the shutdown event (inline; lib can't import
    the services-side worker_observability helper)."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except (NotImplementedError, RuntimeError):  # non-unix / no main loop
            pass


async def _run() -> None:
    dsn = os.environ["DATABASE_URL"]
    once = os.environ.get("EXTENSION_WORKERS_ONCE", "").strip().lower() in {
        "1", "true", "yes", "on", "y",
    }
    pool_max = int(os.environ.get("EXTENSION_WORKERS_POOL_MAX", "4"))

    workers = active_workers()
    log.info("extension_workers.starting count=%d once=%s", len(workers), once)
    if not workers and once:
        log.info("extension_workers.no_workers_once_exit")
        return

    shutdown = asyncio.Event()
    _install_signal_handlers(shutdown)
    heartbeat = Heartbeat()
    # Acquire pool/health/ticker INSIDE the try so a failure of any one of them
    # (e.g. health-server port already bound) still closes what was created.
    pool: asyncpg.Pool | None = None
    health = None
    ticker = None
    try:
        pool = await asyncpg.create_pool(
            dsn=dsn, min_size=1, max_size=max(2, pool_max), statement_cache_size=0
        )
        register_pool(_POOL_NAME, pool)
        health = start_health_server(
            worker_name=_POOL_NAME, render_metrics=render_default, heartbeat=heartbeat
        )
        ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, shutdown))
        if not workers:
            # Nothing to supervise: idle until shutdown so the container does
            # not crash-loop (an extension may be installed later + restarted).
            await shutdown.wait()
            return
        await asyncio.gather(
            *(
                _supervise(w, pool, shutdown, heartbeat, once=once)
                for w in workers
            )
        )
    finally:
        shutdown.set()
        if ticker is not None:
            ticker.cancel()
            try:
                await ticker
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if health is not None:
            health.shutdown()
        if pool is not None:
            unregister_pool(_POOL_NAME)
            await pool.close()
        log.info("extension_workers.stopped")


def reset_for_tests() -> None:
    """Force re-discovery (test isolation only)."""
    global _discovered
    _discovered = None


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run())


if __name__ == "__main__":
    main()
