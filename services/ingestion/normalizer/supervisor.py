"""services/ingestion/normalizer/supervisor.py — process supervisor.

Per ingestion LLD §5.2 and M2 work-order §M2.3.

Spawns N normalizer-worker processes; monitors them; restarts on
crash with a configurable backoff; handles SIGTERM/SIGINT for
graceful shutdown.

Why multiprocessing (not threads):
  - One Kafka consumer per process — librdkafka and aiokafka both
    work cleanly with one consumer per Python process.
  - Crash isolation — a SEGV in one worker doesn't take down the
    whole pool.
  - GIL — the normalize step is CPU-bound enough (orjson + Pydantic
    validation) that threads would serialise under the GIL.

Why `spawn` (not `fork`):
  - Fresh interpreter per child → no inherited asyncpg imports from
    a parent that may have touched the database. Reinforces M2.3's
    Path B contract structurally.
  - macOS forks unsafely after multi-threaded init (the dev team's
    laptops are macOS). `spawn` is the safe portable default.
"""
from __future__ import annotations

import logging
import multiprocessing as mp
import os
import signal
import time
from dataclasses import dataclass

from services.ingestion.normalizer.worker import main as worker_main


log = logging.getLogger(__name__)


# Path B reinforcement (see module docstring): always use `spawn`.
_SPAWN_CONTEXT = "spawn"


@dataclass
class SupervisorConfig:
    """Supervisor settings."""

    num_workers: int = 2
    restart_backoff_seconds: float = 2.0
    # Stop after this many seconds (test mode). Production = None.
    max_runtime_seconds: float | None = None
    # Sleep between liveness checks. Small enough to be responsive,
    # large enough to keep CPU near zero.
    poll_interval_seconds: float = 0.5


def _worker_entry() -> None:
    """Child-process entry. Configures logging then runs the worker.

    A spawn-context child re-imports the world from scratch, so this
    function is the first user code that runs after the interpreter
    boot — log formatting + signal handlers are set inside the worker
    itself.
    """
    logging.basicConfig(
        level=os.environ.get("NORMALIZER_LOG_LEVEL", "INFO"),
        format=(
            "%(asctime)s %(levelname)s %(name)s "
            "[pid=%(process)d] %(message)s"
        ),
    )
    worker_main()


def run_supervisor(config: SupervisorConfig | None = None) -> None:
    """Block until SIGTERM/SIGINT or `max_runtime_seconds` elapses.

    Children that exit non-zero are restarted after
    `restart_backoff_seconds`. On shutdown, sends SIGTERM to each
    child then joins with a 10-second timeout.
    """
    config = config or SupervisorConfig()
    ctx = mp.get_context(_SPAWN_CONTEXT)

    procs: list[mp.Process] = []
    shutdown = False

    def _handle_signal(*_args: object) -> None:
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    def _start_one(label_index: int) -> "mp.Process":
        p = ctx.Process(
            target=_worker_entry,
            name=f"normalizer-{label_index}",
        )
        p.start()
        log.info(
            "normalizer.worker_started",
            extra={"pid": p.pid, "label": p.name},
        )
        return p

    for i in range(config.num_workers):
        procs.append(_start_one(i))

    started_at = time.monotonic()

    try:
        while not shutdown:
            time.sleep(config.poll_interval_seconds)

            for i, p in enumerate(list(procs)):
                if not p.is_alive():
                    log.warning(
                        "normalizer.worker_died",
                        extra={
                            "pid": p.pid,
                            "exitcode": p.exitcode,
                            "label": p.name,
                        },
                    )
                    time.sleep(config.restart_backoff_seconds)
                    procs[i] = _start_one(i)

            if (
                config.max_runtime_seconds is not None
                and time.monotonic() - started_at
                > config.max_runtime_seconds
            ):
                shutdown = True
    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:  # noqa: BLE001
                pass
        for p in procs:
            p.join(timeout=10)


__all__ = ["SupervisorConfig", "run_supervisor"]
