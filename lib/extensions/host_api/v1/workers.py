"""lib.extensions.host_api.v1.workers — the background-worker contribution type.

The SemVer-pinned contract an extension binds to when it contributes a
long-running background worker through the ``company_os.workers`` entry-point
group. This generalizes the hardcoded ``docker-compose`` ``command: python -m …``
worker-launch model: instead of the host owning every worker, an installed
extension *declares* a worker and the host's supervisor
(:mod:`lib.extensions.run_workers`) discovers and runs it — the same inversion as
the draft-enricher / gateway-extension seams (core discovers; it never imports).

Contract for ``run``::

    async def run(*, pool: asyncpg.Pool, shutdown: asyncio.Event) -> None

Two scheduling shapes (``mode``):

* ``"interval"`` (default) — ``run`` does **one bounded pass** (sweep the tenants
  it owns, drain its queue, return). The supervisor calls it on a loop every
  ``interval_s`` seconds and swallows exceptions, so one failed pass never kills
  the worker or its siblings.
* ``"forever"`` — ``run`` owns its **own** loop until ``shutdown`` is set (for a
  worker that blocks on a stream/socket). The supervisor calls it once.

Either way the worker MUST honour ``shutdown`` for clean SIGTERM handling and MUST
enforce its own per-tenant enablement (the host gates only host-API
compatibility + that the owning manifest is active; the worker is trusted to
respect each tenant's feature flag / grant, exactly as the inline enricher does).

Pure stdlib + typing only, so it sits safely under the ``lib`` ↛ ``services``
import-linter floor.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

# A background worker entry point. ``pool`` is a live asyncpg pool; ``shutdown``
# is the shared stop Event the supervisor sets on SIGTERM/SIGINT. The callable
# must be a coroutine function accepting these two keyword arguments.
WorkerRunFn = Callable[..., Awaitable[None]]

WORKER_MODES = ("interval", "forever")


@dataclass(frozen=True)
class BackgroundWorker:
    """One background worker contributed by an extension.

    The unit an extension exposes through the ``company_os.workers`` entry-point
    group: the group's entry point resolves to a ``BackgroundWorker`` (or a list
    of them, or a zero-arg callable returning either).

    * ``name`` — stable identifier, used in logs/metrics labels.
    * ``run`` — the work callable (see module docstring for its signature).
    * ``manifest_id`` — the owning :class:`~lib.extensions.manifest.ExtensionManifest`
      id, so the supervisor can gate the worker on that manifest being host-API
      compatible / active. ``None`` means an in-repo (first-party, ungated) worker.
    * ``interval_s`` — seconds between passes in ``"interval"`` mode.
    * ``mode`` — ``"interval"`` or ``"forever"`` (see module docstring).
    """

    name: str
    run: WorkerRunFn
    manifest_id: str | None = None
    interval_s: float = 60.0
    mode: str = "interval"

    def __post_init__(self) -> None:
        if self.mode not in WORKER_MODES:
            raise ValueError(
                f"BackgroundWorker.mode must be one of {WORKER_MODES}, "
                f"got {self.mode!r}"
            )
        # interval_s <= 0 would make the supervisor's interval loop spin with no
        # pacing (a CPU/DB busy-loop). Reject at construction (discovery time).
        if self.mode == "interval" and self.interval_s <= 0:
            raise ValueError(
                f"BackgroundWorker.interval_s must be > 0 for interval mode, "
                f"got {self.interval_s!r}"
            )


__all__ = ["BackgroundWorker", "WorkerRunFn", "WORKER_MODES"]
