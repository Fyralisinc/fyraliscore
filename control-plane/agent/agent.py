"""agent.py — the outbound-only Fyralis data-plane agent (runs in the customer VPC).

Responsibilities (all invariant-anchored):

* **Heartbeat** (C4): every ``interval_s`` seconds, collect a fresh
  :class:`DeploymentRecord` — version (from the ``VERSION`` file / env), region,
  telemetry tier, license expiry, and a *derived* ``health`` (heartbeat freshness
  folded with a **local SLI probe** of the data-plane ``/healthz``) — and POST it
  to ``<console_url>/api/v1/heartbeat`` over an **outbound https** call.

* **I2 — outbound only**: the agent NEVER opens a listening socket. Its only
  network egress is the outbound POST to the console (and outbound GETs for the
  signed config bundle). There is no server, no bound port, nothing to reach in to.

* **I3 — never crash, never block local ops**: if the console is unreachable the
  heartbeat is parked in a durable local :class:`HeartbeatBuffer` and retried with
  exponential backoff. A network error, a 5xx, a DNS failure — none of these throw
  out of the loop or stall the customer's data plane. On reconnect the buffered
  backlog is flushed oldest-first.

* **I6 — verify before apply**: signed config bundles are pulled and verified by
  :class:`ConfigPuller` (ed25519 against the trust root) *before* being applied; an
  unverified bundle is rejected and the previous config kept.

* **License gate**: the agent validates the local signed license each tick via
  :class:`LicenseChecker` and **refuses its privileged actions** (config pull /
  applying new config) when the license is missing/expired/tampered. It still
  heartbeats so the console can *see* the deployment is unlicensed (the record's
  health goes red because the license is expired — see ``derive_health``).

Run as a daemon: ``python agent.py`` (see ``run.sh`` / ``Dockerfile``). The loop is
plain blocking I/O on a single thread; ``run_forever`` is interruptible (SIGINT /
SIGTERM) and ``tick`` is unit-testable in isolation against a fake console.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)
from lib import DeploymentRecord, TelemetryTier
from lib.primitives import to_rfc3339, utcnow

from buffer import HeartbeatBuffer
from config import AgentConfig, load_agent_config
from config_pull import ConfigPuller
from health_probe import HealthProbe, http_healthz_probe
from license_check import LicenseChecker

LOG = logging.getLogger("fyralis.agent")

__all__ = ["Agent", "HeartbeatSender", "requests_sender", "main"]


# A sender takes the C4 record dict and returns True on accepted delivery,
# False on a (retryable) console-unreachable / rejected condition. Injected so
# the daemon, a real https client, and the test fake-console share one path.
HeartbeatSender = Callable[[dict], bool]


def requests_sender(
    heartbeat_url: str, *, timeout_s: float = 5.0, token: str | None = None
) -> HeartbeatSender:
    """Default OUTBOUND heartbeat sender: POST the record JSON to the console.

    ``requests`` is imported lazily so importing this module needs no network
    stack. When ``token`` is set it is presented as an ``Authorization: Bearer``
    header — the console's write path requires it (I4); without a valid token the
    console answers 401 and this returns ``False`` (the caller buffers + retries).
    Returns ``False`` (retryable) on ANY transport error or non-2xx — the caller
    buffers and retries; it never raises out to the loop.
    """
    import requests  # lazy

    headers = {"Authorization": f"Bearer {token}"} if token else None

    def _send(record: dict) -> bool:
        try:
            resp = requests.post(
                heartbeat_url, json=record, headers=headers, timeout=timeout_s
            )
        except Exception as exc:  # connection refused, DNS, timeout, TLS, ...
            LOG.warning("heartbeat POST failed (transport): %s", exc)
            return False
        if 200 <= resp.status_code < 300:
            return True
        if resp.status_code in (401, 403):
            LOG.warning(
                "heartbeat POST rejected: HTTP %s — console write token "
                "missing/invalid (check AGENT_CONSOLE_TOKEN)",
                resp.status_code,
            )
            return False
        LOG.warning("heartbeat POST rejected: HTTP %s", resp.status_code)
        return False

    return _send


@dataclass
class TickResult:
    """What one ``tick`` did (for tests / logging)."""

    record: dict
    delivered: bool
    buffered: bool
    flushed: int
    licensed: bool
    sli_breached: bool


@dataclass
class Agent:
    """The agent daemon. Construct with a config; inject sender/probe for tests."""

    config: AgentConfig
    sender: HeartbeatSender | None = None
    probe: HealthProbe | None = None
    license_checker: LicenseChecker | None = None
    config_puller: ConfigPuller | None = None
    buffer: HeartbeatBuffer | None = None

    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _backoff_s: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        cfg = self.config
        if self.sender is None:
            self.sender = requests_sender(
                cfg.heartbeat_url,
                timeout_s=cfg.heartbeat_timeout_s,
                token=cfg.console_token,
            )
        if self.probe is None:
            self.probe = HealthProbe(
                http_healthz_probe(cfg.healthz_url, timeout_s=cfg.healthz_timeout_s)
            )
        if self.license_checker is None:
            self.license_checker = LicenseChecker(
                cfg.license_path, trust_root_path=str(cfg.trust_root_path)
            )
        if self.config_puller is None:
            self.config_puller = ConfigPuller(
                config_dir=cfg.config_dir, trust_root_path=str(cfg.trust_root_path)
            )
        if self.buffer is None:
            self.buffer = HeartbeatBuffer(
                cfg.buffer_path, max_records=cfg.buffer_max_records
            )
        self._backoff_s = cfg.backoff_base_s

    # ------------------------------------------------------------------ #
    # Collection: build a fresh DeploymentRecord                          #
    # ------------------------------------------------------------------ #

    def read_version(self) -> str:
        """Version from the VERSION file, falling back to env, then 'unknown'."""
        vf = self.config.version_file
        try:
            if vf.is_file():
                txt = vf.read_text(encoding="utf-8").strip()
                if txt:
                    return txt.splitlines()[0].strip()
        except OSError as exc:
            LOG.warning("could not read VERSION file %s: %s", vf, exc)
        if self.config.version_env:
            return self.config.version_env
        return "unknown"

    def is_licensed(self) -> bool:
        """I6/license gate: does the local signed license verify + remain valid?"""
        return self.license_checker.is_licensed()

    def collect(self, *, now=None) -> tuple[DeploymentRecord, bool]:
        """Build the C4 :class:`DeploymentRecord` for this tick.

        Returns ``(record, sli_breached)``. ``health`` is derived from heartbeat
        freshness (always fresh at mint), the local SLI probe (degrades to
        yellow on breach), and the license expiry (forces red when expired) —
        all inside :meth:`DeploymentRecord.heartbeat`.

        The license expiry that stamps the record is taken from the *verified*
        license when available; if the license is missing/unverifiable we stamp a
        past expiry so the console sees the deployment as unlicensed (red), never
        a fake healthy record.
        """
        now = now or utcnow()
        sli = self.probe.probe()

        status = self.license_checker.evaluate(now=now)
        if status.expires_at is not None:
            license_expiry = status.expires_at
        else:
            # No verifiable license -> advertise an already-passed expiry so the
            # derived health is red (an unlicensed deployment is not healthy).
            license_expiry = now

        record = DeploymentRecord.heartbeat(
            tenant_id=self.config.tenant_id,
            deployment_id=self.config.deployment_id,
            version=self.read_version(),
            region=self.config.region,
            license_expiry=license_expiry,
            telemetry_tier=self.config.telemetry_tier,
            now=now,
            sli_breached=sli.breached,
        )
        return record, sli.breached

    # ------------------------------------------------------------------ #
    # Delivery: send + buffer + flush (I3)                                #
    # ------------------------------------------------------------------ #

    def _try_send(self, record_dict: dict) -> bool:
        try:
            return bool(self.sender(record_dict))
        except Exception as exc:  # a sender must not crash the loop (I3)
            LOG.warning("heartbeat sender raised (treated as undelivered): %s", exc)
            return False

    def deliver(self, record: DeploymentRecord) -> tuple[bool, bool, int]:
        """Flush any backlog, then deliver this heartbeat.

        Returns ``(delivered, buffered, flushed_count)``.

        I3: on console-unreachable the record is appended to the durable buffer
        and the method returns cleanly (never raises, never blocks). On reconnect
        the buffered backlog is flushed oldest-first *before* the new record so
        the console replays history in order.
        """
        flushed = 0
        # 1. Try to drain the backlog first (oldest-first), if any.
        if not self.buffer.is_empty():
            flushed, _remaining = self.buffer.flush(self._try_send)

        record_dict = record.to_registry_dict()

        # 2. If the backlog still has entries, the console is down — buffer this one too.
        if not self.buffer.is_empty():
            evicted = self.buffer.append(record_dict)
            if evicted:
                LOG.warning("heartbeat buffer full — dropped oldest record")
            return False, True, flushed

        # 3. Backlog clear: attempt the live send.
        if self._try_send(record_dict):
            return True, False, flushed

        # 4. Live send failed — park it.
        evicted = self.buffer.append(record_dict)
        if evicted:
            LOG.warning("heartbeat buffer full — dropped oldest record")
        return False, True, flushed

    # ------------------------------------------------------------------ #
    # Config pull (I6) — a privileged action gated by the license        #
    # ------------------------------------------------------------------ #

    def pull_config(self, config_url: str) -> bool:
        """Pull + verify + apply a signed config bundle (I6). License-gated.

        Refuses (no-op, returns False) when unlicensed — applying new config is a
        privileged action. Never raises; a pull/verify failure keeps the old config.
        """
        if not self.is_licensed():
            LOG.warning("refusing config pull: deployment is unlicensed/expired")
            return False
        try:
            res = self.config_puller.pull_and_apply(config_url)
        except Exception as exc:  # config pull must never crash the daemon
            LOG.warning("config pull raised (ignored): %s", exc)
            return False
        if res.ok:
            LOG.info("config applied: %s", res.reason)
        else:
            LOG.warning("config not applied: %s", res.reason)
        return res.ok

    # ------------------------------------------------------------------ #
    # The loop                                                            #
    # ------------------------------------------------------------------ #

    def tick(self, *, now=None) -> TickResult:
        """One heartbeat cycle: collect -> deliver. Adjusts backoff. Never raises."""
        record, sli_breached = self.collect(now=now)
        licensed = self.is_licensed()
        delivered, buffered, flushed = self.deliver(record)

        if delivered:
            self._backoff_s = self.config.backoff_base_s  # reset on success
        else:
            self._backoff_s = min(
                self._backoff_s * 2 if self._backoff_s else self.config.backoff_base_s,
                self.config.backoff_max_s,
            )

        LOG.info(
            "tick health=%s licensed=%s sli_breached=%s delivered=%s buffered=%s "
            "flushed=%d backlog=%d",
            record.health.value,
            licensed,
            sli_breached,
            delivered,
            buffered,
            flushed,
            self.buffer.count(),
        )
        return TickResult(
            record=record.to_registry_dict(),
            delivered=delivered,
            buffered=buffered,
            flushed=flushed,
            licensed=licensed,
            sli_breached=sli_breached,
        )

    def next_sleep_s(self) -> float:
        """How long to sleep before the next tick.

        Steady state: ``interval_s``. After a failed delivery: the current
        exponential backoff (capped), so we retry the unreachable console sooner
        than a full interval but never hot-loop.
        """
        if not self.buffer.is_empty():
            return min(self._backoff_s, self.config.interval_s)
        return self.config.interval_s

    def request_stop(self, *_signal_args) -> None:
        """Signal the loop to exit after the current tick (SIGINT/SIGTERM)."""
        LOG.info("stop requested — agent will exit after current tick")
        self._stop.set()

    def run_forever(self, *, max_ticks: int | None = None) -> int:
        """Run the heartbeat loop until stopped. Returns the number of ticks run.

        ``max_ticks`` bounds the loop (used by tests). The loop catches and logs
        any unexpected exception per tick so a single bad tick can never take the
        daemon down (I3).
        """
        ticks = 0
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:  # absolute backstop — keep the daemon alive
                LOG.error("unexpected error in tick (continuing): %s", exc, exc_info=True)
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                break
            # Interruptible sleep: wakes immediately on stop.
            self._stop.wait(self.next_sleep_s())
        LOG.info("agent loop exited after %d ticks", ticks)
        return ticks


def _configure_logging() -> None:
    level = os.environ.get("AGENT_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def main(argv: list[str] | None = None) -> int:
    """Entrypoint: load config from the environment and run the loop forever."""
    _configure_logging()
    cfg = load_agent_config()
    agent = Agent(cfg)

    # I2 reassurance in the log: we are outbound-only.
    LOG.info(
        "fyralis agent starting: deployment=%s tenant=%s region=%s tier=%s "
        "console=%s interval=%ss (OUTBOUND-ONLY, no listener)",
        cfg.deployment_id,
        cfg.tenant_id,
        cfg.region,
        cfg.telemetry_tier.value,
        cfg.console_url,
        cfg.interval_s,
    )
    status = agent.license_checker.evaluate()
    LOG.info("license: %s", status.reason)

    signal.signal(signal.SIGINT, agent.request_stop)
    signal.signal(signal.SIGTERM, agent.request_stop)

    agent.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
