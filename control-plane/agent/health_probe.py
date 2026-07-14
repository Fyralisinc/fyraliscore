"""health_probe — derive a local SLI signal for the heartbeat.

The agent's heartbeat carries a *derived* ``health`` (C4: green|yellow|red). The
console derives health from heartbeat **freshness**; this module supplies the
*local SLI* the agent folds in so a deployment that is still beating but whose
data plane is unhealthy reports ``yellow`` rather than a false ``green``.

The SLI source is the local in-VPC health endpoint (e.g. the data-plane's
``/healthz``). We probe it over the loopback (this is the agent's only *inbound-
to-the-VPC*-but-still-local read; it opens no listener — I2) and map:

* 2xx + body indicating ok  -> healthy (no SLI breach)
* reachable but degraded / non-2xx -> SLI breach (degrade green->yellow)
* unreachable                -> SLI breach (the local plane is down, degrade)

The probe is injectable so the daemon, a real http client, and tests share one
path. ``probe()`` never raises — a probe failure becomes a breach flag, not a
crash.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

__all__ = ["SliResult", "HealthProbe", "http_healthz_probe", "static_probe"]


@dataclass(frozen=True)
class SliResult:
    """Outcome of one local SLI probe."""

    healthy: bool
    detail: str

    @property
    def breached(self) -> bool:
        """True if the local SLI is degraded (caller degrades health)."""
        return not self.healthy


# A probe callable returns an SliResult; it must not raise.
ProbeFn = Callable[[], SliResult]


def http_healthz_probe(url: str, *, timeout_s: float = 2.0) -> ProbeFn:
    """Build a probe that GETs a local ``/healthz`` and classifies the response.

    ``requests`` is imported lazily so importing this module needs no network
    stack; tests inject :func:`static_probe` instead.
    """

    def _probe() -> SliResult:
        try:
            import requests  # lazy

            resp = requests.get(url, timeout=timeout_s)
        except Exception as exc:
            return SliResult(False, f"healthz unreachable: {exc}")

        if 200 <= resp.status_code < 300:
            body = (resp.text or "").strip().lower()
            # Accept common "ok"-ish health bodies; an empty 2xx body is treated
            # as healthy (a bare 200 is the simplest liveness signal).
            if body in ("", "ok", "healthy", "up") or '"status":"ok"' in body.replace(" ", ""):
                return SliResult(True, f"healthz {resp.status_code}")
            # 2xx but body says something else (e.g. {"status":"degraded"}).
            if "degrad" in body or "unhealth" in body or '"status":"down"' in body.replace(" ", ""):
                return SliResult(False, f"healthz degraded: {body[:120]}")
            return SliResult(True, f"healthz {resp.status_code}")
        return SliResult(False, f"healthz HTTP {resp.status_code}")

    return _probe


def static_probe(healthy: bool, detail: str = "static") -> ProbeFn:
    """A fixed probe (tests / a deployment with no local healthz)."""

    def _probe() -> SliResult:
        return SliResult(healthy, detail)

    return _probe


class HealthProbe:
    """Wraps a probe callable, guaranteeing it never raises."""

    def __init__(self, probe: ProbeFn) -> None:
        self._probe = probe

    def probe(self) -> SliResult:
        try:
            return self._probe()
        except Exception as exc:  # defensive: a probe must never crash the loop
            return SliResult(False, f"probe error: {exc}")
