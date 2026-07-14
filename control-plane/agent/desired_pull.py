"""desired_pull — best-effort OUTBOUND pull of the operator's DesiredState (I2/I3).

The agent reaches OUT to ``GET {console}/api/v1/deployments/{deployment_id}/desired``
with its console token. On a 404 (no desired state written) or ANY transport/HTTP
error, the pull returns ``None`` and the agent simply SKIPS reconcile this tick and
heartbeats exactly as before (I3: the data plane never depends on the console being
up, and a missing desired state is the normal steady state). It NEVER raises out to
the loop and NEVER opens a listening socket (I2 — outbound GET only).

The fetcher is injected so the daemon, a real https client, and the test-suite share
one code path; the default does a lazy ``requests.get``.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib)
from lib.desired_state import DesiredState

LOG = logging.getLogger("fyralis.agent.desired")

__all__ = ["DesiredPuller", "DesiredFetcher", "http_desired_fetcher"]

# A fetcher takes the desired URL + optional bearer token and returns the parsed
# desired-state dict, or None on 404 / not-found. It raises on transport error.
DesiredFetcher = Callable[[str, Optional[str]], Optional[dict]]


def http_desired_fetcher(timeout_s: float = 5.0) -> DesiredFetcher:
    """Default OUTBOUND fetcher: GET the desired-state JSON over https.

    ``requests`` is imported lazily so importing this module needs no network
    stack. Returns ``None`` on 404 (no desired state); raises on transport error
    or any other non-2xx (the puller treats both raise and None as "skip reconcile").
    """
    import requests  # lazy

    def _fetch(desired_url: str, token: Optional[str]) -> Optional[dict]:
        headers = {"Authorization": f"Bearer {token}"} if token else None
        resp = requests.get(desired_url, headers=headers, timeout=timeout_s)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    return _fetch


class DesiredPuller:
    """Pulls the operator's :class:`DesiredState` for this deployment (best-effort)."""

    def __init__(
        self,
        *,
        console_url: str,
        deployment_id: str,
        token: Optional[str] = None,
        fetcher: Optional[DesiredFetcher] = None,
        timeout_s: float = 5.0,
    ) -> None:
        self.console_url = console_url.rstrip("/")
        self.deployment_id = deployment_id
        self.token = token
        self._fetcher = fetcher or http_desired_fetcher(timeout_s=timeout_s)

    @property
    def desired_url(self) -> str:
        return f"{self.console_url}/api/v1/deployments/{self.deployment_id}/desired"

    def pull(self) -> Optional[DesiredState]:
        """Pull + parse the DesiredState. Returns ``None`` (skip reconcile) on a
        404, a transport error, or an unparseable body — NEVER raises (I3)."""
        try:
            raw = self._fetcher(self.desired_url, self.token)
        except Exception as exc:  # transport / non-2xx — skip reconcile
            LOG.debug("desired pull failed (skipping reconcile): %s", exc)
            return None
        if raw is None:
            return None
        try:
            return DesiredState.from_dict(raw)
        except Exception as exc:
            LOG.warning("desired state did not parse (skipping reconcile): %s", exc)
            return None
