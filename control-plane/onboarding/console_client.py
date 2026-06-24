#!/usr/bin/env python3
"""console_client.py — thin HTTP client for the P4 console REST contract.

The console (built by WS-CONSOLE, sibling P4 area) exposes the shared P4 REST API
on ``cp-net`` port 8080. Onboarding only ever *talks to* it over HTTP; it never
imports the console package (write-disjoint). The contract this client speaks
(P4 SHARED CONTRACTS):

    POST /api/v1/register      {tenant_id?, region, plan}      -> {tenant_id, deployment_id}
    POST /api/v1/heartbeat     {DeploymentRecord JSON}         -> upsert + recompute health
    GET  /api/v1/deployments                                   -> [DeploymentRecord (derived health)]
    GET  /api/v1/deployments/{deployment_id}                   -> DeploymentRecord | 404
    GET  /                                                     -> minimal HTML fleet rollup

Two transports are supported behind one interface so the same onboarding code can
run against (a) a *real* console reachable over the network and (b) an *in-process*
fake console (``fake_console.build_app()``) during the self-test, with no server
process to manage:

  * :class:`HttpConsoleClient`     — real network calls via ``httpx`` to a base URL.
  * :class:`ASGIConsoleClient`     — same calls dispatched in-process to a FastAPI
                                     app via ``httpx.ASGITransport`` (no socket).

Both raise :class:`ConsoleError` (a ``ControlPlaneError`` subclass) on a transport
failure or a non-2xx response so the onboarding transaction can treat a console
problem as a *step failure* and roll back.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Optional

# Make ``control-plane/lib`` importable when run from anywhere.
_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
if _CP_ROOT not in sys.path:
    sys.path.insert(0, _CP_ROOT)

import httpx  # noqa: E402

from lib.errors import ControlPlaneError  # noqa: E402

__all__ = [
    "ConsoleError",
    "ConsoleClient",
    "HttpConsoleClient",
    "ASGIConsoleClient",
    "make_console_client",
]


class ConsoleError(ControlPlaneError):
    """The console was unreachable or returned a non-2xx status.

    Carries the HTTP ``status`` (``None`` for a transport error) and the response
    ``body`` so the caller can log a precise reason and decide to roll back.
    """

    def __init__(self, message: str, *, status: Optional[int] = None, body: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class ConsoleClient:
    """Common request/parse logic over an ``httpx.Client`` (sync).

    Subclasses only differ in how the underlying ``httpx.Client`` is constructed
    (real base-url transport vs. in-process ASGI transport).
    """

    def __init__(self, client: httpx.Client, *, label: str) -> None:
        self._client = client
        self._label = label

    # --- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ConsoleClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # --- low-level ---------------------------------------------------------

    def _request(self, method: str, path: str, *, json: Any = None) -> Any:
        try:
            resp = self._client.request(method, path, json=json)
        except httpx.HTTPError as exc:  # connect/timeout/etc. — console unreachable
            raise ConsoleError(
                f"console {self._label} unreachable: {method} {path}: {exc}"
            ) from exc
        if resp.status_code >= 300:
            raise ConsoleError(
                f"console {self._label} returned {resp.status_code} for "
                f"{method} {path}: {resp.text[:400]}",
                status=resp.status_code,
                body=resp.text,
            )
        if not resp.content:
            return None
        ctype = resp.headers.get("content-type", "")
        if "application/json" in ctype:
            return resp.json()
        return resp.text

    # --- P4 REST surface ---------------------------------------------------

    def register(self, *, region: str, plan: str, tenant_id: Optional[str] = None) -> dict:
        """POST /api/v1/register -> mints (tenant_id?,) deployment_id.

        Returns ``{"tenant_id": ..., "deployment_id": ...}``.
        """
        body: dict[str, Any] = {"region": region, "plan": plan}
        if tenant_id:
            body["tenant_id"] = tenant_id
        out = self._request("POST", "/api/v1/register", json=body)
        if not isinstance(out, dict) or "deployment_id" not in out or "tenant_id" not in out:
            raise ConsoleError(f"register returned an unexpected body: {out!r}")
        return out

    def heartbeat(self, record: dict) -> Any:
        """POST /api/v1/heartbeat with a C4 DeploymentRecord JSON dict."""
        return self._request("POST", "/api/v1/heartbeat", json=record)

    def list_deployments(self) -> list[dict]:
        """GET /api/v1/deployments -> list of DeploymentRecord dicts."""
        out = self._request("GET", "/api/v1/deployments")
        if not isinstance(out, list):
            raise ConsoleError(f"deployments listing was not a JSON array: {out!r}")
        return out

    def get_deployment(self, deployment_id: str) -> Optional[dict]:
        """GET /api/v1/deployments/{id} -> DeploymentRecord dict, or None on 404."""
        try:
            return self._request("GET", f"/api/v1/deployments/{deployment_id}")
        except ConsoleError as exc:
            if exc.status == 404:
                return None
            raise

    def has_deployment(self, deployment_id: str) -> bool:
        """True iff the console currently lists ``deployment_id`` (confirm step)."""
        return self.get_deployment(deployment_id) is not None

    def ping(self) -> bool:
        """Best-effort liveness: GET / and treat any 2xx as alive."""
        try:
            self._request("GET", "/")
            return True
        except ConsoleError:
            return False


class HttpConsoleClient(ConsoleClient):
    """Console client over a real network base URL (e.g. ``http://console:8080``)."""

    def __init__(self, base_url: str, *, timeout: float = 10.0) -> None:
        client = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout)
        super().__init__(client, label=base_url)


class ASGIConsoleClient(ConsoleClient):
    """Console client that dispatches in-process to a FastAPI/ASGI app.

    Used by the self-test and by ``--embedded-console`` so onboarding can be
    exercised end-to-end against the *real* P4 contract with no server process.

    Implemented over ``starlette.testclient.TestClient`` — a *synchronous*
    ``httpx.Client`` subclass that drives an ASGI app in-process (httpx's own
    ``ASGITransport`` is async-only and cannot back a sync ``httpx.Client``). The
    app is kept on ``.app`` so the onboard/offboard rollback hooks can reach the
    in-memory FleetStore to deregister a deployment.
    """

    def __init__(self, app: Any, *, base_url: str = "http://embedded-console") -> None:
        from starlette.testclient import TestClient  # deferred: test/dev only

        # raise_server_exceptions=False -> HTTP 4xx/5xx come back as responses
        # (so our _request maps 404 -> None etc.) instead of re-raising in-process.
        client = TestClient(app, base_url=base_url, raise_server_exceptions=False)
        super().__init__(client, label="embedded")
        self.app = app


def make_console_client(
    *,
    console_url: Optional[str] = None,
    app: Any = None,
) -> ConsoleClient:
    """Build the appropriate console client.

    * ``app`` given           -> in-process ASGI client (self-test / embedded).
    * else ``console_url``     -> real HTTP client.
    * neither                  -> ``ConsoleError`` (onboarding needs a console target,
                                  or must be told to mint identity locally).
    """
    if app is not None:
        return ASGIConsoleClient(app)
    if console_url:
        return HttpConsoleClient(console_url)
    raise ConsoleError("no console target: pass console_url or an embedded app")
