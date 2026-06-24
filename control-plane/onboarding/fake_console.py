#!/usr/bin/env python3
"""fake_console.py — a minimal in-process console honoring the P4 REST contract.

WS-CONSOLE owns the *production* console service; onboarding must not write there.
But onboarding's self-test (and a ``--embedded-console`` convenience mode) needs a
console that speaks the exact P4 contract so the full transaction — register,
confirm-the-deployment-appears, offboard — can be exercised end-to-end without
depending on another agent's service being up.

This is that fixture: a tiny FastAPI app over an in-memory fleet registry,
implementing the shared P4 REST surface and reusing the committed
``lib.deployment.DeploymentRecord`` (C4) so health derivation matches production.

    POST /api/v1/register   {tenant_id?, region, plan}  -> {tenant_id, deployment_id}
    POST /api/v1/heartbeat  {DeploymentRecord JSON}      -> {ok, health}
    GET  /api/v1/deployments                             -> [DeploymentRecord (derived health)]
    GET  /api/v1/deployments/{deployment_id}             -> DeploymentRecord | 404
    GET  /                                               -> minimal HTML fleet rollup

A ``deployment_id`` is minted as ``<tenant>-<region-slug>-<rand4>`` (matching the
C4 example ``acme-use1-7f3a``); a ``tenant_id`` is minted only if the caller did
not supply one.

It is intentionally not persistent and not authenticated — it is a test/dev
fixture, NOT the production console. Run standalone for local dev::

    python fake_console.py            # uvicorn on :8080
"""

from __future__ import annotations

import os
import secrets
import sys
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
if _CP_ROOT not in sys.path:
    sys.path.insert(0, _CP_ROOT)

from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.responses import HTMLResponse  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from lib.deployment import DeploymentRecord, Health  # noqa: E402

__all__ = ["build_app", "FleetStore"]

_REGION_SLUGS = {
    "us-east": "use1",
    "us-east-1": "use1",
    "us-west": "usw2",
    "us-west-2": "usw2",
    "eu-west": "euw1",
    "eu-west-1": "euw1",
    "ap-south": "aps1",
    "ap-south-1": "aps1",
}


def _region_slug(region: str) -> str:
    r = region.strip().lower()
    if r in _REGION_SLUGS:
        return _REGION_SLUGS[r]
    # Fallback: strip non-alphanumerics, cap to a short slug.
    return "".join(ch for ch in r if ch.isalnum())[:6] or "rgn"


class FleetStore:
    """In-memory fleet registry: ``deployment_id -> DeploymentRecord``."""

    def __init__(self) -> None:
        self._by_id: dict[str, DeploymentRecord] = {}

    def mint_ids(self, *, region: str, tenant_id: Optional[str]) -> tuple[str, str]:
        tid = tenant_id or f"t-{secrets.token_hex(3)}"
        dep_id = f"{tid}-{_region_slug(region)}-{secrets.token_hex(2)}"
        # Vanishingly unlikely, but never collide.
        while dep_id in self._by_id:
            dep_id = f"{tid}-{_region_slug(region)}-{secrets.token_hex(2)}"
        return tid, dep_id

    def upsert(self, record: DeploymentRecord) -> DeploymentRecord:
        fresh = record.with_derived_health()
        self._by_id[fresh.deployment_id] = fresh
        return fresh

    def get(self, deployment_id: str) -> Optional[DeploymentRecord]:
        rec = self._by_id.get(deployment_id)
        return rec.with_derived_health() if rec else None

    def list(self) -> list[DeploymentRecord]:
        return [r.with_derived_health() for r in self._by_id.values()]

    def remove(self, deployment_id: str) -> bool:
        return self._by_id.pop(deployment_id, None) is not None


class RegisterRequest(BaseModel):
    region: str = Field(min_length=1)
    plan: str = Field(min_length=1)
    tenant_id: Optional[str] = None


def build_app(store: Optional[FleetStore] = None) -> FastAPI:
    """Build the FastAPI app. Pass a shared :class:`FleetStore` to inspect state
    from a test, or omit for a fresh store."""
    store = store or FleetStore()
    app = FastAPI(title="Fyralis BYOC console (fake)", version="0")
    app.state.store = store

    @app.post("/api/v1/register")
    def register(req: RegisterRequest) -> dict:
        tenant_id, deployment_id = store.mint_ids(region=req.region, tenant_id=req.tenant_id)
        return {"tenant_id": tenant_id, "deployment_id": deployment_id}

    @app.post("/api/v1/heartbeat")
    def heartbeat(record: dict) -> dict:
        try:
            rec = DeploymentRecord(**record)
        except Exception as exc:  # malformed record -> 422-ish
            raise HTTPException(status_code=422, detail=f"bad DeploymentRecord: {exc}")
        stored = store.upsert(rec)
        return {"ok": True, "deployment_id": stored.deployment_id, "health": stored.health.value}

    @app.get("/api/v1/deployments")
    def list_deployments() -> list[dict]:
        return [r.to_registry_dict() for r in store.list()]

    @app.get("/api/v1/deployments/{deployment_id}")
    def get_deployment(deployment_id: str) -> dict:
        rec = store.get(deployment_id)
        if rec is None:
            raise HTTPException(status_code=404, detail=f"no deployment {deployment_id!r}")
        return rec.to_registry_dict()

    @app.get("/", response_class=HTMLResponse)
    def rollup() -> str:
        rows = store.list()
        counts = {h: 0 for h in Health}
        for r in rows:
            counts[r.health] += 1
        body = "".join(
            f"<tr><td>{r.tenant_id}</td><td>{r.deployment_id}</td>"
            f"<td>{r.region}</td><td>{r.version}</td>"
            f"<td class='{r.health.value}'>{r.health.value}</td>"
            f"<td>{r.telemetry_tier.value}</td></tr>"
            for r in rows
        )
        return (
            "<html><head><title>Fyralis fleet</title></head><body>"
            f"<h1>Fyralis fleet ({len(rows)} deployments)</h1>"
            f"<p>green={counts[Health.GREEN]} "
            f"yellow={counts[Health.YELLOW]} red={counts[Health.RED]}</p>"
            "<table border=1><tr><th>tenant</th><th>deployment</th><th>region</th>"
            "<th>version</th><th>health</th><th>tier</th></tr>"
            f"{body}</table></body></html>"
        )

    return app


def main() -> int:
    import uvicorn  # local dev only

    uvicorn.run(build_app(), host="0.0.0.0", port=int(os.environ.get("CONSOLE_PORT", "8080")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
