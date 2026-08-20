#!/usr/bin/env python3
"""app.py — the Fyralis BYOC operator **console** (P4, port 8080 on cp-net).

A FastAPI service over the fleet registry (C4 ``DeploymentRecord`` rows). It is
the operator's read/write surface on the fleet and implements the P4 CONSOLE REST
API contract **exactly**:

    POST /api/v1/register     {tenant_id?, region, plan}
                              -> mints deployment_id (+ tenant_id if absent)
                              -> {tenant_id, deployment_id}
    POST /api/v1/heartbeat    {DeploymentRecord JSON}   -> upsert + recompute health
    GET  /api/v1/deployments  -> [DeploymentRecord w/ derived health]
    GET  /api/v1/deployments/{deployment_id} -> one record (404 if unknown)
    GET  /                     -> minimal HTML fleet rollup

Health is **derived on read** from heartbeat freshness (NFR-5: stale > 90 s ⇒
yellow, missing > 300 s ⇒ red) plus reported SLI burn / license expiry — the
console never trusts the ``health`` field off the wire. All of that lives in
:mod:`store` (which reuses ``lib.deployment`` for the record + health math); this
module is the thin HTTP/HTML layer.

Run::

    CP_CONSOLE_PORT=8080 python console/app.py
    # or
    uvicorn app:app --host 0.0.0.0 --port 8080   # from inside console/
"""

from __future__ import annotations

import datetime as _dt
import html as _html
import os
import sys
from pathlib import Path

import hmac

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Make the control-plane root importable (see store.py for the same anchor): we
# front-load the root and evict any foreign top-level ``lib`` already imported so
# ``import lib...`` binds to the control-plane shared library.
_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, *_HERE.parents):
    if (_cand / "SPRINT_PLAN.md").is_file() or (_cand / "lib" / "deployment.py").is_file():
        _root = str(_cand)
        while _root in sys.path:
            sys.path.remove(_root)
        sys.path.insert(0, _root)
        _existing = sys.modules.get("lib")
        if _existing is not None and not (
            getattr(_existing, "__file__", "") or ""
        ).startswith(_root):
            for _name in [n for n in list(sys.modules) if n == "lib" or n.startswith("lib.")]:
                del sys.modules[_name]
        break

from lib.deployment import DeploymentRecord, Health  # noqa: E402
from lib.primitives import to_rfc3339, utcnow  # noqa: E402

from deps import (  # noqa: E402
    ConsoleAudit,
    ConsoleDeps,
    ConsoleSettings,
    build_signer,
    make_require_operator,
)
from store import DeploymentStore  # noqa: E402

import importlib  # noqa: E402
import logging  # noqa: E402
import pkgutil  # noqa: E402

__all__ = ["app", "create_app", "store"]

_LOG = logging.getLogger("fyralis.console")


# --- request models (the wire contracts) -----------------------------------


class RegisterRequest(BaseModel):
    """``POST /api/v1/register`` body (P4): ``{tenant_id?, region, plan}``.

    ``tenant_id`` is optional (minted if absent). ``region`` is required.
    ``plan`` is accepted per contract; extra fields are tolerated so a richer
    onboarding payload still registers.
    """

    model_config = ConfigDict(extra="ignore")

    tenant_id: str | None = None
    region: str = Field(min_length=1)
    plan: str | None = None
    # Optional extras a richer onboarding flow may pass through.
    version: str | None = None
    license_expiry: str | None = None
    telemetry_tier: str | None = None


class RegisterResponse(BaseModel):
    """``POST /api/v1/register`` response (P4): ``{tenant_id, deployment_id}``."""

    tenant_id: str
    deployment_id: str


# --- app factory ------------------------------------------------------------


def _console_port() -> int:
    raw = os.environ.get("CP_CONSOLE_PORT", "8080")
    try:
        return int(raw)
    except ValueError:
        return 8080


# --- write-path authentication (I4 integrity) -------------------------------
#
# The console's WRITE endpoints (register / heartbeat / delete) MUST carry a
# bearer token — without one anything that can reach console:8080 on cp-net (or
# the host-published operator port) could enrol, forge heartbeats for, or delete
# any deployment, corrupting the fleet registry the operator trusts (I4). The
# token is the shared ``CONSOLE_INGEST_TOKEN`` minted by bootstrap.sh, passed to
# the console via env and shipped to the agent in its onboarding bundle.
#
# READS (GET /api/v1/deployments[/{id}], GET / rollup, /healthz) stay open for
# the operator UI for now. TODO(next-sprint): put operator READ auth (SSO/VPN
# session) in front of the read surface too; today reads are assumed to sit
# behind the operator's network boundary, writes are authenticated in-band.


def _resolve_ingest_token(explicit: str | None) -> str | None:
    """The configured write token: the explicit arg, else ``CONSOLE_INGEST_TOKEN``.

    Returns ``None`` (or empty) when no token is configured at all — in that case
    the console fails CLOSED on every write (503), never silently open.
    """
    if explicit is not None:
        return explicit
    return os.environ.get("CONSOLE_INGEST_TOKEN")


def _resolve_operator_token(explicit: str | None) -> str | None:
    """The configured OPERATOR write token: the explicit arg, else ``OPERATOR_TOKEN``.

    Distinct from the agent's ``CONSOLE_INGEST_TOKEN`` (I4 operator-vs-agent
    identity). Returns ``None`` when unconfigured — in which case operator WRITE
    endpoints fail CLOSED (503 via ``require_operator``), never silently open.
    """
    if explicit is not None:
        return explicit
    return os.environ.get("OPERATOR_TOKEN")


def _extract_bearer(authorization: str | None) -> str | None:
    """Pull the token out of an ``Authorization: Bearer <token>`` header."""
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


def create_app(
    deployment_store: DeploymentStore | None = None,
    *,
    ingest_token: str | None = None,
    operator_token: str | None = None,
    signer=None,
    audit=None,
    settings: ConsoleSettings | None = None,
    mount_feature_routers: bool = True,
) -> FastAPI:
    """Build the console FastAPI app over ``deployment_store`` (a fresh one by
    default). Exposed as a factory so tests can inject a non-persistent store.

    ``ingest_token`` is the bearer token required on every AGENT WRITE endpoint
    (register/heartbeat/delete + agent-facing desired GET — I4). If ``None`` it is
    read from ``CONSOLE_INGEST_TOKEN``; if that is also unset the console refuses
    those writes with 503 (fail-closed).

    ``operator_token`` is the SEPARATE bearer required on operator WRITE endpoints
    (desired-state mutations), read from ``OPERATOR_TOKEN`` when ``None``; unset ⇒
    operator writes fail closed (503). ``signer`` / ``audit`` / ``settings`` are
    the shared signing/audit/settings handed to feature routers off ``deps`` (each
    built from the real CP libs when not injected). ``mount_feature_routers``
    scans ``console/routers/*.py`` for ``register(app, deps)`` (set False in tests
    that want only the core endpoints).
    """
    st = deployment_store if deployment_store is not None else DeploymentStore()
    configured_token = _resolve_ingest_token(ingest_token)
    configured_operator_token = _resolve_operator_token(operator_token)

    application = FastAPI(
        title="Fyralis BYOC — Fleet Console",
        version="0.1.0",
        description=(
            "Operator console over the fleet registry (C4 deployment records). "
            "Health is derived on read from heartbeat freshness (NFR-5)."
        ),
    )
    # Stash the store on the app so tests / handlers can reach it.
    application.state.store = st
    application.state.ingest_token = configured_token
    application.state.operator_token = configured_operator_token

    def require_write_auth(
        authorization: str | None = Header(default=None),
    ) -> None:
        """FastAPI dependency guarding every WRITE endpoint (I4).

        * No token configured on the server -> 503 (fail-closed; the console is
          misconfigured and must NOT accept unauthenticated writes).
        * Missing/malformed bearer or a non-matching token -> 401.
        Comparison is constant-time (``hmac.compare_digest``) to avoid leaking the
        token via timing.
        """
        if not configured_token:
            raise HTTPException(
                status_code=503,
                detail="console write auth not configured (CONSOLE_INGEST_TOKEN unset)",
            )
        presented = _extract_bearer(authorization)
        if presented is None or not hmac.compare_digest(presented, configured_token):
            raise HTTPException(
                status_code=401,
                detail="missing or invalid bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    # ------------------------------------------------------------------ API

    @application.post(
        "/api/v1/register",
        response_model=RegisterResponse,
        tags=["console"],
        summary="Register a deployment (mint deployment_id, + tenant_id if absent).",
        dependencies=[Depends(require_write_auth)],
    )
    def register(body: RegisterRequest) -> RegisterResponse:
        rec = st.register(
            tenant_id=body.tenant_id,
            region=body.region,
            plan=body.plan,
            version=body.version or "0.0.0",
            license_expiry=body.license_expiry,
            telemetry_tier=body.telemetry_tier or "T1",
        )
        return RegisterResponse(
            tenant_id=rec.tenant_id, deployment_id=rec.deployment_id
        )

    @application.post(
        "/api/v1/heartbeat",
        tags=["console"],
        summary="Upsert a heartbeat (DeploymentRecord JSON) and recompute health.",
        dependencies=[Depends(require_write_auth)],
    )
    async def heartbeat(request: Request) -> JSONResponse:
        # Parse the body into the shared C4 record. A malformed record is a 422
        # with the pydantic detail (the agent's bug, not the console's).
        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="body must be JSON")
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=422, detail="body must be a DeploymentRecord object"
            )
        # ADDITIVE: a heartbeat MAY carry an optional ``applied`` facet (the agent's
        # reconcile result: applied_config_version / applied_release /
        # acked_action_ids / license_state_applied). Pop it BEFORE constructing the
        # strict C4 record (which forbids extras) and record it on the side so the
        # console can compute drift. Old agents that don't send it still work.
        applied_facet = payload.pop("applied", None)
        try:
            record = DeploymentRecord(**payload)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors())
        except Exception as exc:
            # A field validator may raise a typed control-plane error (e.g.
            # TierError on a bad telemetry_tier) rather than wrapping into a
            # ValidationError — that is still the caller's malformed record, so
            # surface it as a 422, never a 500.
            raise HTTPException(status_code=422, detail=str(exc))

        stored = st.upsert(record)  # upsert by deployment_id; health re-derived
        if isinstance(applied_facet, dict) and applied_facet:
            # Record the agent-reported applied facets alongside the registry so
            # GET desired drift (the C1 drill-down) can compute desired-vs-applied.
            st.record_applied(stored.deployment_id, applied_facet)
        return JSONResponse(content=stored.to_registry_dict())

    @application.get(
        "/api/v1/deployments",
        tags=["console"],
        summary="List all deployments with health derived on read.",
    )
    def list_deployments() -> JSONResponse:
        records = st.list_records()
        return JSONResponse(content=[r.to_registry_dict() for r in records])

    @application.get(
        "/api/v1/deployments/{deployment_id}",
        tags=["console"],
        summary="Get one deployment with health derived on read (404 if unknown).",
    )
    def get_deployment(deployment_id: str) -> JSONResponse:
        rec = st.record(deployment_id)
        if rec is None:
            raise HTTPException(
                status_code=404, detail=f"unknown deployment_id {deployment_id!r}"
            )
        return JSONResponse(content=rec.to_registry_dict())

    @application.delete(
        "/api/v1/deployments/{deployment_id}",
        tags=["console"],
        summary="Deregister (remove) a deployment row. Idempotent.",
        dependencies=[Depends(require_write_auth)],
    )
    def delete_deployment(deployment_id: str) -> JSONResponse:
        """Idempotent deregistration (FR-E onboarding rollback / offboard).

        Removes the row from the registry. Returns 200 with ``{removed: true}``
        when a row was deleted and 200 with ``{removed: false}`` when the row was
        already absent — deregistration is idempotent, so re-issuing the DELETE
        (e.g. a retried rollback) is never an error. The store remains the single
        source of truth; no health is derived here.
        """
        removed = st.delete(deployment_id)
        return JSONResponse(
            content={"deployment_id": deployment_id, "removed": removed}
        )

    # ------------------------------------------------------ agent-facing desired
    #
    # The OUTBOUND-only agent (I2) POLLS this on each tick to learn the operator's
    # desired state, then VERIFIES (I6) + reconciles + reports applied facets on
    # its next heartbeat. Guarded by the AGENT write token (CONSOLE_INGEST_TOKEN):
    # it is the agent's own credential, the same one it already presents on
    # heartbeat — NOT the operator token. 404 when no desired state has been
    # written for this deployment yet (the agent simply skips reconcile — I3).

    @application.get(
        "/api/v1/deployments/{deployment_id}/desired",
        tags=["agent"],
        summary="Agent pulls the operator-written DesiredState (404 if none).",
        dependencies=[Depends(require_write_auth)],
    )
    def get_desired(deployment_id: str) -> JSONResponse:
        desired = st.get_desired(deployment_id)
        if desired is None:
            raise HTTPException(
                status_code=404,
                detail=f"no desired state for deployment_id {deployment_id!r}",
            )
        return JSONResponse(content=desired.to_dict())

    @application.get("/healthz", tags=["ops"], summary="Liveness probe.")
    def healthz() -> dict:
        return {"status": "ok", "fleet_size": len(st)}

    # ------------------------------------------------------------------ HTML

    @application.get(
        "/",
        response_class=HTMLResponse,
        tags=["console"],
        summary="Minimal HTML fleet rollup.",
    )
    def rollup() -> HTMLResponse:
        return HTMLResponse(content=render_rollup(st))

    # ------------------------------------------------------------ feature deps
    #
    # Build the ONE deps namespace every feature router gets, then scan
    # console/routers/*.py for register(app, deps) and call each. Features ADD
    # files there; they never edit this module.
    require_operator = make_require_operator(configured_operator_token)
    built_signer = signer if signer is not None else build_signer()
    built_audit = audit if audit is not None else ConsoleAudit()
    deps = ConsoleDeps(
        store=st,
        signer=built_signer,
        audit=built_audit,
        require_operator=require_operator,
        require_agent_write=require_write_auth,
        settings=settings if settings is not None else ConsoleSettings.from_env(),
    )
    application.state.deps = deps

    if mount_feature_routers:
        _mount_feature_routers(application, deps)

    return application


# --- feature router mount loop ----------------------------------------------


def _mount_feature_routers(application: FastAPI, deps: ConsoleDeps) -> list[str]:
    """Scan ``console/routers/*.py`` and call each module's ``register(app, deps)``.

    A feature plugs in by dropping a module under ``console/routers/`` exposing a
    module-level ``def register(app, deps): ...``. We import each, and if it has a
    callable ``register`` we call it with the shared ``deps``.

    NON-FATAL-LOUD: a router that fails to IMPORT, or whose ``register`` raises,
    is LOGGED at error level and SKIPPED — a single broken feature can never take
    the console down. Returns the list of router module names successfully
    registered (for tests / introspection).
    """
    import routers as _routers_pkg  # console/routers/__init__.py

    registered: list[str] = []
    for mod_info in pkgutil.iter_modules(_routers_pkg.__path__):
        name = mod_info.name
        if name.startswith("_"):
            continue  # skip private/helper modules
        full = f"routers.{name}"
        try:
            module = importlib.import_module(full)
        except Exception as exc:  # broken import must not down the console
            _LOG.error("console: router %s failed to import (skipped): %s", full, exc)
            continue
        register = getattr(module, "register", None)
        if not callable(register):
            _LOG.warning("console: router %s has no register(app, deps) — skipped", full)
            continue
        try:
            register(application, deps)
        except Exception as exc:  # broken register() must not down the console
            _LOG.error("console: router %s register() raised (skipped): %s", full, exc)
            continue
        registered.append(name)
        _LOG.info("console: mounted feature router %s", full)
    return registered


# --- HTML rollup ------------------------------------------------------------

_BADGE_COLORS = {
    Health.GREEN: ("#0a7d28", "#e6f6ea"),
    Health.YELLOW: ("#8a6d00", "#fff6d6"),
    Health.RED: ("#a11", "#fde6e6"),
}


def _humanize_age(seconds: float) -> str:
    """Compact 'last heartbeat age' for the table (e.g. '12s', '3m', '2h')."""
    s = int(max(0.0, seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    if s < 86400:
        return f"{s // 3600}h {(s % 3600) // 60}m"
    return f"{s // 86400}d {(s % 86400) // 3600}h"


def _license_cell(record: DeploymentRecord, now: _dt.datetime) -> tuple[str, bool]:
    """Return (display string, expired?) for the license-expiry column."""
    exp = record.license_expiry
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=_dt.timezone.utc)
    expired = exp <= now
    days = (exp - now).total_seconds() / 86400.0
    when = to_rfc3339(exp)
    if expired:
        return f"{when} (EXPIRED)", True
    if days <= 30:
        return f"{when} (in {int(days)}d)", False
    return when, False


def render_rollup(st: DeploymentStore) -> str:
    """Render the minimal HTML fleet rollup table (operator landing page).

    A single self-contained HTML document (no external assets) so it renders the
    same in a container, a test ``TestClient``, or a plain browser. Columns:
    tenant, deployment, version, region, telemetry tier, **health badge**,
    last-heartbeat-age, license-expiry.
    """
    now = utcnow()
    records = st.list_records(now=now)
    summary = st.summary(now=now)

    rows_html: list[str] = []
    for rec in records:
        fg, bg = _BADGE_COLORS[rec.health]
        age_s = DeploymentStore.heartbeat_age_seconds(rec, now=now)
        lic_str, lic_expired = _license_cell(rec, now)
        badge = (
            f'<span class="badge" style="color:{fg};background:{bg};'
            f'border:1px solid {fg}">{_html.escape(rec.health.value.upper())}</span>'
        )
        lic_html = _html.escape(lic_str)
        if lic_expired:
            lic_html = f'<span class="expired">{lic_html}</span>'
        dep_href = f"/deployments/{_html.escape(rec.deployment_id)}"
        rows_html.append(
            "<tr>"
            f"<td>{_html.escape(rec.tenant_id)}</td>"
            f'<td class=mono><a href="{dep_href}">{_html.escape(rec.deployment_id)}</a></td>'
            f"<td>{_html.escape(rec.version)}</td>"
            f"<td>{_html.escape(rec.region)}</td>"
            f"<td>{_html.escape(rec.telemetry_tier.value)}</td>"
            f"<td>{badge}</td>"
            f"<td class=mono title='{_html.escape(to_rfc3339(rec.last_heartbeat_ts))}'>"
            f"{_html.escape(_humanize_age(age_s))} ago</td>"
            f"<td class=mono>{lic_html}</td>"
            "</tr>"
        )

    if not records:
        body_rows = (
            '<tr><td colspan="8" class="empty">No deployments registered yet. '
            "POST /api/v1/register to enroll one.</td></tr>"
        )
    else:
        body_rows = "\n".join(rows_html)

    generated = to_rfc3339(now)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fyralis Fleet Console</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
          margin: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 .25rem; }}
  .sub {{ color: #666; font-size: .85rem; margin-bottom: 1rem; }}
  .counts {{ margin: 0 0 1rem; font-size: .9rem; }}
  .counts span {{ display: inline-block; margin-right: .9rem; padding: .15rem .55rem;
                  border-radius: 999px; font-weight: 600; }}
  .c-green {{ color:#0a7d28; background:#e6f6ea; }}
  .c-yellow {{ color:#8a6d00; background:#fff6d6; }}
  .c-red {{ color:#a11; background:#fde6e6; }}
  .c-total {{ color:#333; background:#eee; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff;
           box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
  th, td {{ text-align: left; padding: .5rem .7rem; border-bottom: 1px solid #eee;
            font-size: .88rem; }}
  th {{ background: #f3f3f3; font-weight: 600; position: sticky; top: 0; }}
  tr:hover td {{ background: #fbfbfb; }}
  .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .82rem; }}
  .badge {{ display:inline-block; padding:.1rem .5rem; border-radius:4px;
            font-weight:700; font-size:.75rem; letter-spacing:.03em; }}
  .expired {{ color:#a11; font-weight:600; }}
  .empty {{ text-align:center; color:#777; padding:1.5rem; }}
  footer {{ margin-top: 1rem; color:#999; font-size:.78rem; }}
  nav.top {{ margin: 0 0 1rem; font-size:.85rem; }}
  nav.top a {{ display:inline-block; margin-right:.9rem; color:#2257a8;
               text-decoration:none; font-weight:600; }}
  nav.top a:hover {{ text-decoration:underline; }}
  td a {{ color:#2257a8; text-decoration:none; }}
  td a:hover {{ text-decoration:underline; }}
</style>
</head>
<body>
  <h1>Fyralis BYOC — Fleet Console</h1>
  <nav class="top">
    <a href="/">Fleet</a>
    <a href="/audit">Audit</a>
    <a href="/alerts">Alerts</a>
    <a href="/metering">Metering</a>
  </nav>
  <div class="sub">Operator rollup over the fleet registry (C4 deployment records).
    Health derived on read: stale &gt; {st.yellow_after_s}s &rarr; yellow,
    missing &gt; {st.red_after_s}s &rarr; red (NFR-5).</div>
  <div class="counts">
    <span class="c-total">{summary['total']} total</span>
    <span class="c-green">{summary['green']} green</span>
    <span class="c-yellow">{summary['yellow']} yellow</span>
    <span class="c-red">{summary['red']} red</span>
  </div>
  <table>
    <thead>
      <tr>
        <th>Tenant</th><th>Deployment</th><th>Version</th><th>Region</th>
        <th>Tier</th><th>Health</th><th>Last heartbeat</th><th>License expiry</th>
      </tr>
    </thead>
    <tbody>
{body_rows}
    </tbody>
  </table>
  <footer>Generated {generated} &middot; GET /api/v1/deployments for JSON.</footer>
</body>
</html>"""


# --- module-level app (uvicorn entrypoint) ----------------------------------

# A process-wide store (persistent under console/data/) and the app over it.
store: DeploymentStore = DeploymentStore()
app: FastAPI = create_app(store)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.environ.get("CP_CONSOLE_HOST", "0.0.0.0"),
        port=_console_port(),
        log_level=os.environ.get("CP_LOG_LEVEL", "info").lower(),
    )
