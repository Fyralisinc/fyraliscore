"""config_service.py — signed per-deployment config distribution (FR-C3 / FR-C4 / FR-D4).

A FastAPI service on **cp-net** that serves each data plane its **signed config bundle**
= feature **flags** + **telemetry_tier** (C3) + **token-rotation schedule** (FR-D4). The
outbound-only **agent** points ``AGENT_CONFIG_URL`` at this service, ``GET``s the bundle,
and **verifies-before-apply** (I6) using the committed ``control-plane/signing`` keyring —
which the agent already does in ``agent/config_pull.py``.

The agent's pull contract (``agent/config_pull.py:http_fetcher``)
----------------------------------------------------------------
Given ``AGENT_CONFIG_URL = <config_url>`` the agent fetches **three** URLs:

    GET <config_url>                  -> the config JSON          (response.content)
    GET <config_url>.sig              -> base64 detached ed25519  (response.text)
    GET <config_url>.manifest.json    -> the C2 manifest JSON     (response.content)

then runs ``verify_bundle.verify_file`` over the trio against its shipped trust root and
applies ONLY if the signature verifies, the ``key_id`` is known/active, and the manifest
``artifact == "config"``. A tamper of any served byte fails verification and is rejected.

So this service exposes, for the HEAD (current) version of a deployment::

    GET /config/{deployment_id}                     -> config.json bytes
    GET /config/{deployment_id}.sig                 -> the .sig text
    GET /config/{deployment_id}.manifest.json       -> the manifest JSON

(and a pinned-version variant ``/config/{deployment_id}/v{n}`` + sidecars). Set the
agent's ``AGENT_CONFIG_URL`` to ``http://config-dist:8090/config/<deployment_id>``.

Operator / publish surface (NOT for the agent)
----------------------------------------------
    GET  /healthz
    POST /api/v1/config/{deployment_id}        -> publish a new signed version
    GET  /api/v1/config/{deployment_id}        -> describe HEAD (version, tier, flags)
    GET  /api/v1/config/{deployment_id}/versions
    GET  /api/v1/deployments

Run::

    uvicorn config_service:app --host 0.0.0.0 --port 8090

Reuses (does NOT redefine) the committed siblings ``control-plane/signing``
(``sign_bundle``/``verify_bundle``/``signing_lib``) and ``control-plane/lib`` tiers; all
signing/verifying is ed25519 via that keyring (C2/I6).
"""

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Path as PathParam, Response
from pydantic import BaseModel, Field, field_validator

_HERE = Path(__file__).resolve().parent
_CP_ROOT = _HERE.parent
for _p in (str(_CP_ROOT), str(_CP_ROOT / "signing")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from store import (  # noqa: E402
    ConfigStore,
    ConfigStoreError,
    ConfigVersion,
    DEFAULT_SIGNING_HOME,
    DEFAULT_STORE_ROOT,
    SigningHome,
    default_config_payload,
)

# TelemetryTier from the shared lib (C3) — validate tier values against it.
try:
    from lib import TelemetryTier  # type: ignore
except Exception:  # pragma: no cover - lib import is best-effort for validation only
    TelemetryTier = None  # type: ignore

__all__ = ["app", "build_store", "create_app"]

# Media types matching what the agent's requests client will see.
CONFIG_MEDIA = "application/json"
MANIFEST_MEDIA = "application/json"
SIG_MEDIA = "text/plain"


# --------------------------------------------------------------------------- #
# Store wiring (env-driven, single shared instance)                           #
# --------------------------------------------------------------------------- #


def build_store() -> ConfigStore:
    """Build the ConfigStore from ``CONFIG_DIST_*`` env (with config-dist defaults)."""
    store_root = os.environ.get("CONFIG_DIST_STORE_ROOT") or str(DEFAULT_STORE_ROOT)
    signing_home_root = (
        os.environ.get("CONFIG_DIST_SIGNING_HOME") or str(DEFAULT_SIGNING_HOME)
    )
    key_id = os.environ.get("CONFIG_DIST_KEY_ID", "cp-config-dist")
    home = SigningHome(Path(signing_home_root), key_id=key_id)
    return ConfigStore(store_root=store_root, signing_home=home)


# --------------------------------------------------------------------------- #
# Request models                                                              #
# --------------------------------------------------------------------------- #


def _valid_tier(v: str) -> str:
    if TelemetryTier is not None:
        return TelemetryTier.parse(v).value  # raises on garbage
    if str(v).strip().upper() not in ("T1", "T2", "T3"):
        raise ValueError(f"telemetry_tier must be T1|T2|T3, got {v!r}")
    return str(v).strip().upper()


class PublishRequest(BaseModel):
    """Publish a new signed config version for a deployment.

    Either supply the whole ``config`` body, or supply individual pieces
    (``flags`` / ``telemetry_tier`` / ``token_rotation``) which are layered on top of
    the deployment's current config (or a default for a brand-new deployment). Supplying
    ``config`` and a piece together is a 422 (avoid ambiguity).
    """

    tenant_id: str = Field(min_length=1)
    config: Optional[Dict[str, Any]] = None
    flags: Optional[Dict[str, Any]] = None
    telemetry_tier: Optional[str] = None
    token_rotation: Optional[Dict[str, Any]] = None

    @field_validator("telemetry_tier")
    @classmethod
    def _check_tier(cls, v):
        if v is None:
            return v
        return _valid_tier(v)


# --------------------------------------------------------------------------- #
# App factory                                                                  #
# --------------------------------------------------------------------------- #


def create_app(store: Optional[ConfigStore] = None) -> FastAPI:
    app = FastAPI(title="Fyralis Config Distribution", version="0.1.0")
    app.state.store = store or build_store()
    # Serialize publishes per process so the version counter can't race.
    app.state.publish_lock = threading.Lock()

    def _store() -> ConfigStore:
        return app.state.store

    def _load_head(deployment_id: str) -> ConfigVersion:
        try:
            cv = _store().get_head(deployment_id)
        except ConfigStoreError as exc:
            raise HTTPException(400, str(exc))
        if cv is None:
            raise HTTPException(
                404, f"no published config for deployment {deployment_id!r}"
            )
        return cv

    def _load_pinned(deployment_id: str, version: int) -> ConfigVersion:
        try:
            cv = _store().get_version(deployment_id, version)
        except ConfigStoreError as exc:
            raise HTTPException(400, str(exc))
        if cv is None:
            raise HTTPException(
                404, f"no config v{version} for deployment {deployment_id!r}"
            )
        return cv

    # ------------------------------------------------------------------ #
    # AGENT PULL SURFACE — the three URLs config_pull.http_fetcher hits.  #
    # Order matters: register the ".sig" / ".manifest.json" routes BEFORE #
    # the bare config route so the literal suffixes win over the catch.   #
    # FastAPI matches in declaration order; explicit suffix paths first.  #
    # ------------------------------------------------------------------ #

    @app.get("/config/{deployment_id}.sig", response_class=Response)
    def get_head_sig(deployment_id: str = PathParam(...)) -> Response:
        cv = _load_head(deployment_id)
        # config_pull reads this with response.text and re-adds a trailing newline.
        return Response(content=cv.sig_b64 + "\n", media_type=SIG_MEDIA)

    @app.get("/config/{deployment_id}.manifest.json", response_class=Response)
    def get_head_manifest(deployment_id: str = PathParam(...)) -> Response:
        cv = _load_head(deployment_id)
        return Response(
            content=(cv.manifest_path.read_bytes()), media_type=MANIFEST_MEDIA
        )

    @app.get("/config/{deployment_id}", response_class=Response)
    def get_head_config(deployment_id: str = PathParam(...)) -> Response:
        cv = _load_head(deployment_id)
        # Serve the EXACT signed bytes (config_pull verifies over these via the sig).
        return Response(content=cv.config_bytes, media_type=CONFIG_MEDIA)

    # Pinned-version pull (operator pinning / rollback): /config/<id>/v<N>[.sig|...]
    @app.get("/config/{deployment_id}/v{version}.sig", response_class=Response)
    def get_pinned_sig(deployment_id: str, version: int) -> Response:
        cv = _load_pinned(deployment_id, version)
        return Response(content=cv.sig_b64 + "\n", media_type=SIG_MEDIA)

    @app.get("/config/{deployment_id}/v{version}.manifest.json", response_class=Response)
    def get_pinned_manifest(deployment_id: str, version: int) -> Response:
        cv = _load_pinned(deployment_id, version)
        return Response(
            content=(cv.manifest_path.read_bytes()), media_type=MANIFEST_MEDIA
        )

    @app.get("/config/{deployment_id}/v{version}", response_class=Response)
    def get_pinned_config(deployment_id: str, version: int) -> Response:
        cv = _load_pinned(deployment_id, version)
        return Response(content=cv.config_bytes, media_type=CONFIG_MEDIA)

    # ------------------------------------------------------------------ #
    # OPERATOR / PUBLISH SURFACE (cp-net behind the auth-proxy)           #
    # ------------------------------------------------------------------ #

    @app.get("/healthz")
    def healthz() -> dict:
        st = _store()
        return {
            "ok": True,
            "active_key_id": st.signing_home.active_key_id(),
            "store_root": str(st.store_root),
            "deployments": len(st.list_deployments()),
        }

    @app.get("/trust_root.json", response_class=Response)
    def trust_root() -> Response:
        """The PUBLIC trust root agents pin to verify configs (convenience export).

        This is the same public keyring shape the agent ships; serving it lets a fresh
        installer fetch the verifier keys. It contains no private material.
        """
        st = _store()
        p = st.signing_home.trust_root_path
        if not p.is_file():
            raise HTTPException(503, "signing home not initialized")
        return Response(content=p.read_bytes(), media_type="application/json")

    @app.get("/api/v1/deployments")
    def list_deployments() -> dict:
        st = _store()
        return {"deployments": st.list_deployments()}

    @app.post("/api/v1/config/{deployment_id}")
    def publish(deployment_id: str, req: PublishRequest) -> dict:
        st = _store()
        # Build the config_body to publish.
        if req.config is not None and any(
            x is not None for x in (req.flags, req.telemetry_tier, req.token_rotation)
        ):
            raise HTTPException(
                422,
                "supply either a whole `config` body OR individual "
                "flags/telemetry_tier/token_rotation pieces, not both",
            )

        try:
            with app.state.publish_lock:
                if req.config is not None:
                    body = dict(req.config)
                    if "telemetry_tier" in body:
                        body["telemetry_tier"] = _valid_tier(body["telemetry_tier"])
                else:
                    # Layer the supplied pieces over the current config (or a default).
                    current = st.get_head(deployment_id)
                    if current is not None:
                        body = dict(current.document().get("config", {}))
                    else:
                        body = default_config_payload(
                            tenant_id=req.tenant_id, deployment_id=deployment_id
                        )
                    if req.flags is not None:
                        merged_flags = dict(body.get("flags", {}))
                        merged_flags.update(req.flags)
                        body["flags"] = merged_flags
                    if req.telemetry_tier is not None:
                        body["telemetry_tier"] = req.telemetry_tier
                    if req.token_rotation is not None:
                        merged_rot = dict(body.get("token_rotation", {}))
                        merged_rot.update(req.token_rotation)
                        body["token_rotation"] = merged_rot

                cv = st.publish(
                    deployment_id=deployment_id,
                    tenant_id=req.tenant_id,
                    config_body=body,
                )
        except ConfigStoreError as exc:
            raise HTTPException(400, str(exc))

        return {
            "deployment_id": cv.deployment_id,
            "version": cv.version,
            "key_id": cv.key_id,
            "telemetry_tier": cv.telemetry_tier,
            "sha256": cv.manifest.get("sha256"),
            "signed_at": cv.manifest.get("signed_at"),
            "config_url": f"/config/{cv.deployment_id}",
        }

    @app.get("/api/v1/config/{deployment_id}")
    def describe(deployment_id: str) -> dict:
        cv = _load_head(deployment_id)
        doc = cv.document()
        return {
            "deployment_id": cv.deployment_id,
            "version": cv.version,
            "key_id": cv.key_id,
            "tenant_id": doc.get("tenant_id"),
            "telemetry_tier": cv.telemetry_tier,
            "created": doc.get("created"),
            "config": doc.get("config"),
            "manifest": cv.manifest,
            "config_url": f"/config/{cv.deployment_id}",
        }

    @app.get("/api/v1/config/{deployment_id}/versions")
    def versions(deployment_id: str) -> dict:
        st = _store()
        try:
            vs = st.list_versions(deployment_id)
        except ConfigStoreError as exc:
            raise HTTPException(400, str(exc))
        return {
            "deployment_id": deployment_id,
            "head": st.current_version(deployment_id),
            "versions": vs,
        }

    return app


# Module-level ASGI app for ``uvicorn config_service:app``.
app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("CONFIG_DIST_PORT", "8090")),
    )
