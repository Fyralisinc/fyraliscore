#!/usr/bin/env python3
"""service.py — OPTIONAL license-service HTTP endpoint (control-plane side).

The CLI (``issue_license.py``) + the local ``validator.py`` are the core deliverable; this
service is a thin convenience wrapper so the console/onboarding flow can mint a license over
HTTP and so the revocation list can be managed by an operator UI. The **agent never calls
this** — the agent validates locally against its shipped trust root + revocation list
(outbound-only, I2). This service lives on ``cp-net`` behind the auth-proxy in production.

Endpoints
---------
    GET  /healthz                          -> {"ok": true, "active_key_id": ...}
    POST /api/v1/licenses                  -> issue a signed license; returns the bundle
                                              {license, license_b64, sig_b64, manifest}
    POST /api/v1/licenses/validate         -> validate a posted bundle; returns the Decision
    GET  /api/v1/revocations               -> the revocation list
    POST /api/v1/revocations               -> add a revocation (FR-F)
    DELETE /api/v1/revocations             -> remove a revocation (un-revoke)

Issuing writes the bundle to a server-side temp dir, signs it with ``signing/sign_bundle``,
then returns the three files base64-encoded so the caller can persist the bundle wherever the
agent will read it. Run::

    uvicorn service:app --host 0.0.0.0 --port 8088
"""

from __future__ import annotations

import base64
import os
import sys
import tempfile

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import issue_license as il  # noqa: E402
import validator as vd  # noqa: E402
import revoke as rev  # noqa: E402
import sign_bundle as sb  # noqa: E402

app = FastAPI(title="Fyralis License Service", version="0.1.0")


# --------------------------------------------------------------------------- #
# Request models                                                              #
# --------------------------------------------------------------------------- #


class IssueRequest(BaseModel):
    tenant_id: str = Field(min_length=1)
    deployment_id: str = Field(min_length=1)
    plan: str = "standard"
    features: list[str] = Field(default_factory=list)
    duration_days: float | None = None
    duration_seconds: float | None = None
    expires_at: str | None = None
    key_id: str | None = None


class ValidateRequest(BaseModel):
    # The bundle the caller wants validated (e.g. echoed back from a stored bundle).
    license: dict
    sig_b64: str
    manifest: dict
    expected_tenant_id: str | None = None
    expected_deployment_id: str | None = None


class RevokeRequest(BaseModel):
    type: str = Field(description="license_id | deployment_id | tenant_id")
    value: str = Field(min_length=1)
    reason: str = ""


# --------------------------------------------------------------------------- #
# Routes                                                                       #
# --------------------------------------------------------------------------- #


@app.get("/healthz")
def healthz() -> dict:
    active = None
    try:
        import json

        with open(sb.TRUST_ROOT_PATH, "r", encoding="utf-8") as fh:
            active = json.load(fh).get("active_key_id")
    except Exception:
        active = None
    return {"ok": True, "active_key_id": active, "trust_root": sb.TRUST_ROOT_PATH}


@app.post("/api/v1/licenses")
def issue(req: IssueRequest) -> dict:
    n_expiry = sum(x is not None for x in (req.expires_at, req.duration_days, req.duration_seconds))
    if n_expiry != 1:
        raise HTTPException(
            422, "supply exactly one of expires_at / duration_days / duration_seconds"
        )
    with tempfile.TemporaryDirectory(prefix="license-issue-") as td:
        try:
            res = il.issue_license(
                tenant_id=req.tenant_id,
                deployment_id=req.deployment_id,
                plan=req.plan,
                features=req.features,
                duration_days=req.duration_days,
                duration_seconds=req.duration_seconds,
                expires_at=req.expires_at,
                out_dir=td,
                key_id=req.key_id,
            )
        except Exception as exc:
            raise HTTPException(500, f"issue failed: {exc}")

        def _b64(path: str) -> str:
            with open(path, "rb") as fh:
                return base64.b64encode(fh.read()).decode("ascii")

        import json

        with open(res["manifest_path"], "r", encoding="utf-8") as fh:
            manifest = json.load(fh)

        return {
            "license": res["license"],
            "license_id": res["license_id"],
            "license_b64": _b64(res["license_path"]),
            "sig_b64": open(res["sig_path"], "r", encoding="utf-8").read().strip(),
            "manifest": manifest,
        }


@app.post("/api/v1/licenses/validate")
def validate(req: ValidateRequest) -> dict:
    import json

    with tempfile.TemporaryDirectory(prefix="license-validate-") as td:
        lic_path = os.path.join(td, "license.json")
        with open(lic_path, "w", encoding="utf-8") as fh:
            json.dump(req.license, fh, indent=2, sort_keys=True)
        with open(lic_path + ".sig", "w", encoding="utf-8") as fh:
            fh.write(req.sig_b64.strip() + "\n")
        with open(lic_path + ".manifest.json", "w", encoding="utf-8") as fh:
            json.dump(req.manifest, fh, indent=2, sort_keys=True)
        d = vd.validate(
            license_path=lic_path,
            expected_tenant_id=req.expected_tenant_id,
            expected_deployment_id=req.expected_deployment_id,
        )
    return d.to_dict()


@app.get("/api/v1/revocations")
def list_revocations() -> dict:
    return rev.load_revocations()


@app.post("/api/v1/revocations")
def add_revocation(req: RevokeRequest) -> dict:
    try:
        entry = rev.add_revocation(rtype=req.type, value=req.value, reason=req.reason)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    return {"revoked": entry}


@app.delete("/api/v1/revocations")
def remove_revocation(req: RevokeRequest) -> dict:
    removed = rev.remove_revocation(rtype=req.type, value=req.value)
    return {"removed": removed, "type": req.type, "value": req.value}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("LICENSE_SERVICE_PORT", "8088")))
