from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from services.app.gateway.byoc_control_plane_router import (
    build_byoc_control_plane_router,
)
from services.platform.runtime.byoc_control_plane_intake import (
    InMemoryByocEvidencePackageIntakeStore,
    evidence_package_submission_payload,
    signed_evidence_package_submission,
)
from services.platform.runtime.byoc_evidence_package import load_byoc_evidence_package


ROOT = Path(__file__).resolve().parents[4]
PACKAGE = load_byoc_evidence_package(ROOT / "deploy/byoc/evidence-package.example.yaml")
SIGNING_SECRET = "local-control-plane-intake-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"
AGENT_ID = "agt_intake01"
AGENT_VERSION = "0.1.0"
SUBMITTED_AT = datetime(2026, 6, 26, 12, 30, tzinfo=UTC)


def _submission(*, signing_secret: str = SIGNING_SECRET):
    payload = evidence_package_submission_payload(
        package=PACKAGE,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce="nonce-intake-router-001",
        submitted_at=SUBMITTED_AT,
    )
    return signed_evidence_package_submission(
        payload,
        signing_secret=signing_secret,
        key_ref=SIGNING_KEY_REF,
    )


class _FakeReceiptPool:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.calls: list[tuple[str, tuple]] = []

    async def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        if query.lstrip().upper().startswith("INSERT"):
            row = {
                "receipt_id": args[0],
                "deployment_id": args[1],
                "customer_id": args[2],
                "agent_id": args[3],
                "agent_version": args[4],
                "artifact_revision": args[5],
                "cloud_provider": args[6],
                "region": args[7],
                "package_digest": args[8],
                "package_generated_at": args[9],
                "ledger_overall_status": args[10],
                "required_evidence_passed": args[11],
                "live_report_envelope_digest": args[12],
                "submitted_at": args[13],
                "accepted_at": args[14],
                "stored_scope": args[15],
            }
            self.rows[row["receipt_id"]] = row
            return row
        return self.rows.get(args[0])


def _app(
    *,
    configured: bool = True,
) -> tuple[FastAPI, InMemoryByocEvidencePackageIntakeStore]:
    store = InMemoryByocEvidencePackageIntakeStore()
    app = FastAPI()
    app.state.byoc_evidence_intake_store = store
    if configured:
        app.state.byoc_evidence_intake_secret = SIGNING_SECRET
        app.state.byoc_evidence_intake_key_ref = SIGNING_KEY_REF
    app.include_router(build_byoc_control_plane_router())
    return app, store


@pytest.mark.asyncio
async def test_byoc_control_plane_accepts_signed_evidence_package() -> None:
    app, store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )
        assert response.status_code == 202
        receipt = response.json()
        lookup = await client.get(
            f"/byoc/control-plane/evidence-packages/{receipt['receipt_id']}"
        )

    assert receipt["status"] == "accepted"
    assert receipt["stored_scope"] == "sanitized_metadata_only"
    assert receipt["deployment_id"] == PACKAGE.deployment_id
    assert len(store.records) == 1
    assert lookup.status_code == 200
    assert '"ledger":' not in lookup.text
    assert "source_artifacts" not in lookup.text
    assert '"evidence":' not in lookup.text
    assert "gateway.customer.internal" not in lookup.text
    assert "postgresql://" not in lookup.text


@pytest.mark.asyncio
async def test_byoc_control_plane_uses_postgres_store_from_gateway_deps() -> None:
    pool = _FakeReceiptPool()
    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=pool)
    app.state.byoc_evidence_intake_secret = SIGNING_SECRET
    app.state.byoc_evidence_intake_key_ref = SIGNING_KEY_REF
    app.include_router(build_byoc_control_plane_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )

    assert response.status_code == 202
    assert pool.calls
    flattened_args = " ".join(str(arg) for _, args in pool.calls for arg in args)
    assert "source_artifacts" not in flattened_args
    assert "fyralis.byoc.evidence_package.v1" not in flattened_args


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_bad_signature() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission(signing_secret="wrong-secret").model_dump(mode="json"),
        )

    assert response.status_code == 403
    assert "invalid_signature" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_requires_configured_intake_secret() -> None:
    app, _ = _app(configured=False)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )

    assert response.status_code == 503
    assert "not configured" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_raw_report_extra_field() -> None:
    app, _ = _app()
    submission = _submission().model_dump(mode="json")
    submission["package"]["raw_report"] = {
        "details": "https://gateway.customer.internal token=secret",
    }
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=submission,
        )

    assert response.status_code == 422
    assert "Extra inputs are not permitted" in response.text
