from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

import services.app.gateway.byoc_control_plane_keys as key_resolvers
from services.app.gateway.byoc_control_plane_router import (
    build_byoc_control_plane_router,
)
from services.app.gateway.settings import GatewaySettings
from services.platform.runtime.byoc_control_plane_intake import (
    InMemoryByocEvidencePackageIntakeStore,
    evidence_package_submission_payload,
    signed_evidence_receipt_read_headers,
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


def _read_headers(
    path: str,
    *,
    query: str = "",
    nonce: str = "nonce-intake-router-read-001",
) -> dict[str, str]:
    return signed_evidence_receipt_read_headers(
        method="GET",
        path=path,
        query=query,
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
        nonce=nonce,
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

    async def fetch(self, query: str, *args):
        self.calls.append((query, args))
        rows = list(self.rows.values())
        arg_index = 0
        if "deployment_id = $" in query:
            deployment_id = args[arg_index]
            arg_index += 1
            rows = [row for row in rows if row["deployment_id"] == deployment_id]
        if "customer_id = $" in query:
            customer_id = args[arg_index]
            arg_index += 1
            rows = [row for row in rows if row["customer_id"] == customer_id]
        limit = args[-1]
        rows.sort(
            key=lambda row: (row["accepted_at"], row["receipt_id"]),
            reverse=True,
        )
        return rows[:limit]


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
        lookup_path = (
            f"/byoc/control-plane/evidence-packages/{receipt['receipt_id']}"
        )
        lookup = await client.get(
            lookup_path,
            headers=_read_headers(
                lookup_path,
                nonce="nonce-intake-router-read-lookup",
            ),
        )
        query = f"deployment_id={PACKAGE.deployment_id}&limit=10"
        list_path = "/byoc/control-plane/evidence-packages"
        receipt_list = await client.get(
            f"{list_path}?{query}",
            headers=_read_headers(
                list_path,
                query=query,
                nonce="nonce-intake-router-read-list",
            ),
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
    assert receipt_list.status_code == 200
    assert receipt_list.json()["result_count"] == 1
    assert "source_artifacts" not in receipt_list.text
    assert "gateway.customer.internal" not in receipt_list.text


@pytest.mark.asyncio
async def test_byoc_control_plane_receipt_reads_require_signed_headers() -> None:
    app, _ = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/byoc/control-plane/evidence-packages/evpkg_0")

    assert response.status_code == 403
    assert "missing_read_auth_headers" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_receipt_list_requires_query_bound() -> None:
    app, _ = _app()
    list_path = "/byoc/control-plane/evidence-packages"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            list_path,
            headers=_read_headers(
                list_path,
                nonce="nonce-intake-router-unbounded-list",
            ),
        )

    assert response.status_code == 400
    assert "deployment_id or customer_id" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_unknown_key_ref() -> None:
    app, _ = _app()
    submission = _submission().model_dump(mode="json")
    submission["signature"]["key_ref"] = "control-plane/byoc/unknown-key"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=submission,
        )

    assert response.status_code == 403
    assert "unknown_key_ref" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_rejects_legacy_static_secret_in_production() -> None:
    app, _ = _app()
    app.state.gateway_settings = GatewaySettings(environment="production")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )

    assert response.status_code == 503
    assert "not configured" in response.text


@pytest.mark.asyncio
async def test_byoc_control_plane_uses_managed_key_resolver_from_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, bool | None]] = []

    def _fake_load_app_secret_text_from_env(
        name: str,
        *,
        production: bool | None = None,
        **_,
    ) -> str:
        calls.append((name, production))
        return SIGNING_SECRET

    monkeypatch.setattr(
        key_resolvers,
        "load_app_secret_text_from_env",
        _fake_load_app_secret_text_from_env,
    )
    store = InMemoryByocEvidencePackageIntakeStore()
    app = FastAPI()
    app.state.byoc_evidence_intake_store = store
    app.state.gateway_settings = GatewaySettings(
        environment="production",
        byoc_evidence_intake_key_ref=SIGNING_KEY_REF,
        byoc_evidence_intake_signing_key_secret_ref=(
            "prod/fyralis/dep-test/evidence-intake-signing-key"
        ),
        byoc_evidence_read_key_ref=SIGNING_KEY_REF,
        byoc_evidence_read_signing_key_secret_ref=(
            "prod/fyralis/dep-test/evidence-read-signing-key"
        ),
    )
    app.include_router(build_byoc_control_plane_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/byoc/control-plane/evidence-packages",
            json=_submission().model_dump(mode="json"),
        )
        receipt = response.json()
        lookup_path = (
            f"/byoc/control-plane/evidence-packages/{receipt['receipt_id']}"
        )
        lookup = await client.get(
            lookup_path,
            headers=_read_headers(
                lookup_path,
                nonce="nonce-intake-managed-read-lookup",
            ),
        )

    assert response.status_code == 202
    assert lookup.status_code == 200
    assert calls == [
        ("FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY", True),
        ("FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY", True),
    ]


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
        query = f"deployment_id={PACKAGE.deployment_id}&customer_id={PACKAGE.customer_id}"
        list_path = "/byoc/control-plane/evidence-packages"
        receipt_list = await client.get(
            f"{list_path}?{query}",
            headers=_read_headers(
                list_path,
                query=query,
                nonce="nonce-intake-router-pg-list",
            ),
        )

    assert response.status_code == 202
    assert receipt_list.status_code == 200
    assert receipt_list.json()["result_count"] == 1
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
