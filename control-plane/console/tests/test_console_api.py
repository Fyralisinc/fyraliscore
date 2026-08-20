#!/usr/bin/env python3
"""API + store tests for the fleet console (P4).

Covered:
  * POST /api/v1/register mints deployment_id (+ tenant_id when absent) and the
    registered deployment shows up green on GET /api/v1/deployments.
  * POST /api/v1/heartbeat is an UPSERT keyed by deployment_id (same id twice =
    one row, fields replaced) and recomputes health.
  * Health DERIVATION on read: a fresh heartbeat is green; a heartbeat aged past
    the yellow threshold drifts yellow; past the red threshold drifts red — even
    though the wire record claimed green (the console never trusts wire health).
  * An expired license forces red regardless of freshness.
  * GET / renders the HTML rollup (table + the registered deployment).

These run with the in-process FastAPI TestClient against a NON-persistent store
so they never touch console/data/.
"""

from __future__ import annotations

import datetime as _dt
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Make console/ (for app.py, store.py) and the control-plane root (for lib)
# importable.
_HERE = Path(__file__).resolve().parent
_CONSOLE = _HERE.parent
for _p in (_CONSOLE,):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from app import create_app  # noqa: E402
from store import DeploymentStore  # noqa: E402

from lib.deployment import DeploymentRecord, Health  # noqa: E402
from lib.primitives import to_rfc3339, utcnow  # noqa: E402


# --- fixtures ---------------------------------------------------------------


# The write-path bearer token used across these tests (I4). The client fixture
# builds the app WITH this token and auto-attaches it on writes, so the existing
# happy-path tests authenticate; dedicated tests below exercise the 401/503 paths.
TEST_TOKEN = "dummy-test-ingest-token"


@pytest.fixture()
def store(tmp_path) -> DeploymentStore:
    """A non-persistent store with the NFR-5 thresholds (yellow>90s, red>300s)."""
    return DeploymentStore(data_dir=tmp_path, persist=False)


class _AuthClient(TestClient):
    """A TestClient that auto-attaches the write-path bearer on POST/PUT/DELETE so
    the happy-path tests don't repeat the header. Reads go through unauthenticated.
    """

    def request(self, method, url, *args, **kwargs):  # type: ignore[override]
        if method.upper() in ("POST", "PUT", "DELETE", "PATCH"):
            headers = dict(kwargs.get("headers") or {})
            headers.setdefault("Authorization", f"Bearer {TEST_TOKEN}")
            kwargs["headers"] = headers
        return super().request(method, url, *args, **kwargs)


@pytest.fixture()
def client(store) -> TestClient:
    return _AuthClient(create_app(store, ingest_token=TEST_TOKEN))


def _record_dict(
    *,
    tenant_id: str,
    deployment_id: str,
    last_heartbeat_ts,
    license_expiry,
    version: str = "1.4.2",
    region: str = "us-east-1",
    health: str = "green",
    telemetry_tier: str = "T1",
) -> dict:
    return {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "version": version,
        "region": region,
        "last_heartbeat_ts": to_rfc3339(last_heartbeat_ts),
        "health": health,
        "license_expiry": to_rfc3339(license_expiry),
        "telemetry_tier": telemetry_tier,
    }


# --- register ---------------------------------------------------------------


def test_register_mints_ids_and_shows_green(client: TestClient):
    resp = client.post(
        "/api/v1/register",
        json={"tenant_id": "acme", "region": "us-east-1", "plan": "enterprise"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["tenant_id"] == "acme"
    dep_id = body["deployment_id"]
    assert dep_id, "a deployment_id must be minted"
    assert dep_id.startswith("acme-"), dep_id  # shape: <tenant>-<region>-<rand>

    # It shows up on the fleet list, GREEN (fresh heartbeat at register time).
    lst = client.get("/api/v1/deployments").json()
    assert len(lst) == 1
    row = lst[0]
    assert row["deployment_id"] == dep_id
    assert row["tenant_id"] == "acme"
    assert row["health"] == "green"
    assert row["telemetry_tier"] == "T1"


def test_register_mints_tenant_id_when_absent(client: TestClient):
    resp = client.post("/api/v1/register", json={"region": "eu-west-1", "plan": "pro"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["tenant_id"], "tenant_id must be minted when absent"
    assert body["deployment_id"]
    # The minted tenant prefixes the deployment id.
    assert body["deployment_id"].startswith(body["tenant_id"] + "-")


def test_register_requires_region(client: TestClient):
    resp = client.post("/api/v1/register", json={"tenant_id": "acme"})
    assert resp.status_code == 422  # region is required


# --- heartbeat upsert -------------------------------------------------------


def test_heartbeat_inserts_then_upserts(client: TestClient):
    now = utcnow()
    rec = _record_dict(
        tenant_id="acme",
        deployment_id="acme-use1-aaaa",
        last_heartbeat_ts=now,
        license_expiry=now + _dt.timedelta(days=365),
        version="1.0.0",
    )
    r1 = client.post("/api/v1/heartbeat", json=rec)
    assert r1.status_code == 200, r1.text
    assert client.get("/api/v1/deployments").json().__len__() == 1

    # Same deployment_id, new version + fresh heartbeat -> UPSERT (still 1 row).
    rec2 = dict(rec)
    rec2["version"] = "1.0.1"
    rec2["last_heartbeat_ts"] = to_rfc3339(utcnow())
    r2 = client.post("/api/v1/heartbeat", json=rec2)
    assert r2.status_code == 200, r2.text

    lst = client.get("/api/v1/deployments").json()
    assert len(lst) == 1, "heartbeat must upsert by deployment_id, not append"
    assert lst[0]["version"] == "1.0.1"
    assert lst[0]["health"] == "green"


def test_get_one_deployment_and_404(client: TestClient):
    now = utcnow()
    rec = _record_dict(
        tenant_id="globex",
        deployment_id="globex-euw1-bbbb",
        last_heartbeat_ts=now,
        license_expiry=now + _dt.timedelta(days=10),
    )
    client.post("/api/v1/heartbeat", json=rec)

    got = client.get("/api/v1/deployments/globex-euw1-bbbb")
    assert got.status_code == 200
    assert got.json()["tenant_id"] == "globex"

    missing = client.get("/api/v1/deployments/nope-x-0000")
    assert missing.status_code == 404


# --- deregister (DELETE) ----------------------------------------------------


def test_delete_deployment_removes_row(client: TestClient):
    """DELETE /api/v1/deployments/{id} removes the row (FR-E onboarding rollback)."""
    resp = client.post(
        "/api/v1/register", json={"tenant_id": "acme", "region": "us-east-1"}
    )
    dep_id = resp.json()["deployment_id"]
    assert len(client.get("/api/v1/deployments").json()) == 1

    d = client.delete(f"/api/v1/deployments/{dep_id}")
    assert d.status_code == 200, d.text
    assert d.json()["removed"] is True
    assert d.json()["deployment_id"] == dep_id

    # The row is gone: list is empty and GET is a 404.
    assert client.get("/api/v1/deployments").json() == []
    assert client.get(f"/api/v1/deployments/{dep_id}").status_code == 404


def test_delete_deployment_is_idempotent(client: TestClient):
    """Deleting an absent (or already-deleted) deployment is NOT an error — a
    retried rollback must not blow up. The endpoint answers 200 removed=false."""
    # Never-existed id.
    d0 = client.delete("/api/v1/deployments/never-existed-0000")
    assert d0.status_code == 200, d0.text
    assert d0.json()["removed"] is False

    # Register, delete twice: first removes, second is a no-op (still 200).
    dep_id = client.post(
        "/api/v1/register", json={"tenant_id": "globex", "region": "eu-west-1"}
    ).json()["deployment_id"]
    assert client.delete(f"/api/v1/deployments/{dep_id}").json()["removed"] is True
    d2 = client.delete(f"/api/v1/deployments/{dep_id}")
    assert d2.status_code == 200
    assert d2.json()["removed"] is False
    assert client.get("/api/v1/deployments").json() == []


# --- write-path authentication (I4) -----------------------------------------


def _raw_client(store, *, ingest_token: str | None = TEST_TOKEN) -> TestClient:
    """A PLAIN TestClient (no auto-auth) so a test controls headers explicitly."""
    return TestClient(create_app(store, ingest_token=ingest_token))


def test_writes_without_token_are_401(store: DeploymentStore):
    """register/heartbeat/delete with NO Authorization header -> 401."""
    c = _raw_client(store)  # token configured, but we send no header
    now = utcnow()
    rec = _record_dict(
        tenant_id="acme",
        deployment_id="acme-use1-noauth",
        last_heartbeat_ts=now,
        license_expiry=now + _dt.timedelta(days=365),
    )
    assert c.post("/api/v1/register", json={"region": "us-east-1"}).status_code == 401
    assert c.post("/api/v1/heartbeat", json=rec).status_code == 401
    assert c.delete("/api/v1/deployments/acme-use1-noauth").status_code == 401
    # Nothing was written.
    assert c.get("/api/v1/deployments").json() == []


def test_writes_with_wrong_token_are_401(store: DeploymentStore):
    c = _raw_client(store)
    bad = {"Authorization": "Bearer not-the-token"}
    assert c.post("/api/v1/register", json={"region": "us-east-1"}, headers=bad).status_code == 401
    # A malformed (non-Bearer) header is also 401.
    worse = {"Authorization": "Basic Zm9vOmJhcg=="}
    assert c.post("/api/v1/register", json={"region": "us-east-1"}, headers=worse).status_code == 401


def test_writes_with_token_are_200(store: DeploymentStore):
    """With the correct bearer, register + heartbeat + delete all succeed."""
    c = _raw_client(store)
    auth = {"Authorization": f"Bearer {TEST_TOKEN}"}
    r = c.post("/api/v1/register", json={"tenant_id": "acme", "region": "us-east-1"}, headers=auth)
    assert r.status_code == 200, r.text
    dep_id = r.json()["deployment_id"]

    now = utcnow()
    rec = _record_dict(
        tenant_id="acme",
        deployment_id=dep_id,
        last_heartbeat_ts=now,
        license_expiry=now + _dt.timedelta(days=365),
    )
    assert c.post("/api/v1/heartbeat", json=rec, headers=auth).status_code == 200
    assert c.delete(f"/api/v1/deployments/{dep_id}", headers=auth).status_code == 200


def test_reads_stay_open_without_token(store: DeploymentStore):
    """Reads (list/get/rollup/healthz) do NOT require the token (operator UI)."""
    c = _raw_client(store)
    # Seed a row via an authenticated write first.
    auth = {"Authorization": f"Bearer {TEST_TOKEN}"}
    dep_id = c.post(
        "/api/v1/register", json={"tenant_id": "acme", "region": "us-east-1"}, headers=auth
    ).json()["deployment_id"]
    # Reads with NO Authorization header all succeed.
    assert c.get("/api/v1/deployments").status_code == 200
    assert c.get(f"/api/v1/deployments/{dep_id}").status_code == 200
    assert c.get("/").status_code == 200
    assert c.get("/healthz").status_code == 200


def test_writes_fail_closed_when_token_unconfigured(store: DeploymentStore):
    """A console built with NO token (and no env) refuses ALL writes with 503 —
    fail-closed, never silently open."""
    c = _raw_client(store, ingest_token="")  # explicitly unconfigured
    auth = {"Authorization": "Bearer anything"}
    assert c.post("/api/v1/register", json={"region": "us-east-1"}, headers=auth).status_code == 503
    assert c.get("/api/v1/deployments").status_code == 200  # reads still fine


def test_heartbeat_rejects_malformed_record(client: TestClient):
    # Missing required fields -> 422 (the agent's bug surfaces, console stays up).
    r = client.post("/api/v1/heartbeat", json={"tenant_id": "acme"})
    assert r.status_code == 422


def test_heartbeat_bad_tier_is_422_not_500(client: TestClient):
    """A bad telemetry_tier raises a typed TierError inside the record validator;
    the console must surface that as a 422, never a 500."""
    now = utcnow()
    bad = _record_dict(
        tenant_id="acme",
        deployment_id="acme-use1-tier",
        last_heartbeat_ts=now,
        license_expiry=now + _dt.timedelta(days=365),
        telemetry_tier="T9",  # not T1|T2|T3
    )
    r = client.post("/api/v1/heartbeat", json=bad)
    assert r.status_code == 422, r.text


def test_heartbeat_non_object_body_is_422(client: TestClient):
    r = client.post("/api/v1/heartbeat", json=["not", "an", "object"])
    assert r.status_code == 422


# --- health derivation on read ----------------------------------------------


def test_stale_heartbeat_derives_yellow(store: DeploymentStore):
    """A heartbeat aged past the yellow threshold reads yellow on the store,
    even though the wire record was stamped green."""
    now = utcnow()
    stale = now - _dt.timedelta(seconds=120)  # > 90s, <= 300s
    rec = DeploymentRecord(
        **_record_dict(
            tenant_id="acme",
            deployment_id="acme-use1-cccc",
            last_heartbeat_ts=stale,
            license_expiry=now + _dt.timedelta(days=365),
            health="green",  # the wire LIES; the store must not trust it
        )
    )
    store.upsert(rec)
    out = store.record("acme-use1-cccc", now=now)
    assert out is not None
    assert out.health is Health.YELLOW, "stale (>90s) must derive yellow"


def test_missing_heartbeat_derives_red(store: DeploymentStore):
    now = utcnow()
    missing = now - _dt.timedelta(seconds=600)  # > 300s
    rec = DeploymentRecord(
        **_record_dict(
            tenant_id="acme",
            deployment_id="acme-use1-dddd",
            last_heartbeat_ts=missing,
            license_expiry=now + _dt.timedelta(days=365),
            health="green",
        )
    )
    store.upsert(rec)
    out = store.record("acme-use1-dddd", now=now)
    assert out is not None
    assert out.health is Health.RED, "missing (>300s) must derive red"


def test_expired_license_forces_red_even_when_fresh(store: DeploymentStore):
    now = utcnow()
    rec = DeploymentRecord(
        **_record_dict(
            tenant_id="acme",
            deployment_id="acme-use1-eeee",
            last_heartbeat_ts=now,  # perfectly fresh
            license_expiry=now - _dt.timedelta(seconds=1),  # but expired
            health="green",
        )
    )
    store.upsert(rec)
    out = store.record("acme-use1-eeee", now=now)
    assert out is not None
    assert out.health is Health.RED, "expired license must force red"


def test_register_then_advance_time_drifts_health(client: TestClient):
    """End-to-end self-test flavor: register green, then advancing 'now' past the
    thresholds (via the store's derive-on-read) drifts yellow then red."""
    store: DeploymentStore = client.app.state.store
    resp = client.post(
        "/api/v1/register", json={"tenant_id": "acme", "region": "us-east-1"}
    )
    dep_id = resp.json()["deployment_id"]

    # Fresh: green.
    rec = store.record(dep_id)
    base = rec.last_heartbeat_ts

    # +120s -> yellow.
    yellow = store.record(dep_id, now=base + _dt.timedelta(seconds=120))
    assert yellow.health is Health.YELLOW

    # +600s -> red.
    red = store.record(dep_id, now=base + _dt.timedelta(seconds=600))
    assert red.health is Health.RED


# --- HTML rollup ------------------------------------------------------------


def test_root_renders_html_rollup(client: TestClient):
    now = utcnow()
    client.post(
        "/api/v1/heartbeat",
        json=_record_dict(
            tenant_id="acme",
            deployment_id="acme-use1-ffff",
            last_heartbeat_ts=now,
            license_expiry=now + _dt.timedelta(days=365),
        ),
    )
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    html = r.text
    assert "Fyralis BYOC" in html
    assert "<table" in html
    assert "acme-use1-ffff" in html  # the deployment row is in the table
    assert "GREEN" in html  # its health badge


def test_root_empty_fleet_renders(client: TestClient):
    r = client.get("/")
    assert r.status_code == 200
    assert "No deployments registered yet" in r.text


# --- persistence round-trip -------------------------------------------------


def test_persistence_round_trip(tmp_path):
    """A persistent store reloads its rows from console/data on restart."""
    s1 = DeploymentStore(data_dir=tmp_path, persist=True)
    now = utcnow()
    s1.upsert(
        DeploymentRecord(
            **_record_dict(
                tenant_id="acme",
                deployment_id="acme-use1-pers",
                last_heartbeat_ts=now,
                license_expiry=now + _dt.timedelta(days=365),
            )
        )
    )
    assert (tmp_path / "fleet_registry.json").is_file()

    s2 = DeploymentStore(data_dir=tmp_path, persist=True)
    assert "acme-use1-pers" in s2
    assert len(s2) == 1
