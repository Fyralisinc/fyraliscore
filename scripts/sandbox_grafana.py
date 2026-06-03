#!/usr/bin/env python3
"""scripts/sandbox_grafana.py — local end-to-end sandbox for Grafana ingestion
(IN-GRAFANA), with NO real Grafana credentials.

Grafana is an HTTP API (service-account Bearer token) with BOTH a historical pull
surface (GET /api/annotations) and a live push surface (Alerting webhook). This
sandbox stands up a REAL local mock of the Grafana endpoints and drives the REAL
pipeline against it:

    GrafanaClient (real httpx, spammer auth) -> fetch_page_grafana (real backward
    window walk + high-water cursor) -> handle_grafana_annotation (real
    ObservationDraft) -> ingest() (real observation insert + dedup)

It exercises: org probe, install provisioning + onboarding trigger, the org-wide
annotations shard, backfill (manual annotation -> signal; alert-state-change
annotation -> state_change), cross-fetch dedup, the incremental high-water delta,
the live Alerting-webhook path (HMAC verify + the SAME grafana:alert handler ->
state_change), and the reconciler gap probe — then prints the observations.

This is the dry-run that proves the integration end-to-end BEFORE real creds.
When you supply real Grafana creds, `scripts/sandbox_grafana_seed.py` wires the
SAME flow against your live instance (see docs/ingestion/sources/grafana.md).

Database:
  - If DATABASE_URL is set, it is used as-is (migrations applied idempotently).
  - Otherwise a throwaway DB is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_grafana.py
    python scripts/sandbox_grafana.py --keep
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("FYRALIS_ENV", "test")

import asyncpg


_DEFAULT_ADMIN_URL = "postgresql://company_os:company_os@localhost:5434/company_os"
_TENANT_ID = UUID("00000000-0000-0000-0000-0000000060af")  # 'grafana'-ish marker
_BASE_URL = "https://grafana.sandbox.local"
_INSTANCE = "grafana.sandbox.local"
_WEBHOOK_SECRET = "sandbox-grafana-hmac-secret"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _build_fixtures() -> list[dict]:
    now = datetime.now(timezone.utc)
    return [
        # Manual deploy annotation (user-created) -> signal, actor=grafana:user:2.
        {
            "id": 101, "time": _ms(now - timedelta(days=3)),
            "text": "Deployed checkout v2.3", "tags": ["deploy", "prod"],
            "userId": 2, "userName": "alice", "dashboardUID": "dash-checkout",
            "panelId": 4,
        },
        # Alert FIRED (auto annotation) -> state_change, actorless (userId 0).
        {
            "id": 102, "time": _ms(now - timedelta(days=2)),
            "text": "HighErrorRate on checkout", "tags": ["alert"],
            "alertId": 42, "prevState": "Normal", "newState": "Alerting",
            "userId": 0,
        },
        # Alert RESOLVED -> state_change.
        {
            "id": 103, "time": _ms(now - timedelta(days=1)),
            "text": "HighErrorRate resolved", "tags": ["alert"],
            "alertId": 42, "prevState": "Alerting", "newState": "Normal",
            "userId": 0,
        },
    ]


async def _create_throwaway_db(admin_url: str, name: str) -> None:
    admin = await asyncpg.connect(admin_url)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


async def _drop_throwaway_db(admin_url: str, name: str) -> None:
    admin = await asyncpg.connect(admin_url)
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()", name,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    finally:
        await admin.close()


async def _drain_shard(pool, install_row, shard_identifier) -> list[str]:
    """Run the REAL fetcher loop for the org-annotations shard, ingesting each
    record. Returns the external_ids of NON-deduped observations."""
    from services.ingest.ingestion.core import ingest
    from services.ingest.ingestion.fetchers.grafana import fetch_page_grafana

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_grafana(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("grafana:annotation", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.mock_servers.grafana import start_mock_grafana

    fixtures = _build_fixtures()

    # 1. Start the mock; route the spammer single-host base at it (the client
    #    resolves grafana_api -> <base>/grafana, and the mock matches path suffix).
    #    Force all-time backfill so the fixtures (relative to now) always land.
    server, base_url = start_mock_grafana(fixtures)
    os.environ["SYNTHETIC_SOURCE_API_BASE"] = base_url
    os.environ["GRAFANA_BACKFILL_WINDOW_DAYS"] = "0"  # all-time floor (None)
    _hr("MOCK SERVER")
    print(f"  Grafana API base : {base_url} (served under /grafana via spammer routing)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"grafana_sandbox_{uuid4().hex[:8]}"
        await _create_throwaway_db(admin_url, created_db)
        db_url = admin_url.rsplit("/", 1)[0] + "/" + created_db
        _hr("DATABASE"); print(f"  Created throwaway DB: {created_db}")

    from services.app.gateway.db_bootstrap import _register_codecs
    pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5, init=_register_codecs)
    try:
        from lib.shared.migrations import apply_migrations_dir
        from services.domain.observations.partitions import ensure_partitions
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, _REPO_ROOT / "db" / "migrations")
        await ensure_partitions(pool, months_ahead=3)
        await pool.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'grafana-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Org probe via the REAL client (proves connectivity + auth shape).
        _hr("ORG PROBE (GrafanaClient.get_org)")
        from services.ingest.ingestion.fetchers._clients import build_grafana_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID,
                  "base_url": _BASE_URL, "org_id": "1", "secret_ref": None}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_grafana_client(_Inst())
        org = await client.get_org()
        print(f"  org: {org}")
        _check("org probe returned an org", bool(org.get("name")))

        # 3. Provision: grafana_installations + onboarding trigger + the webhook
        #    provider_installations row (live path).
        _hr("PROVISION (grafana.onboarding.finalize_install)")
        from services.ingest.integrations.grafana.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, base_url=_BASE_URL, org_id="1",
        )
        await register_webhook_installation(
            pool, tenant_id=_TENANT_ID, base_url=_BASE_URL, webhook_secret_ref=None,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        prov = await pool.fetchrow(
            "SELECT installation_id FROM provider_installations "
            "WHERE tenant_id=$1 AND provider='grafana'", _TENANT_ID,
        )
        _check("grafana_installations row + onboarding trigger (source=grafana)",
               trig is not None and trig["source"] == "grafana")
        _check("provider_installations webhook row (installation_id=instance host)",
               prov is not None and prov["installation_id"] == _INSTANCE)

        # 4. Plan the shard exactly as SourceOnboarding does.
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.grafana import plan_shards_grafana
        from services.ingest.ingestion.workflows.source_onboarding import _LOAD_GRAFANA_INSTALL_SQL
        install_row = await pool.fetchrow(_LOAD_GRAFANA_INSTALL_SQL, _TENANT_ID)
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_grafana(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_kind for s in shards))
        _check("one org-annotations shard", len(shards) == 1)

        # 5. Backfill: real fetcher -> real ingest.
        _hr("BACKFILL (annotations walk -> ingest)")
        ext = await _drain_shard(pool, install_row, shards[0].shard_identifier)
        print(f"  ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, "
            "count(*) FILTER (WHERE kind='state_change') AS sc, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='grafana:annotation'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']} signal={counts['sig']} state_change={counts['sc']}")
        # 1 deploy annotation (signal) + 2 alert-state annotations (state_change) = 3.
        _check("backfill produced 3 observations (1 signal + 2 state_change)",
               counts["tot"] == 3)
        _check("alert-state-change annotations landed as state_change", counts["sc"] == 2)

        # 6. Dedup: re-ingest a backfilled annotation twin -> deduped.
        _hr("DEDUP (re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = dict(fixtures[0])
        twin["_fyralis_record_type"] = "annotation"
        twin["_fyralis_instance"] = _INSTANCE
        res = await ingest("grafana:annotation", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing annotation dedups (versioned external_id)",
               res.deduped is True)

        # 7. Incremental: append a NEW annotation (newer time) and warm-start the
        #    shard from the high-water -> only the new one lands.
        _hr("INCREMENTAL (high-water delta)")
        hw = await pool.fetchval(
            "SELECT max((content->>'time_ms')::bigint) FROM observations "
            "WHERE tenant_id=$1 AND source_channel='grafana:annotation'", _TENANT_ID,
        )
        fixtures.append({
            "id": 104, "time": _ms(datetime.now(timezone.utc) - timedelta(minutes=5)),
            "text": "LatencyHigh on api", "tags": ["alert"],
            "alertId": 7, "prevState": "Normal", "newState": "Alerting", "userId": 0,
        })
        incr_shard = {"shard_kind": "grafana_org_annotations",
                      "installation_id": str(install_id), "base_url": _BASE_URL,
                      "org_id": "1", "updated_cursor": int(hw)}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observation(s): {incr}")
        _check("incremental delta surfaced exactly the new annotation", len(incr) == 1)

        # 8. LIVE WEBHOOK (alert): HMAC-verify a signed Alerting payload, then run
        #    it through the SAME grafana:alert handler -> a state_change observation.
        _hr("LIVE WEBHOOK (HMAC verify + grafana:alert handler)")
        import json as _json
        from services.app.webhooks.signatures.grafana import verifier
        from services.app.webhooks.verifier import Secret, WebhookVerificationError
        now = datetime.now(timezone.utc)
        alert_payload = {
            "status": "firing",
            "externalURL": _BASE_URL,
            "orgId": 1,
            "groupKey": '{}/{alertname="HighErrorRate"}:{}',
            "commonLabels": {"alertname": "HighErrorRate", "service": "checkout"},
            "commonAnnotations": {"summary": "error rate > 5%"},
            "alerts": [{
                "status": "firing",
                "labels": {"alertname": "HighErrorRate", "service": "checkout"},
                "annotations": {"summary": "error rate > 5%"},
                "startsAt": _iso(now), "endsAt": "0001-01-01T00:00:00Z",
                "fingerprint": "fp-abc123",
            }],
        }
        body = _json.dumps(alert_payload).encode("utf-8")
        good_sig = hmac.new(_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
        secrets = [Secret("grafana", _WEBHOOK_SECRET, label="sandbox")]
        ctx_v = await verifier.verify(
            body=body, headers={"X-Grafana-Alerting-Signature": good_sig}, secrets=secrets,
        )
        _check("HMAC verifier accepts a correctly-signed alert", ctx_v.provider == "grafana")
        tampered = True
        try:
            await verifier.verify(
                body=b'{"status":"resolved"}',
                headers={"X-Grafana-Alerting-Signature": good_sig}, secrets=secrets,
            )
            tampered = False
        except WebhookVerificationError:
            tampered = True
        _check("HMAC verifier rejects a tampered body", tampered)

        res = await ingest("grafana:alert", alert_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("alert webhook lands as a grafana:alert state_change observation",
               res.deduped is False and res.observation.kind == "state_change")

        # 9. Reconciler gap probe against the live (mock) org.
        _hr("RECONCILER GAP PROBE (has_annotations_since)")
        has_updates = await client.has_annotations_since(from_ms=1)
        _check("reconciler probe detects annotations since an old high-water", has_updates is True)

        # 10. Inspect.
        _hr("OBSERVATIONS")
        rows = await pool.fetch(
            "SELECT kind, trust_tier, source_channel, external_id, content_text "
            "FROM observations WHERE tenant_id=$1 ORDER BY occurred_at", _TENANT_ID,
        )
        for r in rows:
            print(f"  [{r['kind']:<12} {r['source_channel']:<19}] {r['external_id']}")
            print(f"       {r['content_text']}")
        print(f"\n  total observations: {len(rows)}")
        _check("all observations are authoritative grafana:*",
               all(r["trust_tier"] == "authoritative"
                   and r["source_channel"].startswith("grafana:") for r in rows))

    finally:
        await pool.close()
        server.shutdown()
        if created_db and not args.keep:
            await _drop_throwaway_db(admin_url, created_db)
            print(f"\n  Dropped throwaway DB {created_db}.")
        elif created_db:
            print(f"\n  Kept throwaway DB {created_db}.")

    _hr("SUMMARY")
    passed = sum(1 for _, ok in _checks if ok)
    for label, ok in _checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    print(f"\n  {passed}/{len(_checks)} checks passed.")
    return 0 if passed == len(_checks) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Grafana ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
