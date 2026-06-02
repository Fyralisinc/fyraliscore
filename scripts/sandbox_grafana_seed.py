#!/usr/bin/env python3
"""scripts/sandbox_grafana_seed.py — onboard a REAL Grafana instance (IN-GRAFANA).

This is the operator handoff step: once you supply Grafana credentials in env
(GRAFANA_BASE_URL / GRAFANA_TOKEN, optionally GRAFANA_ORG_ID and
GRAFANA_WEBHOOK_SECRET), run this to wire the integration so backfill + live
ingestion start:

  1. Verify the service-account token with GET /api/org.
  2. Store the SA token in the envelope-encrypted secret store -> secret_ref.
  3. finalize_install(): UPSERT grafana_installations + emit the
     onboarding_triggers row (source='grafana') that fires the M6 backfill chain
     (the org-wide annotations shard).
  4. If GRAFANA_WEBHOOK_SECRET is set: store it + register the
     provider_installations row (provider='grafana', installation_id=instance
     host) so the Alerting-webhook edge resolves the tenant and verifies the
     X-Grafana-Alerting-Signature HMAC (the LIVE alert path).
  5. Flip ingestion.kafka_path_enabled=TRUE for the tenant so observations
     actually persist (the full-pipeline gate).

Run inside the sandbox stack so it shares DATABASE_URL + MASTER_KEK:

    python scripts/sandbox_grafana_seed.py --tenant <TENANT_UUID>

Env (or flags) consumed:
    GRAFANA_BASE_URL, GRAFANA_TOKEN, GRAFANA_ORG_ID, GRAFANA_WEBHOOK_SECRET,
    DATABASE_URL, COMPANY_OS_TENANT_ID.

Notes:
  - GRAFANA_TOKEN is a Grafana SERVICE-ACCOUNT token (glsa_...) for an account
    whose role includes `annotations:read` (Viewer is enough). API keys were
    deprecated in 2025 — use a service account.
  - GRAFANA_WEBHOOK_SECRET is the HMAC shared secret you set on the Alerting
    webhook contact point (requires Grafana 12.0+). Leave unset for
    backfill-only (no live alerts).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
from uuid import UUID

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import asyncpg


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sandbox_grafana_seed")
    p.add_argument("--tenant", default=os.environ.get("COMPANY_OS_TENANT_ID"))
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--base-url", default=os.environ.get("GRAFANA_BASE_URL"))
    p.add_argument("--token", default=os.environ.get("GRAFANA_TOKEN"))
    p.add_argument("--org-id", default=os.environ.get("GRAFANA_ORG_ID", "1"))
    p.add_argument("--webhook-secret", default=os.environ.get("GRAFANA_WEBHOOK_SECRET"))
    return p.parse_args()


async def _run(args: argparse.Namespace) -> int:
    missing = [
        n for n, v in (
            ("--tenant/COMPANY_OS_TENANT_ID", args.tenant),
            ("--dsn/DATABASE_URL", args.dsn),
            ("--base-url/GRAFANA_BASE_URL", args.base_url),
            ("--token/GRAFANA_TOKEN", args.token),
        ) if not v
    ]
    if missing:
        print(f"ERROR: missing required values: {', '.join(missing)}", file=sys.stderr)
        return 1

    tenant_id = UUID(args.tenant)
    base_url = args.base_url.rstrip("/")
    org_id = str(args.org_id or "1")

    from lib.shared.secrets import build_secret_store
    from services.app.gateway.db_bootstrap import _register_codecs
    from services.ingest.integrations.grafana.client import GrafanaClient
    from services.ingest.integrations.grafana.onboarding import (
        finalize_install, register_webhook_installation,
    )
    from services.ingest.ingestion.feature_flags.client import (
        KAFKA_PATH_ENABLED, TenantFlags,
    )

    pool = await asyncpg.create_pool(dsn=args.dsn, min_size=1, max_size=3, init=_register_codecs)
    try:
        # 1. Verify the token with a live org probe.
        probe = GrafanaClient(base_url=base_url, api_token=args.token)
        try:
            org = await probe.get_org()
        finally:
            await probe.aclose()
        print(f"OK: authenticated against Grafana org {org.get('name')!r} (id={org.get('id')})")

        # 2. Store the service-account token in the encrypted secret store.
        store = build_secret_store(pool)  # reads MASTER_KEK from env
        secret_ref = await store.put(
            args.token, label=f"grafana_sa_token:{base_url}", tenant_id=tenant_id,
        )

        # 3 + 5. finalize_install (+ webhook secret) — install + onboarding trigger.
        webhook_secret_ref = None
        if args.webhook_secret:
            webhook_secret_ref = await store.put(
                args.webhook_secret, label=f"grafana_webhook_secret:{base_url}",
                tenant_id=tenant_id,
            )
        install_id = await finalize_install(
            pool, tenant_id=tenant_id, base_url=base_url, org_id=org_id,
            secret_ref=secret_ref, webhook_secret_ref=webhook_secret_ref,
        )
        print(f"OK: grafana_installations row {install_id} + onboarding trigger emitted.")

        # 4. Webhook (live alert) path registration.
        if webhook_secret_ref:
            await register_webhook_installation(
                pool, tenant_id=tenant_id, base_url=base_url,
                webhook_secret_ref=webhook_secret_ref,
            )
            print("OK: provider_installations row registered for the Alerting webhook edge.")
            print("    Point a webhook contact point at:  <gateway>/webhooks/grafana/events")
            print("    with HMAC signing enabled using GRAFANA_WEBHOOK_SECRET (Grafana 12.0+).")
        else:
            print("NOTE: GRAFANA_WEBHOOK_SECRET unset -> backfill-only (no live alerts).")

        # 6. Flip the full-pipeline gate so observations persist.
        flags = TenantFlags(pool)
        await flags.set_bool(tenant_id, KAFKA_PATH_ENABLED, True,
                             set_by="operator:grafana_seed",
                             note="IN-GRAFANA grafana onboarding")
        print("OK: ingestion.kafka_path_enabled=TRUE for the tenant.")
        print("\nDone. The backfill chain will pick up the trigger on its next "
              "tick and walk GET /api/annotations; live alerts (if configured) "
              "flow through the pipeline.")
        return 0
    finally:
        await pool.close()


def main() -> None:
    raise SystemExit(asyncio.run(_run(_parse_args())))


if __name__ == "__main__":
    main()
