#!/usr/bin/env python3
"""scripts/sandbox_jira_seed.py — onboard a REAL Jira Cloud site (IN-17).

This is the operator handoff step: once you supply Jira credentials in env
(JIRA_BASE_URL / JIRA_ACCOUNT_EMAIL / JIRA_API_TOKEN, and optionally
JIRA_WEBHOOK_SECRET), run this to wire the integration so backfill + live
ingestion start:

  1. Verify the credentials with GET /rest/api/3/myself.
  2. Store the API token in the envelope-encrypted secret store -> secret_ref.
  3. Enumerate the site's projects (GET /rest/api/3/project/search), or use
     --projects to pin a subset.
  4. finalize_install(): UPSERT jira_installations + jira_projects + emit the
     onboarding_triggers row (source='jira') that fires the M6 backfill chain.
  5. If JIRA_WEBHOOK_SECRET is set: store it + register the
     provider_installations row (provider='jira') so the webhook edge resolves
     the tenant and verifies HMAC signatures (the LIVE path).
  6. Flip ingestion.kafka_path_enabled=TRUE for the tenant so observations
     actually persist (the full-pipeline gate; see the adding-a-source notes).

Run inside the sandbox stack so it shares DATABASE_URL + MASTER_KEK:

    python scripts/sandbox_jira_seed.py --tenant <TENANT_UUID>

Env (or flags) consumed:
    JIRA_BASE_URL, JIRA_ACCOUNT_EMAIL, JIRA_API_TOKEN, JIRA_WEBHOOK_SECRET,
    DATABASE_URL, COMPANY_OS_TENANT_ID.
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
    p = argparse.ArgumentParser(prog="sandbox_jira_seed")
    p.add_argument("--tenant", default=os.environ.get("COMPANY_OS_TENANT_ID"))
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--base-url", default=os.environ.get("JIRA_BASE_URL"))
    p.add_argument("--email", default=os.environ.get("JIRA_ACCOUNT_EMAIL"))
    p.add_argument("--api-token", default=os.environ.get("JIRA_API_TOKEN"))
    p.add_argument("--webhook-secret", default=os.environ.get("JIRA_WEBHOOK_SECRET"))
    p.add_argument(
        "--projects", default=None,
        help="comma-separated project keys to backfill (default: all visible)",
    )
    return p.parse_args()


async def _run(args: argparse.Namespace) -> int:
    missing = [
        n for n, v in (
            ("--tenant/COMPANY_OS_TENANT_ID", args.tenant),
            ("--dsn/DATABASE_URL", args.dsn),
            ("--base-url/JIRA_BASE_URL", args.base_url),
            ("--email/JIRA_ACCOUNT_EMAIL", args.email),
            ("--api-token/JIRA_API_TOKEN", args.api_token),
        ) if not v
    ]
    if missing:
        print(f"ERROR: missing required values: {', '.join(missing)}", file=sys.stderr)
        return 1

    tenant_id = UUID(args.tenant)
    base_url = args.base_url.rstrip("/")

    from lib.shared.secrets import build_secret_store
    from services.app.gateway.db_bootstrap import _register_codecs
    from services.ingest.integrations.jira.client import JiraClient
    from services.ingest.integrations.jira.onboarding import (
        finalize_install, register_webhook_installation,
    )
    from services.ingest.ingestion.feature_flags.client import (
        KAFKA_PATH_ENABLED, TenantFlags,
    )

    pool = await asyncpg.create_pool(dsn=args.dsn, min_size=1, max_size=3, init=_register_codecs)
    try:
        # 1. Verify credentials with a live probe.
        probe = JiraClient(base_url=base_url, account_email=args.email,
                           api_token=args.api_token)
        try:
            me = await probe.myself()
        finally:
            await probe.aclose()
        print(f"OK: authenticated as {me.get('displayName')} <{me.get('emailAddress')}>")

        # 2. Store the API token in the encrypted secret store.
        store = build_secret_store(pool)  # reads MASTER_KEK from env
        secret_ref = await store.put(
            args.api_token, label=f"jira_api_token:{base_url}", tenant_id=tenant_id,
        )

        # 3. Enumerate projects (or use the pinned subset).
        if args.projects:
            project_keys = [k.strip() for k in args.projects.split(",") if k.strip()]
            meta: dict = {}
        else:
            client = JiraClient(base_url=base_url, account_email=args.email,
                                api_token=args.api_token)
            try:
                project_keys, meta, start = [], {}, 0
                while True:
                    page, nxt, _total = await client.list_projects(start_at=start)
                    for pr in page:
                        project_keys.append(pr["key"])
                        meta[pr["key"]] = {"project_id": pr.get("id"),
                                           "project_name": pr.get("name")}
                    if nxt is None:
                        break
                    start = nxt
            finally:
                await client.aclose()
        print(f"OK: {len(project_keys)} project(s) to backfill: {project_keys}")

        # 4. finalize_install — install + projects + onboarding trigger.
        webhook_secret_ref = None
        if args.webhook_secret:
            webhook_secret_ref = await store.put(
                args.webhook_secret, label=f"jira_webhook_secret:{base_url}",
                tenant_id=tenant_id,
            )
        install_id = await finalize_install(
            pool, tenant_id=tenant_id, base_url=base_url, account_email=args.email,
            project_keys=project_keys, secret_ref=secret_ref,
            webhook_secret_ref=webhook_secret_ref, project_meta=meta,
        )
        print(f"OK: jira_installations row {install_id} + onboarding trigger emitted.")

        # 5. Webhook (live) path registration.
        if webhook_secret_ref:
            await register_webhook_installation(
                pool, tenant_id=tenant_id, base_url=base_url,
                webhook_secret_ref=webhook_secret_ref,
            )
            print("OK: provider_installations row registered for the webhook edge.")
        else:
            print("NOTE: JIRA_WEBHOOK_SECRET unset -> backfill-only (no live webhooks).")

        # 6. Flip the full-pipeline gate so observations persist.
        flags = TenantFlags(pool)
        await flags.set_bool(tenant_id, KAFKA_PATH_ENABLED, True,
                             set_by="operator:jira_seed",
                             note="IN-17 jira onboarding")
        print("OK: ingestion.kafka_path_enabled=TRUE for the tenant.")
        print("\nDone. The backfill chain will pick up the trigger on its next "
              "tick; live webhooks (if configured) flow through the pipeline.")
        return 0
    finally:
        await pool.close()


def main() -> None:
    raise SystemExit(asyncio.run(_run(_parse_args())))


if __name__ == "__main__":
    main()
