"""services/gateway/finance_router.py — finance-source testing control plane.

Powers the `/finance` UI panel: a developer surface to drive the Mercury +
QuickBooks ingestion sources end-to-end without real provider credentials.

Three controls per source, all scoped by `X-Tenant-Id` (falls back to
`COMPANY_OS_TENANT_ID` in dev — same convention as debug_router):

  POST /finance/{source}/install
      Provision the dedicated install + sub-resource rows (mercury_accounts /
      quickbooks_entities), register the live-webhook provider_installations row
      (with an HMAC secret stored in encrypted_secrets), ensure observation
      partitions, and emit the onboarding_triggers row so the full M6 backfill
      chain is also available. Idempotent.

  POST /finance/{source}/backfill
      Synthesize a batch of HISTORICAL records and run them through the REAL
      per-source handler via inline `ingest()` — exactly the path the sandbox
      proves (S3→Kafka is exercised separately by the workers). Deterministic:
      returns the per-record ingest result (observation_id / deduped).

  POST /finance/{source}/live/emit
      Synthesize ONE fresh event and POST it, HMAC-signed, to the gateway's own
      `/webhooks/{source}/events` edge — exercising the genuine live path
      (signature verify → tenant resolve → ingest/cutover). Call repeatedly (the
      UI auto-loop) to drive live traffic CONCURRENTLY with a running backfill.

  GET /finance/{source}/status
      Observation counts (by kind) + install state + the last N observations, so
      the UI can show progress as backfill + live land rows.

This router is mounted in `build_app()` and `/finance/` is a public path prefix
(tenant via header, no bearer) — it is a dev/testing tool, env-gated by
COMPANY_OS_ENV at mount time.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse


log = structlog.get_logger("gateway.finance")


_SOURCES = ("mercury", "quickbooks")
_CHANNEL = {"mercury": "mercury:transaction", "quickbooks": "quickbooks:object"}
_MERCURY_BASE = "https://api.mercury.com/api/v1"
_QBO_BASE = "https://sandbox-quickbooks.api.intuit.com"


# ---------------------------------------------------------------------
# Helpers (mirror debug_router)
# ---------------------------------------------------------------------

def _resolve_tenant(req: Request) -> UUID:
    hdr = req.headers.get("X-Tenant-Id")
    if hdr:
        try:
            return UUID(hdr)
        except Exception:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="invalid X-Tenant-Id")
    env_tid = os.environ.get("DEFAULT_TENANT_ID") or os.environ.get("COMPANY_OS_TENANT_ID")
    if env_tid:
        try:
            return UUID(env_tid)
        except Exception:  # noqa: BLE001
            pass
    raise HTTPException(status_code=400, detail="tenant_id missing")


def _deps(req: Request):  # type: ignore[no-untyped-def]
    deps = getattr(req.app.state, "deps", None)
    if deps is None:
        raise HTTPException(status_code=500, detail="gateway deps unavailable")
    return deps


def _pool(req: Request) -> asyncpg.Pool:
    deps = _deps(req)
    return deps.pool


def _require_source(source: str) -> None:
    if source not in _SOURCES:
        raise HTTPException(status_code=404, detail=f"unknown finance source {source!r}")


async def _ensure_tenant(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
        tenant_id, f"finance-{tenant_id.hex[:8]}",
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _iso_qbo(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S-08:00")


# ---------------------------------------------------------------------
# Synthetic data generators
# ---------------------------------------------------------------------

_MERCURY_ACCOUNTS = [
    {"account_id": "acc-checking", "account_name": "Operating Checking",
     "account_kind": "checking"},
    {"account_id": "acc-savings", "account_name": "Reserve Savings",
     "account_kind": "savings"},
]

_MERCURY_COUNTERPARTIES = [
    ("Acme Cloud", -4200.00, "externalTransfer"),
    ("Stripe Payout", 86000.00, "incomingPayment"),
    ("AWS", -1850.00, "externalTransfer"),
    ("Payroll Run", -52000.00, "externalTransfer"),
    ("Customer Wire — Globex", 24000.00, "incomingPayment"),
]

_QBO_ENTITIES = ("Invoice", "Bill", "BillPayment", "Payment")


def _mercury_backfill_records(account_id: str, n: int, seed: int) -> list[dict]:
    """N historical transaction records + one balance snapshot, handler-shaped."""
    recs: list[dict] = []
    base = _now()
    # Balance snapshot first (cash position).
    recs.append({
        "_fyralis_record_type": "account_snapshot",
        "_fyralis_account_id": account_id,
        "as_of": _iso_z(base),
        "account": {
            "id": account_id,
            "name": next((a["account_name"] for a in _MERCURY_ACCOUNTS
                          if a["account_id"] == account_id), account_id),
            "type": "checking",
            "availableBalance": round(500000 - seed * 1375.5, 2),
            "currentBalance": round(512000 - seed * 1375.5, 2),
        },
    })
    for i in range(n):
        cp, amount, kind = _MERCURY_COUNTERPARTIES[(seed + i) % len(_MERCURY_COUNTERPARTIES)]
        created = _iso_z(base - timedelta(days=n - i, hours=(seed + i) % 12))
        recs.append({
            "_fyralis_record_type": "transaction",
            "_fyralis_account_id": account_id,
            "transaction": {
                "id": f"txn-{account_id}-{seed}-{i}",
                "amount": amount,
                "counterpartyName": cp,
                "status": "sent",
                "kind": kind,
                "createdAt": created,
                "postedAt": created,
                "bankDescription": f"{cp} {kind}",
            },
        })
    return recs


def _mercury_live_event(account_id: str, seq: int) -> tuple[dict, str]:
    """One live transaction.created webhook body + the organization id."""
    cp, amount, kind = _MERCURY_COUNTERPARTIES[seq % len(_MERCURY_COUNTERPARTIES)]
    # Alternate a failed payment in to exercise the state_change path.
    status = "failed" if seq % 4 == 3 else "sent"
    return {
        "type": "transaction.created",
        "organizationId": _org_id_for(account_id),
        "accountId": account_id,
        "transaction": {
            "id": f"txn-live-{account_id}-{seq}",
            "amount": amount,
            "counterpartyName": cp,
            "status": status,
            "kind": kind,
            "createdAt": _iso_z(_now()),
        },
    }, _org_id_for(account_id)


def _org_id_for(_account_id: str) -> str:
    # One organization per tenant install; stable id used for webhook resolution.
    return "fin-mercury-org"


def _qbo_backfill_records(entity: str, realm_id: str, n: int, seed: int) -> list[dict]:
    recs: list[dict] = []
    base = _now()
    for i in range(n):
        eid = f"{entity[:3].lower()}-{seed}-{i}"
        updated = _iso_qbo(base - timedelta(days=n - i))
        if entity == "Invoice":
            overdue = (seed + i) % 3 == 0
            due = (base - timedelta(days=2)) if overdue else (base + timedelta(days=20))
            entity_obj = {
                "Id": eid, "SyncToken": "0", "DocNumber": f"INV-{seed}{i}",
                "TotalAmt": round(2000 + i * 1500.0, 2),
                "Balance": round(2000 + i * 1500.0, 2),
                "CustomerRef": {"value": "1", "name": ["Globex", "Initech", "Hooli"][(seed + i) % 3]},
                "TxnDate": (base - timedelta(days=20)).strftime("%Y-%m-%d"),
                "DueDate": due.strftime("%Y-%m-%d"),
                "MetaData": {"LastUpdatedTime": updated},
            }
        elif entity == "Bill":
            entity_obj = {
                "Id": eid, "SyncToken": "0",
                "TotalAmt": round(800 + i * 600.0, 2),
                "Balance": round(800 + i * 600.0, 2),
                "VendorRef": {"value": "7", "name": ["AWS", "Datadog", "Figma"][(seed + i) % 3]},
                "TxnDate": (base - timedelta(days=15)).strftime("%Y-%m-%d"),
                "MetaData": {"LastUpdatedTime": updated},
            }
        elif entity == "BillPayment":
            entity_obj = {
                "Id": eid, "SyncToken": "0",
                "TotalAmt": round(800 + i * 600.0, 2),
                "VendorRef": {"value": "7", "name": "AWS"},
                "MetaData": {"LastUpdatedTime": updated},
            }
        else:  # Payment
            entity_obj = {
                "Id": eid, "SyncToken": "0",
                "TotalAmt": round(3000 + i * 1200.0, 2),
                "CustomerRef": {"value": "1", "name": "Initech"},
                "MetaData": {"LastUpdatedTime": updated},
            }
        recs.append({
            "_fyralis_record_type": entity.lower(),
            "_fyralis_realm_id": realm_id,
            "entity": entity_obj,
        })
    return recs


def _qbo_live_event(realm_id: str, seq: int) -> dict:
    """One live Intuit eventNotifications body."""
    entity = _QBO_ENTITIES[seq % len(_QBO_ENTITIES)]
    return {
        "eventNotifications": [{
            "realmId": realm_id,
            "dataChangeEvent": {"entities": [{
                "name": entity,
                "id": f"live-{seq}",
                "operation": "Update",
                "lastUpdated": _iso_qbo(_now()),
            }]},
        }],
    }


# ---------------------------------------------------------------------
# Inline ingest (the deterministic backfill path)
# ---------------------------------------------------------------------

async def _ingest_record(req: Request, tenant_id: UUID, channel: str, record: dict) -> dict:
    from services.ingestion.core import ingest

    deps = _deps(req)
    res = await ingest(
        channel, record,
        pool=deps.pool,
        tenant_id=tenant_id,
        actor_repo=deps.actor_repo,
        alias_repo=deps.alias_repo,
        embedder=deps.embedder,
    )
    return {
        "observation_id": str(res.observation.id),
        "external_id": res.observation.external_id,
        "deduped": res.deduped,
        "kind": res.observation.kind,
    }


# ---------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------

def build_finance_router() -> APIRouter:
    router = APIRouter(prefix="/finance", tags=["finance"])

    @router.get("/sources")
    async def list_sources() -> dict[str, Any]:
        return {
            "sources": [
                {"source": "mercury", "channel": "mercury:transaction",
                 "label": "Mercury (banking / cash)"},
                {"source": "quickbooks", "channel": "quickbooks:object",
                 "label": "QuickBooks (accounting / AR-AP)"},
            ],
        }

    @router.post("/{source}/install")
    async def install(source: str, req: Request) -> JSONResponse:
        _require_source(source)
        tenant_id = _resolve_tenant(req)
        pool = _pool(req)
        await _ensure_tenant(pool, tenant_id)

        # Ensure observation partitions cover the historical backfill window.
        try:
            from services.observations.partitions import ensure_partitions
            await ensure_partitions(pool, months_ahead=2)
            old = _now() - timedelta(days=60)
            await ensure_partitions(pool, as_of=old.date(), months_ahead=0)
        except Exception as exc:  # noqa: BLE001
            log.warning("finance_partition_ensure_failed", error=str(exc))

        # Store a webhook HMAC secret for the live path.
        secret_value = f"fin-{source}-{uuid4().hex}"
        secret_ref: str | None = None
        try:
            deps = _deps(req)
            store = getattr(req.app.state, "secret_store", None)
            if store is not None:
                secret_ref = await store.put(
                    secret_value, label=f"{source}_webhook_secret", tenant_id=tenant_id,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("finance_secret_put_failed", source=source, error=str(exc))

        if source == "mercury":
            from services.integrations.mercury.onboarding import (
                finalize_install, register_webhook_installation,
            )
            install_id = await finalize_install(
                pool, tenant_id=tenant_id, base_url=_MERCURY_BASE,
                accounts=list(_MERCURY_ACCOUNTS),
                organization_id=_org_id_for(""),
                webhook_secret_ref=secret_ref,
            )
            await register_webhook_installation(
                pool, tenant_id=tenant_id, organization_id=_org_id_for(""),
                webhook_secret_ref=secret_ref,
            )
            sub_count = len(_MERCURY_ACCOUNTS)
        else:
            realm_id = f"realm-{tenant_id.hex[:12]}"
            from services.integrations.quickbooks.onboarding import (
                finalize_install, register_webhook_installation,
            )
            install_id = await finalize_install(
                pool, tenant_id=tenant_id, realm_id=realm_id, base_url=_QBO_BASE,
                entities=list(_QBO_ENTITIES), webhook_secret_ref=secret_ref,
            )
            await register_webhook_installation(
                pool, tenant_id=tenant_id, realm_id=realm_id,
                webhook_secret_ref=secret_ref,
            )
            sub_count = len(_QBO_ENTITIES)

        # Stash the plaintext secret on app.state so live/emit can sign without a
        # second decrypt round-trip (per-process, dev-only).
        cache = getattr(req.app.state, "_finance_secrets", None)
        if cache is None:
            cache = {}
            req.app.state._finance_secrets = cache
        cache[(str(tenant_id), source)] = secret_value

        return JSONResponse({
            "source": source,
            "installation_id": str(install_id),
            "sub_resources": sub_count,
            "webhook_secret_registered": secret_ref is not None,
            "message": f"{source} installed with {sub_count} sub-resources; "
                       "backfill + live ready.",
        }, status_code=201)

    @router.post("/{source}/backfill")
    async def backfill(source: str, req: Request) -> JSONResponse:
        _require_source(source)
        tenant_id = _resolve_tenant(req)
        try:
            body = await req.json()
        except Exception:
            body = {}
        per = int(body.get("count", 5)) if isinstance(body, dict) else 5
        per = max(1, min(50, per))
        seed = int(body.get("seed", 0)) if isinstance(body, dict) else 0
        channel = _CHANNEL[source]

        results: list[dict] = []
        if source == "mercury":
            for acct in _MERCURY_ACCOUNTS:
                for rec in _mercury_backfill_records(acct["account_id"], per, seed):
                    results.append(await _ingest_record(req, tenant_id, channel, rec))
        else:
            realm_id = f"realm-{tenant_id.hex[:12]}"
            for entity in _QBO_ENTITIES:
                for rec in _qbo_backfill_records(entity, realm_id, per, seed):
                    results.append(await _ingest_record(req, tenant_id, channel, rec))

        new = sum(1 for r in results if not r["deduped"])
        deduped = sum(1 for r in results if r["deduped"])
        return JSONResponse({
            "source": source,
            "records": len(results),
            "ingested": new,
            "deduped": deduped,
            "results": results[:50],
            "message": f"backfill ingested {new} new observations "
                       f"({deduped} deduped) across {channel}.",
        }, status_code=201)

    @router.post("/{source}/live/emit")
    async def live_emit(source: str, req: Request) -> JSONResponse:
        """Synthesize one live event and POST it, HMAC-signed, to the gateway's
        own webhook edge. Falls back to inline ingest if the self-call fails."""
        _require_source(source)
        tenant_id = _resolve_tenant(req)
        try:
            body = await req.json()
        except Exception:
            body = {}
        seq = int(body.get("seq", 0)) if isinstance(body, dict) else 0

        if source == "mercury":
            payload, _org = _mercury_live_event("acc-checking", seq)
            header_name = "Mercury-Signature"
            sig_prefix = "sha256="
        else:
            realm_id = f"realm-{tenant_id.hex[:12]}"
            payload = _qbo_live_event(realm_id, seq)
            header_name = "intuit-signature"
            sig_prefix = ""  # QBO is base64, no prefix

        raw = json.dumps(payload).encode("utf-8")
        cache = getattr(req.app.state, "_finance_secrets", {}) or {}
        secret = cache.get((str(tenant_id), source))

        delivered_via = None
        webhook_status = None
        webhook_body: Any = None
        if secret:
            mac = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256)
            if source == "mercury":
                signature = sig_prefix + mac.hexdigest()
            else:
                import base64
                signature = base64.b64encode(mac.digest()).decode("ascii")
            port = os.environ.get("GATEWAY_SELF_PORT", "8000")
            url = f"http://127.0.0.1:{port}/webhooks/{source}/events"
            try:
                async with httpx.AsyncClient(timeout=15.0) as client:
                    resp = await client.post(
                        url, content=raw,
                        headers={"content-type": "application/json",
                                 header_name: signature,
                                 "X-Tenant-Id": str(tenant_id)},
                    )
                webhook_status = resp.status_code
                try:
                    webhook_body = resp.json()
                except Exception:  # noqa: BLE001
                    webhook_body = resp.text[:200]
                if resp.status_code in (200, 201, 202):
                    delivered_via = "webhook"
            except Exception as exc:  # noqa: BLE001
                log.warning("finance_live_webhook_failed", source=source, error=str(exc))

        # Fallback: inline-ingest the same webhook-shaped payload through the real
        # handler (its live-webhook branch) so the demo never silently no-ops.
        inline_result = None
        if delivered_via is None:
            inline_result = await _ingest_record(req, tenant_id, _CHANNEL[source], payload)
            delivered_via = "inline_fallback"

        return JSONResponse({
            "source": source,
            "delivered_via": delivered_via,
            "webhook_status": webhook_status,
            "webhook_response": webhook_body,
            "inline_result": inline_result,
            "payload_kind": ("transaction.created" if source == "mercury"
                             else "eventNotifications"),
        }, status_code=201)

    @router.get("/{source}/status")
    async def status(source: str, req: Request) -> dict[str, Any]:
        _require_source(source)
        tenant_id = _resolve_tenant(req)
        pool = _pool(req)
        channel = _CHANNEL[source]

        counts = await pool.fetchrow(
            """
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE kind='signal') AS signal,
                   count(*) FILTER (WHERE kind='state_change') AS state_change
              FROM observations
             WHERE tenant_id = $1 AND source_channel = $2
            """,
            tenant_id, channel,
        )
        recent = await pool.fetch(
            """
            SELECT id, kind, external_id, content_text, occurred_at, ingested_at
              FROM observations
             WHERE tenant_id = $1 AND source_channel = $2
             ORDER BY ingested_at DESC
             LIMIT 15
            """,
            tenant_id, channel,
        )
        # Install state.
        if source == "mercury":
            install = await pool.fetchrow(
                "SELECT id, base_url, organization_id, created_at FROM mercury_installations "
                "WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1", tenant_id,
            )
            subs = await pool.fetch(
                "SELECT account_id, account_name, state FROM mercury_accounts ma "
                "JOIN mercury_installations mi ON ma.mercury_installation_id = mi.id "
                "WHERE mi.tenant_id = $1", tenant_id,
            ) if install else []
        else:
            install = await pool.fetchrow(
                "SELECT id, base_url, realm_id, created_at FROM quickbooks_installations "
                "WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1", tenant_id,
            )
            subs = await pool.fetch(
                "SELECT entity_type, state FROM quickbooks_entities qe "
                "JOIN quickbooks_installations qi ON qe.quickbooks_installation_id = qi.id "
                "WHERE qi.tenant_id = $1", tenant_id,
            ) if install else []

        return {
            "source": source,
            "channel": channel,
            "installed": install is not None,
            "install": {k: (str(v) if isinstance(v, (UUID, datetime)) else v)
                        for k, v in dict(install).items()} if install else None,
            "sub_resources": [dict(s) for s in subs],
            "counts": {
                "total": counts["total"] if counts else 0,
                "signal": counts["signal"] if counts else 0,
                "state_change": counts["state_change"] if counts else 0,
            },
            "recent": [
                {
                    "id": str(r["id"]),
                    "kind": r["kind"],
                    "external_id": r["external_id"],
                    "content_text": r["content_text"],
                    "occurred_at": r["occurred_at"].isoformat() if r["occurred_at"] else None,
                    "ingested_at": r["ingested_at"].isoformat() if r["ingested_at"] else None,
                }
                for r in recent
            ],
        }

    return router


__all__ = ["build_finance_router"]
