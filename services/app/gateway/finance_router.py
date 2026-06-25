"""services/app/gateway/finance_router.py — finance-source testing control plane.

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


_SOURCES = ("mercury", "quickbooks", "brex", "ramp", "gusto", "deel")
_CHANNEL = {
    "mercury": "mercury:transaction",
    "quickbooks": "quickbooks:object",
    "brex": "brex:transaction",
    "ramp": "ramp:transaction",
    "gusto": "gusto:object",
    "deel": "deel:payment",
}
_MERCURY_BASE = "https://api.mercury.com/api/v1"
_QBO_BASE = "https://sandbox-quickbooks.api.intuit.com"
# IN-FIN2 finance sources. Defaults follow lib/integrations/endpoints.py; each is
# overridable per-install (base_url column) + per-env (<SRC>_API_BASE_URL).
# TODO(human): confirm prod API host/base path for each (endpoints.py §2.7).
_BREX_BASE = "https://platform.brexapis.com"
_RAMP_BASE = "https://api.ramp.com/developer/v1"
_GUSTO_BASE = "https://api.gusto.com"
_DEEL_BASE = "https://api.letsdeel.com"


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
        raise HTTPException(status_code=500, detail="service_unavailable")
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
    categories = ["SaaS", "Travel", "Payroll", "Cloud Infra", "Revenue"]
    for i in range(n):
        cp, amount, kind = _MERCURY_COUNTERPARTIES[(seed + i) % len(_MERCURY_COUNTERPARTIES)]
        created = _iso_z(base - timedelta(days=n - i, hours=(seed + i) % 12))
        txn: dict = {
            "id": f"txn-{account_id}-{seed}-{i}",
            "amount": amount,
            "counterpartyName": cp,
            "counterpartyId": f"cp-{abs(hash(cp)) % 100000}",
            "status": "sent",
            "kind": kind,
            "createdAt": created,
            "postedAt": created,
            "bankDescription": f"{cp} {kind}",
            "externalMemo": f"Memo for {cp}",
            "mercuryCategory": categories[(seed + i) % len(categories)],
            "generalLedgerCodeName": f"GL-{6000 + ((seed + i) % 5) * 10}",
            "estimatedDeliveryDate": _iso_z(base - timedelta(days=n - i - 1)),
        }
        # Rail/counterparty-bank routing — masked by the handler before it lands.
        if kind == "externalTransfer" and amount < 0:
            txn["details"] = {
                "electronicRoutingInfo": {
                    "accountNumber": f"00012345{(seed + i) % 10000:04d}",
                    "routingNumber": "021000021",
                    "bankName": "Acme Partner Bank",
                    "electronicAccountType": "businessChecking",
                },
            }
        recs.append({
            "_fyralis_record_type": "transaction",
            "_fyralis_account_id": account_id,
            "transaction": txn,
        })
    return recs


def _mercury_live_event(account_id: str, seq: int) -> tuple[dict, str]:
    """One live transaction.created webhook body + the organization id."""
    cp, amount, kind = _MERCURY_COUNTERPARTIES[seq % len(_MERCURY_COUNTERPARTIES)]
    # Alternate a failed payment in to exercise the state_change path.
    status = "failed" if seq % 4 == 3 else "sent"
    txn: dict = {
        "id": f"txn-live-{account_id}-{seq}",
        "amount": amount,
        "counterpartyName": cp,
        "counterpartyId": f"cp-{abs(hash(cp)) % 100000}",
        "status": status,
        "kind": kind,
        "createdAt": _iso_z(_now()),
        "mercuryCategory": "Revenue" if amount > 0 else "Operating",
    }
    if status == "failed":
        txn["reasonForFailure"] = "insufficient funds at counterparty bank"
        txn["failedAt"] = _iso_z(_now())
    return {
        "type": "transaction.created",
        "organizationId": _org_id_for(account_id),
        "accountId": account_id,
        "transaction": txn,
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
        _class = ["Sales", "Services", "Marketing"][(seed + i) % 3]
        _dept = ["East", "West"][(seed + i) % 2]
        if entity == "Invoice":
            overdue = (seed + i) % 3 == 0
            due = (base - timedelta(days=2)) if overdue else (base + timedelta(days=20))
            total = round(2000 + i * 1500.0, 2)
            item = ["Platform License", "Onboarding", "Support Plan"][(seed + i) % 3]
            entity_obj = {
                "Id": eid, "SyncToken": "0", "DocNumber": f"INV-{seed}{i}",
                "TotalAmt": total,
                "Balance": total,
                "CustomerRef": {"value": "1", "name": ["Globex", "Initech", "Hooli"][(seed + i) % 3]},
                "TxnDate": (base - timedelta(days=20)).strftime("%Y-%m-%d"),
                "DueDate": due.strftime("%Y-%m-%d"),
                "Line": [{
                    "Id": "1", "LineNum": 1, "Amount": total,
                    "Description": item, "DetailType": "SalesItemLineDetail",
                    "SalesItemLineDetail": {
                        "ItemRef": {"value": "5", "name": item},
                        "Qty": 1, "UnitPrice": total,
                        "ClassRef": {"value": "3", "name": _class},
                    },
                }],
                "TxnTaxDetail": {"TotalTax": round(total * 0.08, 2),
                                 "TaxLine": [{"Amount": round(total * 0.08, 2)}]},
                "ClassRef": {"value": "3", "name": _class},
                "DepartmentRef": {"value": "2", "name": _dept},
                "CurrencyRef": {"value": "USD", "name": "US Dollar"},
                "MetaData": {"LastUpdatedTime": updated},
            }
        elif entity == "Bill":
            total = round(800 + i * 600.0, 2)
            vendor = ["AWS", "Datadog", "Figma"][(seed + i) % 3]
            acct = ["Cloud Infra", "Observability", "Design Tools"][(seed + i) % 3]
            entity_obj = {
                "Id": eid, "SyncToken": "0",
                "TotalAmt": total,
                "Balance": total,
                "VendorRef": {"value": "7", "name": vendor},
                "TxnDate": (base - timedelta(days=15)).strftime("%Y-%m-%d"),
                "Line": [{
                    "Id": "1", "Amount": total, "Description": acct,
                    "DetailType": "AccountBasedExpenseLineDetail",
                    "AccountBasedExpenseLineDetail": {
                        "AccountRef": {"value": "33", "name": acct},
                        "ClassRef": {"value": "3", "name": _class},
                    },
                }],
                "DepartmentRef": {"value": "2", "name": _dept},
                "CurrencyRef": {"value": "USD", "name": "US Dollar"},
                "MetaData": {"LastUpdatedTime": updated},
            }
        elif entity == "BillPayment":
            total = round(800 + i * 600.0, 2)
            entity_obj = {
                "Id": eid, "SyncToken": "0",
                "TotalAmt": total,
                "VendorRef": {"value": "7", "name": "AWS"},
                "PayType": "Check",
                "CheckPayment": {"BankAccountRef": {"value": "35", "name": "Operating Checking"}},
                "LinkedTxn": [{"TxnId": f"bil-{seed}-{i}", "TxnType": "Bill"}],
                "MetaData": {"LastUpdatedTime": updated},
            }
        else:  # Payment
            total = round(3000 + i * 1200.0, 2)
            entity_obj = {
                "Id": eid, "SyncToken": "0",
                "TotalAmt": total,
                "UnappliedAmt": 0.0,
                "CustomerRef": {"value": "1", "name": "Initech"},
                "DepositToAccountRef": {"value": "35", "name": "Operating Checking"},
                "PaymentMethodRef": {"value": "2", "name": "Wire"},
                "PaymentRefNum": f"WIRE-{seed}{i}",
                "Line": [{"Amount": total,
                          "LinkedTxn": [{"TxnId": f"inv-{seed}-{i}", "TxnType": "Invoice"}]}],
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
# IN-FIN2 synthetic generators (brex/deel = Mercury archetype; ramp/gusto =
# QuickBooks archetype). Each produces records/events shaped EXACTLY as the
# REAL per-source handler reads them (see handlers/{brex,ramp,gusto,deel}.py),
# so the dev console drives the genuine ingest + webhook paths.
# ---------------------------------------------------------------------

# Bearer-archetype (brex/deel) sub-resources: one shard each.
_BREX_ACCOUNTS = [
    {"account_id": "brex-cash", "account_name": "Brex Cash", "account_kind": "cash"},
    {"account_id": "brex-card", "account_name": "Brex Card", "account_kind": "card"},
]
_DEEL_CONTRACTS = [
    {"contract_id": "deel-ctr-eng", "contract_name": "Engineering Contractor",
     "contract_kind": "eor"},
    {"contract_id": "deel-ctr-design", "contract_name": "Design Contractor",
     "contract_kind": "contractor"},
]
_RAMP_ENTITIES = ("Invoice", "Bill", "BillPayment", "Payment")
# Gusto's real /v1 taxonomy (employees + payrolls) — see integrations/gusto.
_GUSTO_ENTITIES = ("employee", "payroll")

_BREX_COUNTERPARTIES = [
    ("AWS", -1850.00, "card_payment"),
    ("Stripe Payout", 86000.00, "deposit"),
    ("Figma", -180.00, "card_payment"),
    ("Payroll Run", -52000.00, "transfer"),
    ("Customer Wire — Globex", 24000.00, "deposit"),
]
_DEEL_COUNTERPARTIES = [
    ("Contractor — Ana", -4200.00, "eor_salary"),
    ("Contractor — Ben", -3100.00, "contractor_payment"),
    ("Contractor — Cy", -2750.00, "contractor_payment"),
    ("Contractor — Di", -5200.00, "eor_salary"),
    ("Contractor — Ev", -1900.00, "contractor_payment"),
]


def _brex_backfill_records(account_id: str, n: int, seed: int) -> list[dict]:
    """N historical transaction records + one balance snapshot, brex-handler-
    shaped (`_fyralis_record_type` + `_fyralis_account_id`)."""
    recs: list[dict] = []
    base = _now()
    recs.append({
        "_fyralis_record_type": "account_snapshot",
        "_fyralis_account_id": account_id,
        "as_of": _iso_z(base),
        "account": {
            "id": account_id,
            "name": next((a["account_name"] for a in _BREX_ACCOUNTS
                          if a["account_id"] == account_id), account_id),
            "type": "cash",
            "availableBalance": round(500000 - seed * 1375.5, 2),
            "currentBalance": round(512000 - seed * 1375.5, 2),
        },
    })
    for i in range(n):
        cp, amount, kind = _BREX_COUNTERPARTIES[(seed + i) % len(_BREX_COUNTERPARTIES)]
        created = _iso_z(base - timedelta(days=n - i, hours=(seed + i) % 12))
        recs.append({
            "_fyralis_record_type": "transaction",
            "_fyralis_account_id": account_id,
            "transaction": {
                "id": f"btxn-{account_id}-{seed}-{i}",
                "amount": amount,
                "counterpartyName": cp,
                "counterpartyId": f"cp-{abs(hash(cp)) % 100000}",
                "status": "posted",
                "kind": kind,
                "createdAt": created,
                "postedAt": created,
                "description": f"{cp} {kind}",
            },
        })
    return recs


def _brex_live_event(account_id: str, seq: int) -> tuple[dict, str]:
    """One live Brex `transaction.created` webhook body + the organization id.

    Shape matches handlers/brex.py: top-level `type` + `accountId` + a
    `transaction` object. `organizationId` keys tenant resolution
    (tenant_resolver._extract_brex)."""
    cp, amount, kind = _BREX_COUNTERPARTIES[seq % len(_BREX_COUNTERPARTIES)]
    status = "declined" if seq % 4 == 3 else "posted"
    txn: dict = {
        "id": f"btxn-live-{account_id}-{seq}",
        "amount": amount,
        "counterpartyName": cp,
        "status": status,
        "kind": kind,
        "createdAt": _iso_z(_now()),
    }
    if status == "declined":
        txn["reasonForFailure"] = "card limit exceeded"
    return {
        "type": "transaction.created",
        "organizationId": _brex_org_id(),
        "accountId": account_id,
        "transaction": txn,
    }, _brex_org_id()


def _brex_org_id() -> str:
    return "fin-brex-org"


def _deel_backfill_records(contract_id: str, n: int, seed: int) -> list[dict]:
    """N historical payment records + one contract snapshot, deel-handler-
    shaped (`_fyralis_record_type` + `_fyralis_contract_id`)."""
    recs: list[dict] = []
    base = _now()
    recs.append({
        "_fyralis_record_type": "contract_snapshot",
        "_fyralis_contract_id": contract_id,
        "updated": _iso_z(base),
        "contract": {
            "id": contract_id,
            "name": next((c["contract_name"] for c in _DEEL_CONTRACTS
                          if c["contract_id"] == contract_id), contract_id),
            "status": "in_progress",
            "type": "eor",
        },
    })
    for i in range(n):
        cp, amount, kind = _DEEL_COUNTERPARTIES[(seed + i) % len(_DEEL_COUNTERPARTIES)]
        created = _iso_z(base - timedelta(days=n - i, hours=(seed + i) % 12))
        recs.append({
            "_fyralis_record_type": "payment",
            "_fyralis_contract_id": contract_id,
            "payment": {
                "id": f"dpay-{contract_id}-{seed}-{i}",
                "amount": amount,
                "counterpartyName": cp,
                "status": "paid",
                "kind": kind,
                "createdAt": created,
                "postedAt": created,
                "externalMemo": f"{cp} {kind}",
            },
        })
    return recs


def _deel_live_event(contract_id: str, seq: int) -> tuple[dict, str]:
    """One live Deel `payment.created` webhook body + the organization id.

    Shape matches handlers/deel.py: top-level `type` + `contractId` + a
    `payment` object. `organizationId` keys tenant resolution
    (tenant_resolver._extract_deel)."""
    cp, amount, kind = _DEEL_COUNTERPARTIES[seq % len(_DEEL_COUNTERPARTIES)]
    status = "failed" if seq % 4 == 3 else "paid"
    payment: dict = {
        "id": f"dpay-live-{contract_id}-{seq}",
        "amount": amount,
        "counterpartyName": cp,
        "status": status,
        "kind": kind,
        "createdAt": _iso_z(_now()),
    }
    if status == "failed":
        payment["reasonForFailure"] = "insufficient funds at counterparty bank"
    return {
        "type": "payment.created",
        "organizationId": _deel_org_id(),
        "contractId": contract_id,
        "payment": payment,
    }, _deel_org_id()


def _deel_org_id() -> str:
    return "fin-deel-org"


def _ramp_backfill_records(entity: str, business_id: str, n: int,
                           seed: int) -> list[dict]:
    """Ramp backfill records — QBO-shaped entity bodies (ramp handler reuses the
    QuickBooks entity decoder); `_fyralis_business_id` keys the scope id."""
    recs = _qbo_backfill_records(entity, business_id, n, seed)
    for r in recs:
        r.pop("_fyralis_realm_id", None)
        r["_fyralis_business_id"] = business_id
    return recs


def _ramp_live_event(business_id: str, seq: int) -> dict:
    """One live Ramp eventNotifications body. Carries BOTH `business_id` (snake,
    for tenant_resolver._extract_ramp) and `businessId` (camel, read by the
    handler webhook decoder)."""
    entity = _RAMP_ENTITIES[seq % len(_RAMP_ENTITIES)]
    return {
        "business_id": business_id,
        "eventNotifications": [{
            "business_id": business_id,
            "businessId": business_id,
            "dataChangeEvent": {"entities": [{
                "name": entity,
                "id": f"live-{seq}",
                "operation": "Update",
                "lastUpdated": _iso_qbo(_now()),
            }]},
        }],
    }


def _gusto_backfill_records(entity: str, company_uuid: str, n: int,
                            seed: int) -> list[dict]:
    """Gusto backfill records — REAL-shaped employee/payroll bodies (the
    gusto:object handler decodes the real /v1 wire fields, see
    fixtures/gusto_generator.py); `_fyralis_company_uuid` keys the scope id.
    `seed` shifts the deterministic id space so re-runs mint fresh
    external_ids."""
    from services.ingest.synthetic.fixtures.gusto_generator import make_gusto

    fixture = make_gusto(
        company_uuid=f"{company_uuid}-s{seed}" if seed else company_uuid,
        entities=[entity],
        rows_per_entity=n,
    )
    return [
        {
            "_fyralis_record_type": entity,
            "_fyralis_company_uuid": company_uuid,
            "entity": row,
        }
        for row in fixture["entities"][entity]
    ]


def _gusto_live_event(company_uuid: str, seq: int) -> dict:
    """One live Gusto thin notification — REAL flat snake_case shape (VERIFIED
    against docs.gusto.com): `resource_uuid` is ALWAYS the company (keys
    tenant_resolver._extract_gusto); `entity_type`/`entity_uuid` name the
    changed resource; no entity body (the poll re-fetch fills it)."""
    entity = _GUSTO_ENTITIES[seq % len(_GUSTO_ENTITIES)]
    return {
        "uuid": f"evt-{company_uuid}-{seq}",
        "event_type": f"{entity}.updated",
        "resource_type": "Company",
        "resource_uuid": company_uuid,
        "entity_type": entity.title(),
        "entity_uuid": f"live-{seq}",
        "timestamp": int(_now().timestamp()),
    }


# ---------------------------------------------------------------------
# Inline ingest (the deterministic backfill path)
# ---------------------------------------------------------------------

async def _ingest_record(req: Request, tenant_id: UUID, channel: str, record: dict) -> dict:
    from services.ingest.ingestion.core import ingest

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
    router.add_api_route("/sources", list_sources, methods=["GET"])
    router.add_api_route("/{source}/install", install, methods=["POST"])
    router.add_api_route("/{source}/backfill", backfill, methods=["POST"])
    router.add_api_route("/{source}/live/emit", live_emit, methods=["POST"])
    router.add_api_route("/{source}/status", status, methods=["GET"])
    return router


async def list_sources() -> dict[str, Any]:
    return {
        "sources": [
            {"source": "mercury", "channel": "mercury:transaction",
             "label": "Mercury (banking / cash)"},
            {"source": "quickbooks", "channel": "quickbooks:object",
             "label": "QuickBooks (accounting / AR-AP)"},
            {"source": "brex", "channel": "brex:transaction",
             "label": "Brex (corporate cards / cash)"},
            {"source": "ramp", "channel": "ramp:transaction",
             "label": "Ramp (corporate cards / spend)"},
            {"source": "gusto", "channel": "gusto:object",
             "label": "Gusto (payroll / HR)"},
            {"source": "deel", "channel": "deel:payment",
             "label": "Deel (contractor payments)"},
        ],
    }

async def install(source: str, req: Request) -> JSONResponse:
    _require_source(source)
    tenant_id = _resolve_tenant(req)
    pool = _pool(req)
    await _ensure_tenant(pool, tenant_id)

    # Ensure observation partitions cover the historical backfill window.
    try:
        from services.domain.observations.partitions import ensure_partitions
        await ensure_partitions(pool, months_ahead=2)
        old = _now() - timedelta(days=60)
        await ensure_partitions(pool, as_of=old.date(), months_ahead=0)
    except Exception as exc:  # noqa: BLE001
        log.warning("finance_partition_ensure_failed", error=str(exc))

    # Store a webhook HMAC secret for the live path.
    secret_value = f"fin-{source}-{uuid4().hex}"
    secret_ref: str | None = None
    try:
        _deps(req)
        store = getattr(req.app.state, "secret_store", None)
        if store is not None:
            secret_ref = await store.put(
                secret_value, label=f"{source}_webhook_secret", tenant_id=tenant_id,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("finance_secret_put_failed", source=source, error=str(exc))

    if source == "mercury":
        from services.ingest.integrations.mercury.onboarding import (
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
    elif source == "quickbooks":
        realm_id = f"realm-{tenant_id.hex[:12]}"
        from services.ingest.integrations.quickbooks.onboarding import (
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
    elif source == "brex":
        from services.ingest.integrations.brex.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=tenant_id, base_url=_BREX_BASE,
            accounts=list(_BREX_ACCOUNTS),
            organization_id=_brex_org_id(),
            webhook_secret_ref=secret_ref,
        )
        await register_webhook_installation(
            pool, tenant_id=tenant_id, organization_id=_brex_org_id(),
            webhook_secret_ref=secret_ref,
        )
        sub_count = len(_BREX_ACCOUNTS)
    elif source == "ramp":
        business_id = f"biz-{tenant_id.hex[:12]}"
        from services.ingest.integrations.ramp.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=tenant_id, business_id=business_id,
            base_url=_RAMP_BASE, entities=list(_RAMP_ENTITIES),
            webhook_secret_ref=secret_ref,
        )
        await register_webhook_installation(
            pool, tenant_id=tenant_id, business_id=business_id,
            webhook_secret_ref=secret_ref,
        )
        sub_count = len(_RAMP_ENTITIES)
    elif source == "gusto":
        company_uuid = f"co-{tenant_id.hex[:12]}"
        from services.ingest.integrations.gusto.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=tenant_id, company_uuid=company_uuid,
            base_url=_GUSTO_BASE, entities=list(_GUSTO_ENTITIES),
            webhook_secret_ref=secret_ref,
        )
        await register_webhook_installation(
            pool, tenant_id=tenant_id, company_uuid=company_uuid,
            webhook_secret_ref=secret_ref,
        )
        sub_count = len(_GUSTO_ENTITIES)
    else:  # deel
        from services.ingest.integrations.deel.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=tenant_id, base_url=_DEEL_BASE,
            contracts=list(_DEEL_CONTRACTS),
            organization_id=_deel_org_id(),
            webhook_secret_ref=secret_ref,
        )
        await register_webhook_installation(
            pool, tenant_id=tenant_id, organization_id=_deel_org_id(),
            webhook_secret_ref=secret_ref,
        )
        sub_count = len(_DEEL_CONTRACTS)

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
    elif source == "quickbooks":
        realm_id = f"realm-{tenant_id.hex[:12]}"
        for entity in _QBO_ENTITIES:
            for rec in _qbo_backfill_records(entity, realm_id, per, seed):
                results.append(await _ingest_record(req, tenant_id, channel, rec))
    elif source == "brex":
        for acct in _BREX_ACCOUNTS:
            for rec in _brex_backfill_records(acct["account_id"], per, seed):
                results.append(await _ingest_record(req, tenant_id, channel, rec))
    elif source == "ramp":
        business_id = f"biz-{tenant_id.hex[:12]}"
        for entity in _RAMP_ENTITIES:
            for rec in _ramp_backfill_records(entity, business_id, per, seed):
                results.append(await _ingest_record(req, tenant_id, channel, rec))
    elif source == "gusto":
        company_uuid = f"co-{tenant_id.hex[:12]}"
        for entity in _GUSTO_ENTITIES:
            for rec in _gusto_backfill_records(entity, company_uuid, per, seed):
                results.append(await _ingest_record(req, tenant_id, channel, rec))
    else:  # deel
        for ctr in _DEEL_CONTRACTS:
            for rec in _deel_backfill_records(ctr["contract_id"], per, seed):
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

    # Each source: (payload, header name, prefix, digest-encoding). The
    # signature scheme mirrors signatures/{source}.py byte-for-byte (the
    # finance handlers' UNVERIFIED knobs default to their archetype):
    #   mercury/brex/deel = hex with `sha256=`; quickbooks/ramp/gusto = base64.
    # TODO(human): confirm brex/ramp/gusto/deel webhook signature schemes
    #   (header name, prefix, hex-vs-base64) against each provider's docs.
    digest_encoding = "hex"
    if source == "mercury":
        payload, _org = _mercury_live_event("acc-checking", seq)
        header_name = "Mercury-Signature"
        sig_prefix = "sha256="
    elif source == "quickbooks":
        realm_id = f"realm-{tenant_id.hex[:12]}"
        payload = _qbo_live_event(realm_id, seq)
        header_name = "intuit-signature"
        sig_prefix = ""  # QBO is base64, no prefix
        digest_encoding = "base64"
    elif source == "brex":
        payload, _org = _brex_live_event("brex-cash", seq)
        header_name = "Brex-Signature"
        sig_prefix = "sha256="
    elif source == "ramp":
        business_id = f"biz-{tenant_id.hex[:12]}"
        payload = _ramp_live_event(business_id, seq)
        header_name = "x-ramp-signature"
        sig_prefix = ""  # ramp default is base64, no prefix
        digest_encoding = "base64"
    elif source == "gusto":
        company_uuid = f"co-{tenant_id.hex[:12]}"
        payload = _gusto_live_event(company_uuid, seq)
        header_name = "intuit-signature"
        sig_prefix = ""  # gusto default mirrors QBO base64
        digest_encoding = "base64"
    else:  # deel
        payload, _org = _deel_live_event("deel-ctr-eng", seq)
        header_name = "Deel-Signature"
        sig_prefix = "sha256="

    raw = json.dumps(payload).encode("utf-8")
    cache = getattr(req.app.state, "_finance_secrets", {}) or {}
    secret = cache.get((str(tenant_id), source))

    delivered_via = None
    webhook_status = None
    webhook_body: Any = None
    if secret:
        mac = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256)
        if digest_encoding == "hex":
            signature = sig_prefix + mac.hexdigest()
        else:
            import base64
            signature = sig_prefix + base64.b64encode(mac.digest()).decode("ascii")
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
        "payload_kind": payload.get("type") or (
            "eventNotifications" if "eventNotifications" in payload
            else "event"),
    }, status_code=201)

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
    elif source == "quickbooks":
        install = await pool.fetchrow(
            "SELECT id, base_url, realm_id, created_at FROM quickbooks_installations "
            "WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1", tenant_id,
        )
        subs = await pool.fetch(
            "SELECT entity_type, state FROM quickbooks_entities qe "
            "JOIN quickbooks_installations qi ON qe.quickbooks_installation_id = qi.id "
            "WHERE qi.tenant_id = $1", tenant_id,
        ) if install else []
    elif source == "brex":
        install = await pool.fetchrow(
            "SELECT id, base_url, organization_id, created_at FROM brex_installations "
            "WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1", tenant_id,
        )
        subs = await pool.fetch(
            "SELECT account_id, account_name, state FROM brex_accounts ba "
            "JOIN brex_installations bi ON ba.brex_installation_id = bi.id "
            "WHERE bi.tenant_id = $1", tenant_id,
        ) if install else []
    elif source == "ramp":
        install = await pool.fetchrow(
            "SELECT id, base_url, business_id, created_at FROM ramp_installations "
            "WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1", tenant_id,
        )
        subs = await pool.fetch(
            "SELECT entity_type, state FROM ramp_entities re "
            "JOIN ramp_installations ri ON re.ramp_installation_id = ri.id "
            "WHERE ri.tenant_id = $1", tenant_id,
        ) if install else []
    elif source == "gusto":
        install = await pool.fetchrow(
            "SELECT id, base_url, company_uuid, created_at FROM gusto_installations "
            "WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1", tenant_id,
        )
        subs = await pool.fetch(
            "SELECT entity_type, state FROM gusto_entities ge "
            "JOIN gusto_installations gi ON ge.gusto_installation_id = gi.id "
            "WHERE gi.tenant_id = $1", tenant_id,
        ) if install else []
    else:  # deel
        install = await pool.fetchrow(
            "SELECT id, base_url, organization_id, created_at FROM deel_installations "
            "WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1", tenant_id,
        )
        subs = await pool.fetch(
            "SELECT contract_id, contract_name, state FROM deel_contracts dc "
            "JOIN deel_installations di ON dc.deel_installation_id = di.id "
            "WHERE di.tenant_id = $1", tenant_id,
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


__all__ = ["build_finance_router"]
