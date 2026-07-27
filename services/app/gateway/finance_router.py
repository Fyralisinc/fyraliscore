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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from services.ingest.source_contract import (
    SOURCE_DEFINITIONS,
    resolve_installation_status_loader,
    resolve_callable_reference,
    source_definition,
)


log = structlog.get_logger("gateway.finance")


_FINANCE_SOURCE_DEFINITIONS = tuple(
    source
    for source in SOURCE_DEFINITIONS
    if "finance_testing" in source.capability_flags
)
_SOURCES = tuple(source.source_id for source in _FINANCE_SOURCE_DEFINITIONS)
_CHANNEL = {
    source.source_id: source.channel_for_ingress("webhook")
    for source in _FINANCE_SOURCE_DEFINITIONS
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

def _request_is_production(req: Request) -> bool:
    settings = getattr(req.app.state, "gateway_settings", None)
    return bool(getattr(settings, "is_production", False))


def _resolve_tenant(req: Request) -> UUID:
    hdr = req.headers.get("X-Tenant-Id")
    if hdr:
        try:
            return UUID(hdr)
        except Exception:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="invalid X-Tenant-Id")
    if _request_is_production(req):
        raise HTTPException(status_code=400, detail="tenant_id missing")
    env_tid = os.environ.get("DEFAULT_TENANT_ID") or os.environ.get(
        "COMPANY_OS_TENANT_ID"
    )
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


@dataclass(frozen=True, slots=True)
class FinanceInstallResult:
    installation_id: UUID
    sub_resource_count: int


@dataclass(frozen=True, slots=True)
class FinanceLiveEvent:
    payload: dict[str, Any]
    header_name: str
    signature_prefix: str
    digest_encoding: str = "hex"


@dataclass(frozen=True, slots=True)
class FinanceTestingOperations:
    install: Callable[
        [asyncpg.Pool, UUID, str | None],
        Awaitable[FinanceInstallResult],
    ]
    backfill_records: Callable[[UUID, int, int], list[dict[str, Any]]]
    live_event: Callable[[UUID, int], FinanceLiveEvent]


async def _install_mercury(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    secret_ref: str | None,
) -> FinanceInstallResult:
    from services.ingest.integrations.mercury.onboarding import (
        finalize_install,
        register_webhook_installation,
    )

    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=_MERCURY_BASE,
        accounts=list(_MERCURY_ACCOUNTS),
        organization_id=_org_id_for(""),
        webhook_secret_ref=secret_ref,
    )
    await register_webhook_installation(
        pool,
        tenant_id=tenant_id,
        organization_id=_org_id_for(""),
        webhook_secret_ref=secret_ref,
    )
    return FinanceInstallResult(install_id, len(_MERCURY_ACCOUNTS))


async def _install_quickbooks(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    secret_ref: str | None,
) -> FinanceInstallResult:
    from services.ingest.integrations.quickbooks.onboarding import (
        finalize_install,
        register_webhook_installation,
    )

    realm_id = f"realm-{tenant_id.hex[:12]}"
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        realm_id=realm_id,
        base_url=_QBO_BASE,
        entities=list(_QBO_ENTITIES),
        webhook_secret_ref=secret_ref,
    )
    await register_webhook_installation(
        pool,
        tenant_id=tenant_id,
        realm_id=realm_id,
        webhook_secret_ref=secret_ref,
    )
    return FinanceInstallResult(install_id, len(_QBO_ENTITIES))


async def _install_brex(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    secret_ref: str | None,
) -> FinanceInstallResult:
    from services.ingest.integrations.brex.onboarding import (
        finalize_install,
        register_webhook_installation,
    )

    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=_BREX_BASE,
        accounts=list(_BREX_ACCOUNTS),
        organization_id=_brex_org_id(),
        webhook_secret_ref=secret_ref,
    )
    await register_webhook_installation(
        pool,
        tenant_id=tenant_id,
        organization_id=_brex_org_id(),
        webhook_secret_ref=secret_ref,
    )
    return FinanceInstallResult(install_id, len(_BREX_ACCOUNTS))


async def _install_ramp(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    secret_ref: str | None,
) -> FinanceInstallResult:
    from services.ingest.integrations.ramp.onboarding import (
        finalize_install,
        register_webhook_installation,
    )

    business_id = f"biz-{tenant_id.hex[:12]}"
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        business_id=business_id,
        base_url=_RAMP_BASE,
        entities=list(_RAMP_ENTITIES),
        webhook_secret_ref=secret_ref,
    )
    await register_webhook_installation(
        pool,
        tenant_id=tenant_id,
        business_id=business_id,
        webhook_secret_ref=secret_ref,
    )
    return FinanceInstallResult(install_id, len(_RAMP_ENTITIES))


async def _install_gusto(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    secret_ref: str | None,
) -> FinanceInstallResult:
    from services.ingest.integrations.gusto.onboarding import (
        finalize_install,
        register_webhook_installation,
    )

    company_uuid = f"co-{tenant_id.hex[:12]}"
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        company_uuid=company_uuid,
        base_url=_GUSTO_BASE,
        entities=list(_GUSTO_ENTITIES),
        webhook_secret_ref=secret_ref,
    )
    await register_webhook_installation(
        pool,
        tenant_id=tenant_id,
        company_uuid=company_uuid,
        webhook_secret_ref=secret_ref,
    )
    return FinanceInstallResult(install_id, len(_GUSTO_ENTITIES))


async def _install_deel(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    secret_ref: str | None,
) -> FinanceInstallResult:
    from services.ingest.integrations.deel.onboarding import (
        finalize_install,
        register_webhook_installation,
    )

    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=_DEEL_BASE,
        contracts=list(_DEEL_CONTRACTS),
        organization_id=_deel_org_id(),
        webhook_secret_ref=secret_ref,
    )
    await register_webhook_installation(
        pool,
        tenant_id=tenant_id,
        organization_id=_deel_org_id(),
        webhook_secret_ref=secret_ref,
    )
    return FinanceInstallResult(install_id, len(_DEEL_CONTRACTS))


def _backfill_mercury(
    _tenant_id: UUID,
    count: int,
    seed: int,
) -> list[dict[str, Any]]:
    return [
        record
        for account in _MERCURY_ACCOUNTS
        for record in _mercury_backfill_records(
            account["account_id"],
            count,
            seed,
        )
    ]


def _backfill_quickbooks(
    tenant_id: UUID,
    count: int,
    seed: int,
) -> list[dict[str, Any]]:
    realm_id = f"realm-{tenant_id.hex[:12]}"
    return [
        record
        for entity in _QBO_ENTITIES
        for record in _qbo_backfill_records(entity, realm_id, count, seed)
    ]


def _backfill_brex(
    _tenant_id: UUID,
    count: int,
    seed: int,
) -> list[dict[str, Any]]:
    return [
        record
        for account in _BREX_ACCOUNTS
        for record in _brex_backfill_records(
            account["account_id"],
            count,
            seed,
        )
    ]


def _backfill_ramp(
    tenant_id: UUID,
    count: int,
    seed: int,
) -> list[dict[str, Any]]:
    business_id = f"biz-{tenant_id.hex[:12]}"
    return [
        record
        for entity in _RAMP_ENTITIES
        for record in _ramp_backfill_records(
            entity,
            business_id,
            count,
            seed,
        )
    ]


def _backfill_gusto(
    tenant_id: UUID,
    count: int,
    seed: int,
) -> list[dict[str, Any]]:
    company_uuid = f"co-{tenant_id.hex[:12]}"
    return [
        record
        for entity in _GUSTO_ENTITIES
        for record in _gusto_backfill_records(
            entity,
            company_uuid,
            count,
            seed,
        )
    ]


def _backfill_deel(
    _tenant_id: UUID,
    count: int,
    seed: int,
) -> list[dict[str, Any]]:
    return [
        record
        for contract in _DEEL_CONTRACTS
        for record in _deel_backfill_records(
            contract["contract_id"],
            count,
            seed,
        )
    ]


def _live_mercury(_tenant_id: UUID, sequence: int) -> FinanceLiveEvent:
    payload, _organization_id = _mercury_live_event("acc-checking", sequence)
    return FinanceLiveEvent(payload, "Mercury-Signature", "sha256=")


def _live_quickbooks(tenant_id: UUID, sequence: int) -> FinanceLiveEvent:
    payload = _qbo_live_event(f"realm-{tenant_id.hex[:12]}", sequence)
    return FinanceLiveEvent(payload, "intuit-signature", "", "base64")


def _live_brex(_tenant_id: UUID, sequence: int) -> FinanceLiveEvent:
    payload, _organization_id = _brex_live_event("brex-cash", sequence)
    return FinanceLiveEvent(payload, "Brex-Signature", "sha256=")


def _live_ramp(tenant_id: UUID, sequence: int) -> FinanceLiveEvent:
    payload = _ramp_live_event(f"biz-{tenant_id.hex[:12]}", sequence)
    return FinanceLiveEvent(payload, "x-ramp-signature", "", "base64")


def _live_gusto(tenant_id: UUID, sequence: int) -> FinanceLiveEvent:
    payload = _gusto_live_event(f"co-{tenant_id.hex[:12]}", sequence)
    return FinanceLiveEvent(payload, "intuit-signature", "", "base64")


def _live_deel(_tenant_id: UUID, sequence: int) -> FinanceLiveEvent:
    payload, _organization_id = _deel_live_event("deel-ctr-eng", sequence)
    return FinanceLiveEvent(payload, "Deel-Signature", "sha256=")


def build_mercury_finance_testing() -> FinanceTestingOperations:
    return FinanceTestingOperations(
        install=_install_mercury,
        backfill_records=_backfill_mercury,
        live_event=_live_mercury,
    )


def build_quickbooks_finance_testing() -> FinanceTestingOperations:
    return FinanceTestingOperations(
        install=_install_quickbooks,
        backfill_records=_backfill_quickbooks,
        live_event=_live_quickbooks,
    )


def build_brex_finance_testing() -> FinanceTestingOperations:
    return FinanceTestingOperations(
        install=_install_brex,
        backfill_records=_backfill_brex,
        live_event=_live_brex,
    )


def build_ramp_finance_testing() -> FinanceTestingOperations:
    return FinanceTestingOperations(
        install=_install_ramp,
        backfill_records=_backfill_ramp,
        live_event=_live_ramp,
    )


def build_gusto_finance_testing() -> FinanceTestingOperations:
    return FinanceTestingOperations(
        install=_install_gusto,
        backfill_records=_backfill_gusto,
        live_event=_live_gusto,
    )


def build_deel_finance_testing() -> FinanceTestingOperations:
    return FinanceTestingOperations(
        install=_install_deel,
        backfill_records=_backfill_deel,
        live_event=_live_deel,
    )


@lru_cache(maxsize=None)
def _finance_operations(source: str) -> FinanceTestingOperations:
    definition = source_definition(source)
    binding = definition.finance_testing_binding
    if binding is None:
        raise RuntimeError(
            f"source {definition.source_id!r} has no finance testing binding"
        )
    builder = resolve_callable_reference(binding)
    operations = builder()
    if not isinstance(operations, FinanceTestingOperations):
        raise TypeError(
            f"source {definition.source_id!r} finance testing binding returned "
            f"{type(operations).__name__}"
        )
    return operations


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
                {
                    "source": definition.source_id,
                    "channel": definition.channel_for_ingress("webhook"),
                    "label": definition.display_name,
                }
            for definition in _FINANCE_SOURCE_DEFINITIONS
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

    install_result = await _finance_operations(source).install(
        pool,
        tenant_id,
        secret_ref,
    )
    install_id = install_result.installation_id
    sub_count = install_result.sub_resource_count

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

    records = _finance_operations(source).backfill_records(tenant_id, per, seed)
    results = [
        await _ingest_record(req, tenant_id, channel, record)
        for record in records
    ]

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
    live_event = _finance_operations(source).live_event(tenant_id, seq)
    payload = live_event.payload
    header_name = live_event.header_name
    sig_prefix = live_event.signature_prefix
    digest_encoding = live_event.digest_encoding

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
    installations = await _finance_installation_status(
        pool,
        tenant_id=tenant_id,
        source=source,
    )
    singular = installations[0] if len(installations) == 1 else None

    return {
        "source": source,
        "channel": channel,
        "installed": bool(installations),
        "install": singular["install"] if singular is not None else None,
        "sub_resources": (
            singular["sub_resources"] if singular is not None else []
        ),
        "installations": installations,
        "installation_count": len(installations),
        "installation_selection_required": len(installations) > 1,
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


async def _finance_installation_status(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
) -> list[dict[str, Any]]:
    definition = source_definition(source)
    adapter = definition.installation_adapter
    management = adapter.management if adapter is not None else None
    if (
        management is None
        or management.entity_table is None
        or management.entity_install_column is None
        or not management.entity_status_columns
    ):
        raise RuntimeError(
            f"finance source {source!r} lacks installation/entity status metadata"
        )

    loader = resolve_installation_status_loader(source)
    rows = await loader(
        pool,
        tenant_id=tenant_id,
        source=source,
        include_disabled=False,
    )
    output: list[dict[str, Any]] = []
    entity_columns = ", ".join(management.entity_status_columns)
    for row in rows:
        sub_resources = await pool.fetch(
            f"""
            SELECT {entity_columns}
              FROM {management.entity_table}
             WHERE tenant_id = $1
               AND {management.entity_install_column} = $2
             ORDER BY {management.entity_status_columns[0]}
            """,
            tenant_id,
            row["id"],
        )
        details = {
            key: (
                value.isoformat()
                if isinstance(value, datetime)
                else str(value)
                if isinstance(value, UUID)
                else value
            )
            for key, value in row["details"].items()
        }
        installation_id = str(row["id"])
        output.append(
            {
                "installation_id": installation_id,
                "enabled": bool(row["enabled"]),
                "has_secret": bool(row["has_secret"]),
                "installed_at": row["installed_at"].isoformat(),
                "details": details,
                "install": {
                    "id": installation_id,
                    "created_at": row["installed_at"].isoformat(),
                    **details,
                },
                "sub_resources": [dict(resource) for resource in sub_resources],
            }
        )
    return output


__all__ = ["build_finance_router"]
