# Finance Sources — API → Information Map

> **What this answers:** for Mercury and QuickBooks, *which API call returns
> which piece of information*, and how each field becomes an observation in the
> pipeline. This is the field-level companion to
> [`finance-sources.md`](finance-sources.md) (which covers the architecture and
> per-source contract).

Source of truth for this map:
- Mercury: [`services/ingest/integrations/mercury/client.py`](../../services/ingest/integrations/mercury/client.py) + [`services/ingest/ingestion/handlers/mercury.py`](../../services/ingest/ingestion/handlers/mercury.py)
- QuickBooks: [`services/ingest/integrations/quickbooks/client.py`](../../services/ingest/integrations/quickbooks/client.py) + [`services/ingest/ingestion/handlers/quickbooks.py`](../../services/ingest/ingestion/handlers/quickbooks.py)

Every field below is read by the real client/handler — nothing here is aspirational.

---

## 1. Mercury (banking / cash)

- **Base URL:** `https://api.mercury.com/api/v1` (sandbox: `https://api-sandbox.mercury.com/api/v1`)
- **Auth:** API token as **Bearer** (`Authorization: Bearer <token>`); also accepted as Basic-auth username with empty password.
- **Pagination:** `limit` + `offset`; list responses are `{total, accounts|transactions: [...]}`.
- **Rate limits:** HTTP 429 + `Retry-After` (bounded retry in `_request`).

### 1.1 `GET /accounts` — *what cash accounts exist + their balances*

Called at **install/seed** (populates `mercury_accounts`) and by the **fetcher** each run (emits a balance snapshot per account).

| API field | Information | Where it lands |
|---|---|---|
| `id` | Account identifier | `mercury_accounts.account_id`; part of every `external_id` |
| `name` | Account name (e.g. "Operating Checking") | `account_snapshot` content, `account_name` |
| `type` | Account kind (`checking` / `savings`) | `mercury_accounts.account_kind` |
| `availableBalance` | **Spendable cash right now** | balance-snapshot observation (cash position) |
| `currentBalance` | Ledger balance incl. pending | balance-snapshot observation |

→ **Observation:** `kind=signal`, `external_id = mercury:{account_id}:balance:{as_of_date}`
(one snapshot row per account per day — this is the **cash position** signal).

### 1.2 `GET /account/{id}` — *one account (balance probe)*

Same fields as a single `/accounts` entry. Used as a connectivity/balance probe (reconciler). Not a separate observation type.

### 1.3 `GET /account/{id}/transactions` — *the actual money movements*

The core backfill + incremental-poll surface. `start` (ISO date) bounds the window for incremental polls; `Metadata`-less so the **cursor is the transaction `createdAt` high-water**.

| API field | Information | Where it lands |
|---|---|---|
| `id` | Transaction id | part of `external_id` |
| `amount` | **Signed amount** (negative = outflow, positive = inflow) | `content_text` money + direction; flow signal |
| `counterpartyName` | Who the money went to / came from | `content_text` ("…to Acme Corp") |
| `status` | `pending` / `sent` / `posted` / **`failed`** / **`cancelled`** | **drives `kind`** (see below) + versions the `external_id` |
| `kind` | Mercury txn kind (`externalTransfer`, `incomingPayment`, …) | `content_text` classifier |
| `createdAt` / `postedAt` | Timestamps | `occurred_at`; `createdAt` is the **incremental cursor / high-water** |
| `bankDescription` | Free-text bank memo | `content_text` detail |

→ **Observation:**
- `external_id = mercury:{account_id}:txn:{txn_id}:{status}` (**versioned by status** — a `sent→failed` transition lands as a *new* row, not a dedup)
- `status ∈ {pending, sent, posted}` → **`kind=signal`** (normal money movement)
- `status ∈ {failed, cancelled}` → **`kind=state_change`** (the cash-risk signal — a payment that didn't go through)

### 1.4 Live webhook — `POST /webhooks/mercury/events`

- **Signature:** `Mercury-Signature: sha256=<hex>` (HMAC-SHA256, constant-time verify).
- **Payload:** `transaction.created` event containing the same `transaction` object as 1.3 + `organizationId` (→ tenant resolution) + `accountId`.
- Flows through the **same handler** as backfill → identical `external_id`, so a webhook event and a later poll of the same transaction **dedup** (or version, if the status changed). This is the backfill/live parity guarantee.

### Mercury summary — *what business questions it answers*
- **How much cash do we have?** → `availableBalance` / `currentBalance` (1.1)
- **What's our burn / inflow-outflow?** → signed `amount` over transactions (1.3)
- **Did a payment fail?** → `status ∈ {failed,cancelled}` → `state_change` (1.3)

---

## 2. QuickBooks Online (accounting / AR-AP)

- **Base URL:** `https://quickbooks.api.intuit.com` (sandbox: `https://sandbox-quickbooks.api.intuit.com`)
- **Auth:** OAuth 2.0 **Bearer access token** (~60 min), every call scoped to a company **`realmId`**. Refresh-token rotation owned by `oauth_poller`.
- **Reads go through ONE endpoint:** the **query endpoint** (SQL-like), not per-resource REST.
- **Rate limits:** 10 req/s, 120/min batch per realm; 429 + `Retry-After`.

### 2.1 `GET /v3/company/{realmId}/query?query=<SQL>` — *everything*

All reads are a SELECT against one entity:

```
SELECT * FROM <Entity>
  [WHERE Metadata.LastUpdatedTime > '<ts>']
  ORDERBY Metadata.LastUpdatedTime
  STARTPOSITION <n> MAXRESULTS <m>
```

- **`Metadata.LastUpdatedTime`** is the **incremental cursor** (only fetch rows changed since the last poll).
- **`STARTPOSITION` / `MAXRESULTS`** = offset pagination.
- Response: `{"QueryResponse": {"<Entity>": [...], "startPosition", "maxResults"}}`.
- Entities ingested (one shard each): **`Invoice`, `Bill`, `BillPayment`, `Payment`** (`DEFAULT_ENTITIES`).

**Per-entity field map** (the fields the handler reads):

| Entity | API field | Information | Drives |
|---|---|---|---|
| **Invoice** (money owed *to* us — AR) | `Id`, `SyncToken` | id + mutation version | `external_id` |
| | `DocNumber` | Invoice number (e.g. INV-1042) | `content_text` |
| | `TotalAmt` | Invoice total | `content_text` (revenue) |
| | `Balance` | **Outstanding amount** | **paid detection** (Balance==0) |
| | `DueDate` | When payment is due | **overdue detection** (DueDate<now & Balance>0) |
| | `CustomerRef.name` | Who owes us | `content_text` |
| | `TxnDate` | Invoice date | `occurred_at` context |
| | `MetaData.LastUpdatedTime` | Last change | **cursor** |
| **Bill** (money *we* owe — AP) | `TotalAmt` / `Balance` | Amount owed / outstanding | AP obligation; paid detection |
| | `VendorRef.name` | Who we owe | `content_text` |
| | `DueDate` | When we must pay | overdue/upcoming obligation |
| **Payment** (cash received from a customer) | `TotalAmt` | Payment amount | revenue collected |
| | `CustomerRef.name` | Who paid | `content_text` |
| **BillPayment** (cash we paid a vendor) | `TotalAmt` | Amount paid out | AP settled |
| | `VendorRef.name` | Who we paid | `content_text` |

→ **Observation:**
- `external_id = qbo:{realm}:{entity}:{id}:{SyncToken}` (**versioned by `SyncToken`** — QBO increments it on every mutation, so each edit lands as a new row)
- created / updated / still-open → **`kind=signal`**
- **`Balance == 0` (paid)** or **`DueDate < now` with `Balance > 0` (overdue)** → **`kind=state_change`** (AR/AP health change)

### 2.2 `GET /v3/company/{realmId}/companyinfo/{realmId}` — *connectivity probe*

Returns company metadata. Used only to verify auth/realm reachability; not an observation.

### 2.3 Live webhook — `POST /webhooks/quickbooks/events`

- **Signature:** `intuit-signature` = **base64** HMAC-SHA256 (no `sha256=` prefix — the one verifier difference vs Mercury).
- **Payload:** Intuit `eventNotifications` — a **thin change event**: `realmId` + `dataChangeEvent.entities[]` with just `{name, id, operation, lastUpdated}` (no entity body).
- Handler emits a **thin-change observation** (`kind=signal`, `external_id = qbo:{realm}:{entity}:{id}:chg:{ver}`); the next **poll re-fetch (2.1)** fills the full body. This is why QBO live + poll work together: the webhook says *"Invoice 42 changed,"* the poll fetches *what* changed.

### QuickBooks summary — *what business questions it answers*
- **What revenue have we booked / collected?** → `Invoice.TotalAmt`, `Payment.TotalAmt` (2.1)
- **Who owes us and is it overdue?** → `Invoice.Balance` + `DueDate` → `state_change` (AR aging)
- **What do we owe and when?** → `Bill.Balance` + `DueDate` (AP / upcoming obligations)
- **Did an invoice/bill get paid?** → `Balance==0` → `state_change` (2.1)

---

## 3. Cross-source picture (why both together)

| Question | Mercury supplies | QuickBooks supplies |
|---|---|---|
| Cash on hand | `availableBalance` | — |
| Burn / runway | signed transaction flow | (booked obligations refine it) |
| Revenue | inflow transactions (cash basis) | `Invoice`/`Payment` (accrual basis) |
| Receivables (who owes us) | — | `Invoice.Balance` + `DueDate` |
| Payables (who we owe) | — | `Bill.Balance` + `DueDate` |
| Failed/at-risk payment | txn `status=failed` | — |

> The **Finance Intelligence** enrichment layer (cross-source `runway_days`,
> burn, AR/AP aging on `content.intelligence`) is **specified but deferred** —
> see `finance-sources.md` §"Finance Intelligence". This document covers the
> **raw signal** layer that is built and working.

---

## 3b. Extended fields now ingested (beyond the core money signal)

The fetcher always passed the **full** transaction / entity object through; these
richer fields were previously dropped when the handler built `content`. They now
land in `content` (JSONB — additive; `external_id` / `kind` unchanged, so dedup
parity is preserved). Only present keys are written (no None-bloat).

**Mercury transaction** → `content`:
`reason_for_failure`, `failed_at` (why a payment died — pairs with `state_change`),
`estimated_delivery_date` (forward cash-flow), `mercury_category` +
`general_ledger_code_name` (spend classification / bookkeeping),
`counterparty_id`, `counterparty_nickname`, `external_memo`, `fee_id`,
`dashboard_link`, `currency_exchange_info` (FX exposure), and `details` (ACH /
wire / card rail routing + counterparty bank). The failure reason is also
appended to `content_text`.
**Mercury account snapshot** → `account_status`, `legal_business_name`,
`account_created_at`.
> **PII hygiene:** `details.*` is deep-copied with `accountNumber`,
> `routingNumber`, and `iban` **masked to last-4** (`••6789`) before it lands —
> full bank identifiers never reach the reasoning layer / LLM context.

**QuickBooks entity** → `content`:
`line_items[]` (per line: amount, description, detail_type, item, quantity,
unit_price, account, class, billable_customer — product-level revenue/expense
breakdown), `linked_txns[]` (the AR/AP graph: Invoice↔Payment, PO→Bill→Payment),
`tax` (total_tax + line count), `home_total_amount` / `home_balance` /
`exchange_rate` (multi-currency), `unapplied_amount` / `deposit_to_account` /
`ar_account` / `ap_account` (cash nuance), `payment_method` / `payment_ref_num` /
`pay_type` (payment channel), and `class` / `department` / `project` (P&L
segmentation). Line-item count is appended to `content_text`.

These are exercised by the finance UI console's synthetic backfill and asserted
by the handler unit tests. The deferred **Finance Intelligence** layer is the
intended consumer (runway from `estimated_delivery_date`, spend analytics from
`line_items` + `mercury_category`, AR/AP reconciliation from `linked_txns`,
multi-currency normalization from `exchange_rate`).

## 4. Field → pipeline quick reference

| Concept | Mercury | QuickBooks |
|---|---|---|
| **Auth** | Bearer API token | OAuth2 Bearer + `realmId` |
| **List/enumerate** | `GET /accounts` | `query SELECT * FROM <Entity>` |
| **Detail/backfill** | `GET /account/{id}/transactions` | same query endpoint, paged |
| **Incremental cursor** | transaction `createdAt` high-water (`start=`) | `Metadata.LastUpdatedTime` (`WHERE >`) |
| **Pagination** | `limit` + `offset` | `STARTPOSITION` + `MAXRESULTS` |
| **Version key (dedup)** | transaction `status` | `SyncToken` |
| **`signal` vs `state_change`** | status pending/sent/posted vs failed/cancelled | open vs paid(Balance==0)/overdue |
| **Live webhook** | `transaction.created`, full body | `eventNotifications`, thin change → poll fills body |
| **Webhook signature** | `Mercury-Signature: sha256=<hex>` | `intuit-signature: <base64>` |
