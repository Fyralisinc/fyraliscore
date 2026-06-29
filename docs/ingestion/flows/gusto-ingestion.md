# Gusto Ingestion — How Fyralis Pulls Gusto Data

This document explains, in detail, **how Gusto data enters Fyralis**: which Gusto
APIs are called, with which token, and how Gusto's finance signal set —
**invoices, bills, bill payments, and payments** — is each ingested.

It deliberately stops at the point where a Gusto change becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **A note on what "Gusto" actually is in this code.** Gusto ships as the
> **OAuth / QuickBooks archetype** (the IN-FIN2 finance work-unit). The module
> docstrings and the migration comments describe an *aspirational* payroll read
> surface (`payrolls` / `employees` / `contractor_payments` under
> `/v1/companies/{company_uuid}/...`), but **the code that actually runs** clones
> the QuickBooks accounting model verbatim: it issues a SQL-like `query` against
> the entities `Invoice`, `Bill`, `BillPayment`, `Payment` over
> `/v3/company/{company_uuid}/query`. Throughout this doc the **verified code
> behaviour is authoritative**; the stale payroll-flavoured comments are flagged
> as such. Several seams (real OAuth bounce, token refresh, the exact webhook
> signature scheme, the production API host) are *documented-but-unbuilt* and
> carry explicit `TODO(human)` callouts in the source — they are surfaced below.

---

## 1. The ways data arrives

Gusto data reaches Fyralis through **three paths that converge on one handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical PULL)** | Onboarding (`onboarding_triggers` source=`gusto`) | Fyralis *pulls* full history via the Gusto **query API** (`/v3/company/{company}/query`) | [planners/gusto.py](../../../services/ingest/ingestion/planners/gusto.py), [fetchers/gusto.py](../../../services/ingest/ingestion/fetchers/gusto.py) |
| **Poll (incremental PULL)** | Reconciler reshare | The **same** fetcher re-runs warm-started with the `updated_at` high-water cursor, so only changed entities come back | [reconcilers/gusto.py](../../../services/ingest/ingestion/reconcilers/gusto.py), [fetchers/gusto.py:133‑145](../../../services/ingest/ingestion/fetchers/gusto.py#L133-L145) |
| **Live (real-time PUSH)** | A change in Gusto | Gusto *pushes* an **HMAC-signed webhook** (`eventNotifications`) to Fyralis's webhook edge | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/gusto.py](../../../services/app/webhooks/signatures/gusto.py), [handlers/gusto.py](../../../services/ingest/ingestion/handlers/gusto.py) |

Crucially, **all three paths converge on one channel and one handler** —
`gusto:object`, served by `handle_gusto_object`
([handlers/gusto.py:406‑475](../../../services/ingest/ingestion/handlers/gusto.py#L406-L475)).
The channel-mapping table routes **every** Gusto ingress kind to that single
channel ([normalizer/channel_mapping.py:195‑197](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L195-L197)):

```python
("gusto", "backfill"): "gusto:object",
("gusto", "poll"):     "gusto:object",
("gusto", "webhook"):  "gusto:object",
```

All paths derive the **same** dedup key, **versioned by `SyncToken`** so a
mutating entity (draft → sent → paid → overdue) re-observes on each state change
([idempotency:215‑220](../../../services/ingest/ingestion/idempotency/__init__.py#L215-L220)):

```
external_id = "gusto:{company_uuid}:{entity_kind}:{entity_id}:{SyncToken}"
```

So an entity that is both backfilled *and* delivered live (with the same
`SyncToken`) collapses into **one** observation. This is the central design
invariant of Gusto ingestion. The one exception is the **thin live webhook**
(§7.2): a webhook carries only `id`+`operation`, not the full body or
`SyncToken`, so it is keyed on a distinct `…:chg:{version}` namespace
([idempotency:223‑229](../../../services/ingest/ingestion/idempotency/__init__.py#L223-L229)) and
the authoritative full-body observation is filled in by the next poll re-fetch.

> Gusto uses the QuickBooks-style **query API** for history and incremental
> reads; real-time is **HTTP webhooks**. There is no GraphQL surface.

---

## 2. Authentication & token model — OAuth 2.0 (QuickBooks archetype)

Gusto authenticates with an **OAuth 2.0 Bearer access token**, and every call is
scoped to a company `company_uuid` (the QuickBooks `realm_id` equivalent). The
access token is resolved **once** from the secret store (or preset in spammer
mode) and reused for the life of the client
([client.py:101‑123](../../../services/ingest/integrations/gusto/client.py#L101-L123),
[client.py:125‑135](../../../services/ingest/integrations/gusto/client.py#L125-L135)).

### 2.1 No OAuth bounce, no token refresh — the operator-mediated install

Unlike Slack/GitHub, this repo deliberately does **not** implement the OAuth
bounce (authorize → callback → code exchange). The genuine production install
surface is **operator-mediated credential submission**: an operator pastes the
`company_uuid` + the `access_token` (and optional `refresh_token`) they obtained
from their own Gusto OAuth app, and the router verifies them against the **real**
Gusto API before seeding the install
([oauth.py:1‑44](../../../services/ingest/integrations/gusto/oauth.py#L1-L44)).

The two endpoints (Bearer-authed; tenant from the session, never a client param):

| Route | Purpose | Code |
|-------|---------|------|
| `POST /integrations/gusto/connect/preflight` | verify the token+company via `company_info()`; on failure a structured `400`, **no secret stored** | [oauth.py:136‑161](../../../services/ingest/integrations/gusto/oauth.py#L136-L161) |
| `POST /integrations/gusto/connect/finalize` | re-verify, then persist tokens encrypted + `finalize_install()` + (if a verifier token was supplied) `register_webhook_installation()` | [oauth.py:164‑250](../../../services/ingest/integrations/gusto/oauth.py#L164-L250) |

Credentials are verified **before any write**, so an invalid token leaves no
`encrypted_secrets` / install rows behind
([oauth.py:187‑196](../../../services/ingest/integrations/gusto/oauth.py#L187-L196)).

> **No token refresh exists.** The install row persists `refresh_secret_ref` +
> `token_expires_at`, but **nothing rotates the short-lived access token**. This
> is a `TODO(human)` seam in three places — the client, the oauth module, and the
> migration — exactly as the QuickBooks archetype ships
> ([client.py:8‑12](../../../services/ingest/integrations/gusto/client.py#L8-L12),
> [oauth.py:12‑17](../../../services/ingest/integrations/gusto/oauth.py#L12-L17)).
> When the token expires, reads fail with `gusto_api_unauthorized` (§3) and stay
> failed until the operator re-finalizes with a fresh token. There is **no
> revocation chokepoint** that auto-disables the install (contrast GitHub) — see
> §9.

### 2.2 Where credentials live

| Credential | Where (secret-store label) | Notes |
|-----------|---------------------------|-------|
| Access token | `gusto_access_token:{company_uuid}` → `gusto_installations.secret_ref` | the only secret the read client uses |
| Refresh token | `gusto_refresh_token:{company_uuid}` → `gusto_installations.refresh_secret_ref` | persisted but **unused** (no refresh loop) |
| Webhook verifier token | `gusto_webhook_verifier:{company_uuid}` → `provider_installations.secret_ref` | the HMAC secret the webhook edge loads |

Only **opaque refs** reach the DB; the tokens themselves are encrypted at rest
via the gateway `secret_store`
([oauth.py:198‑213](../../../services/ingest/integrations/gusto/oauth.py#L198-L213)). The access token /
`Authorization` header are **never logged**
([client.py:25](../../../services/ingest/integrations/gusto/client.py#L25)).

The persistence shapes mirror QuickBooks: a `gusto_installations` row per
`(tenant, company_uuid)` and one `gusto_entities` row per entity type to shard
([onboarding.py:37‑115](../../../services/ingest/integrations/gusto/onboarding.py#L37-L115),
schema [0097_gusto.sql:47‑97](../../../db/migrations/0097_gusto.sql#L47-L97)).
`finalize_install` also emits an `onboarding_triggers` row (`source='gusto'`) so
the existing M6 backfill chain fires
([onboarding.py:96‑108](../../../services/ingest/integrations/gusto/onboarding.py#L96-L108)).

---

## 3. The API surface actually called

All reads funnel through `GustoClient._request`
([client.py:125‑176](../../../services/ingest/integrations/gusto/client.py#L125-L176)), which:

- sets `Authorization: Bearer {token}` + `Accept: application/json`,
- honours `429 Retry-After` within a bounded budget
  (`GUSTO_RL_MAX_ATTEMPTS`=4, `GUSTO_RL_MAX_SLEEP_SEC`=30 s),
- maps any non-2xx to `GustoApiError` (401/403 → `gusto_api_unauthorized`,
  404 → `gusto_api_not_found`, 429 → `gusto_api_rate_limited`, else
  `gusto_api_error`) ([client.py:235‑266](../../../services/ingest/integrations/gusto/client.py#L235-L266)).

The endpoints invoked for ingestion:

| Gusto endpoint | Wrapper | Purpose | Code |
|----------------|---------|---------|------|
| `GET /v3/company/{company}/query?query=SELECT…` | `GustoClient.query(entity, …)` | one page of an entity stream (SQL-like `SELECT * FROM <Entity> … STARTPOSITION n MAXRESULTS m`) | [client.py:182‑214](../../../services/ingest/integrations/gusto/client.py#L182-L214) |
| `GET /v3/company/{company}/companyinfo/{company}` | `GustoClient.company_info()` | connectivity / credential probe (used by preflight + finalize + onboarding) | [client.py:216‑221](../../../services/ingest/integrations/gusto/client.py#L216-L221) |

Every request also carries `minorversion=75`
([client.py:46](../../../services/ingest/integrations/gusto/client.py#L46),
[client.py:203](../../../services/ingest/integrations/gusto/client.py#L203)) — another
QuickBooks-ism the Gusto clone inherited.

> **(inferred)** The `query` endpoint, the `SELECT … STARTPOSITION` query
> language, `minorversion`, and `Metadata.LastUpdatedTime` are all QuickBooks
> Online Accounting API constructs. The source itself flags this as a placeholder
> for Gusto's "real REST surface … page/per_page (and an `updated_since` style
> filter)" ([fetchers/gusto.py:29‑37](../../../services/ingest/ingestion/fetchers/gusto.py#L29-L37)).
> See the `TODO(human)` notes below.

### 3.1 Pagination — `STARTPOSITION` offset

The query API pages by **offset**: `STARTPOSITION n MAXRESULTS m`. `query()`
returns `(rows, next_start_position)`; `next_start_position is None` is terminal
(a short page — `maxResults < max_results` — or no rows ends the walk)
([client.py:182‑214](../../../services/ingest/integrations/gusto/client.py#L182-L214)). The fetcher
persists `start_position` in the shard cursor and resumes on the next invocation
([fetchers/gusto.py:177‑190](../../../services/ingest/ingestion/fetchers/gusto.py#L177-L190)).
Page size is env-overridable via `GUSTO_BACKFILL_PAGE_SIZE` (default 100, capped
at 1000) ([fetchers/gusto.py:59‑63](../../../services/ingest/ingestion/fetchers/gusto.py#L59-L63)).

### 3.2 Rate limits — no dedicated client-side bucket

There is **no Gusto entry** in the client-side token-bucket table
(`services/ingest/ingestion/rate_limit/buckets.py` defines buckets for Slack,
GitHub, etc., but **none for `gusto`**). Gusto rate-limiting is handled purely
**reactively** in the client: a `429` triggers a `Retry-After`-aware sleep within
the `GUSTO_RL_MAX_ATTEMPTS` / `GUSTO_RL_MAX_SLEEP_SEC` budget; once the budget is
exhausted the call raises `gusto_api_rate_limited`, and the **fetcher treats that
as a soft no-op page** (`end_of_data=False`, cursor unchanged) so the shard
retries later rather than failing the run
([fetchers/gusto.py:156‑165](../../../services/ingest/ingestion/fetchers/gusto.py#L156-L165)).

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one `gusto_entity` shard per active
entity type** ([planners/gusto.py:54‑83](../../../services/ingest/ingestion/planners/gusto.py#L54-L83)).
There is a **single** shard kind, `gusto_entity`
([planners/gusto.py:35](../../../services/ingest/ingestion/planners/gusto.py#L35)).

The planner is **entity-type-agnostic** — `ctx.source_client` is `None` and the
active entity list is read from DB state. The `SourceOnboarding` loader
JSON-aggregates the `gusto_entities` child table into `ctx.install["entities"]`,
and the planner emits one shard per `entity_type`, warm-starting each with its
persisted `updated_cursor` (`None` on first sync)
([planners/gusto.py:38‑77](../../../services/ingest/ingestion/planners/gusto.py#L38-L77)):

```python
Shard(
    shard_kind="gusto_entity",
    shard_identifier={
        "shard_kind": "gusto_entity",
        "entity_type": entity_type,            # Invoice | Bill | BillPayment | Payment
        "company_uuid": company_uuid,
        "installation_id": install_id,
        "updated_cursor": ent.get("updated_cursor"),  # high-water LastUpdatedTime
    },
    recency_score=1.0,
)
```

The seeded entity set is `DEFAULT_ENTITIES = ("Invoice", "Bill", "BillPayment",
"Payment")` ([client.py:269‑271](../../../services/ingest/integrations/gusto/client.py#L269-L271)), so a
default install backfills **four shards**.

> **TODO(human) — entity taxonomy.** The planner docstring says the seeded
> entities are `payrolls`, `employees`, `contractor_payments` and asks to confirm
> the resource taxonomy ([planners/gusto.py:12‑18](../../../services/ingest/ingestion/planners/gusto.py#L12-L18)).
> **This is stale relative to the running code:** `DEFAULT_ENTITIES` and the
> handler's `_ENTITY_NORMALISE` map only know the four QuickBooks accounting
> entities. A planner shard for `payrolls` would reach the handler and be rejected
> as an unsupported record type
> ([handlers/gusto.py:47‑52](../../../services/ingest/ingestion/handlers/gusto.py#L47-L52),
> [handlers/gusto.py:463‑468](../../../services/ingest/ingestion/handlers/gusto.py#L463-L468)).

---

## 5. Fetch specifics — one shard, two sync modes

`fetch_page_gusto` ([fetchers/gusto.py:123‑192](../../../services/ingest/ingestion/fetchers/gusto.py#L123-L192))
takes one `(install, shard_identifier, cursor)` triple and returns one page of
records + the next cursor. A `gusto_entity` shard runs in one of two modes:

- **FULL** (initial backfill): `SELECT * FROM <Entity> ORDERBY
  Metadata.LastUpdatedTime …`, offset-paginated, no `WHERE`.
- **INCREMENTAL** (poll): when warm-started with an `updated_cursor`, the first
  page **seeds** `incremental_floor = high_water_updated = updated_cursor`
  ([fetchers/gusto.py:134‑139](../../../services/ingest/ingestion/fetchers/gusto.py#L134-L139)), and the
  `WHERE` clause adds `Metadata.LastUpdatedTime > '<floor>'` so only changed
  entities come back ([fetchers/gusto.py:142‑145](../../../services/ingest/ingestion/fetchers/gusto.py#L142-L145)).

### 5.1 Cursor

```python
class GustoCursor(BaseModel):          # extra="forbid"
    start_position: int = 1            # STARTPOSITION offset within this run
    high_water_updated: str | None     # max LastUpdatedTime seen — warm-start /
                                       #   incremental floor AND reconciler ref
    incremental_floor: str | None      # the LastUpdatedTime > lower bound (None in FULL)
    rows_seen: int = 0                 # diagnostic
    seeded: bool = False               # first-call setup ran
```

([fetchers/gusto.py:66‑86](../../../services/ingest/ingestion/fetchers/gusto.py#L66-L86)).
`high_water_updated` advances over every row via `_bump_high_water`
(monotonic max of `MetaData`/`Metadata`.`LastUpdatedTime`)
([fetchers/gusto.py:104‑117](../../../services/ingest/ingestion/fetchers/gusto.py#L104-L117),
[fetchers/gusto.py:174](../../../services/ingest/ingestion/fetchers/gusto.py#L174)).

### 5.2 Record shape handed to the handler

Each entity row is emitted as a **fetcher-tagged record** — the QuickBooks
parallel to GitHub's "reshape into webhook body" — so the one handler can treat
backfill, poll, and webhook identically
([fetchers/gusto.py:167‑173](../../../services/ingest/ingestion/fetchers/gusto.py#L167-L173)):

```python
{
    "_fyralis_record_type": entity_type.lower(),   # invoice | bill | billpayment | payment
    "_fyralis_company_uuid": company_uuid,
    "entity": row,                                  # the full SELECT * body
}
```

> **TODO(human) — pagination + incremental filter (×2).** The fetcher flags that
> `client.query(...)`, the `STARTPOSITION`/`MAXRESULTS` offset paging, and the
> `LastUpdatedTime >` WHERE clause are a QuickBooks placeholder; Gusto's real
> surface is `page`/`per_page` with an `updated_since`-style filter and an
> unknown timestamp field name. If no incremental filter exists, the documented
> fallback is a **full re-walk** (idempotent via the versioned `external_id`)
> ([fetchers/gusto.py:29‑37](../../../services/ingest/ingestion/fetchers/gusto.py#L29-L37)).

---

## 6. The handler — shaping records into `ObservationDraft`

`handle_gusto_object` ([handlers/gusto.py:406‑475](../../../services/ingest/ingestion/handlers/gusto.py#L406-L475))
is a **pure function** (no DB / network) that branches on the input shape and
produces **exactly one** observation per call. It recognises three shapes:

1. **Live webhook** — an Intuit-style `eventNotifications` array
   ([handlers/gusto.py:418‑438](../../../services/ingest/ingestion/handlers/gusto.py#L418-L438)).
2. **Flattened webhook** (harness convenience) — a top-level `name`+`id`, with an
   optional full `entity` body ([handlers/gusto.py:442‑458](../../../services/ingest/ingestion/handlers/gusto.py#L442-L458)).
3. **Backfill / poll record** — a `_fyralis_record_type`-tagged record from the
   fetcher ([handlers/gusto.py:461‑470](../../../services/ingest/ingestion/handlers/gusto.py#L461-L470)).

A payload matching none of these raises `ValidationError`
([handlers/gusto.py:472‑475](../../../services/ingest/ingestion/handlers/gusto.py#L472-L475)).

### 6.1 Full-body draft (`_entity_draft`)

For a full entity body (backfill, poll, or a webhook that carried `entity`), the
draft is built by `_entity_draft`
([handlers/gusto.py:273‑350](../../../services/ingest/ingestion/handlers/gusto.py#L273-L350)). Branching on
the normalised entity kind:

| Record kind (`_fyralis_record_type` / webhook `name`) | `external_id` | `occurred_at` | `kind` | Trust tier |
|---|---|---|---|---|
| `invoice` / `bill` — **balance ≤ 0** | `gusto:{co}:{kind}:{id}:{SyncToken}` | `Metadata.LastUpdatedTime` or now | `state_change` (status `paid`) | `authoritative` |
| `invoice` / `bill` — **past due, balance > 0** | ″ | ″ | `state_change` (status `overdue`) | `authoritative` |
| `invoice` / `bill` — otherwise | ″ | ″ | `signal` (status `open`) | `authoritative` |
| `billpayment` → `bill_payment` / `payment` | ″ | ″ | `signal` (status `recorded`) | `authoritative` |

The signal mapping is the reasoning value: a zero-balance invoice/bill is the
AR-collected / AP-cleared event; a past-due open balance is the cash-risk event;
everything else is a plain signal ([handlers/gusto.py:250‑266](../../../services/ingest/ingestion/handlers/gusto.py#L250-L266)).

Other notable fields the draft extracts:

- **`source_actor_ref`** — `gusto:{customer|vendor}:{ref_value}` from
  `CustomerRef` / `VendorRef`, else `None`
  ([handlers/gusto.py:101‑115](../../../services/ingest/ingestion/handlers/gusto.py#L101-L115)).
- **`content`** — a money/status header plus rich extras (line items, `LinkedTxn`
  AR/AP graph edges, tax, multi-currency, payment channel, P&L dimensions)
  pulled from the already-fetched `SELECT *` body
  ([handlers/gusto.py:166‑247](../../../services/ingest/ingestion/handlers/gusto.py#L166-L247)).
- **`entities_hint`** — a `gusto_object` ref (`{kind}:{id}`) plus the
  customer/vendor `organization` hint
  ([handlers/gusto.py:305‑309](../../../services/ingest/ingestion/handlers/gusto.py#L305-L309)).
- **`raw_payload`** — the full entity body.

### 6.2 Trust tier

`gusto:object` is **`authoritative`** — Gusto is treated as the accounting system
of record ([handlers/gusto.py:44](../../../services/ingest/ingestion/handlers/gusto.py#L44)). The handler
registers this at import via
`CHANNEL_TRUST_MAP.setdefault("gusto:object", "authoritative")`
([handlers/gusto.py:478](../../../services/ingest/ingestion/handlers/gusto.py#L478)); the channel is **not**
in the static `CHANNEL_TRUST_MAP` literal
([handlers/__init__.py:41‑75](../../../services/ingest/ingestion/handlers/__init__.py#L41-L75)) — it is
inserted when the handler module is imported
([handlers/__init__.py:181](../../../services/ingest/ingestion/handlers/__init__.py#L181)).

---

## 7. Live (real-time) ingestion via webhooks

When a change occurs in Gusto, Gusto **POSTs an HMAC-signed webhook** to Fyralis's
webhook edge. The webhook router maps provider `gusto` → channel `gusto:object`
([webhooks/router.py:457](../../../services/app/webhooks/router.py#L457)), the same channel as
backfill/poll.

### 7.1 Signature verification (HMAC, no timestamp envelope)

`GustoVerifier.verify` computes `HMAC-SHA256(secret, raw_body)` and constant-time
compares it against a signature header, looping over **all** active secrets (to
support per-subscription secret rotation)
([signatures/gusto.py:53‑89](../../../services/app/webhooks/signatures/gusto.py#L53-L89)). The verifier is
registered as `VERIFIERS["gusto"] = gusto.verifier`
([signatures/__init__.py:57](../../../services/app/webhooks/signatures/__init__.py#L57)). The per-tenant
signing secret is the `gusto_webhook_verifier` token, loaded from the
`provider_installations` row by `services/app/webhooks/secrets.py`.

Like GitHub/Jira, the digest is over the **body alone** — there is no timestamp
envelope and therefore **no replay window**; idempotency is enforced downstream
by the versioned `external_id` ([signatures/gusto.py:19‑21](../../../services/app/webhooks/signatures/gusto.py#L19-L21)).
There is **no Gusto-specific replay cache** (the `(installation, delivery)` replay
cache in the router is GitHub-only).

> **TODO(human) — signature scheme UNVERIFIED.** The exact header **name**, the
> digest **algorithm**, and the **encoding** (base64 vs hex, `sha256=` prefix?)
> are unconfirmed. The verifier defaults to the QuickBooks/Intuit archetype but
> exposes all three as one-line module constants — `_SIGNATURE_HEADER =
> "Gusto-Signature"`, `_DIGEST_ENCODING = "base64"`, `_SIGNATURE_PREFIX = ""`
> ([signatures/gusto.py:6‑13](../../../services/app/webhooks/signatures/gusto.py#L6-L13),
> [signatures/gusto.py:42‑44](../../../services/app/webhooks/signatures/gusto.py#L42-L44)).

### 7.2 Tenant resolution + the thin-change observation

The tenant is resolved by `_extract_gusto` from the company id
([tenant_resolver.py:396‑414](../../../services/app/webhooks/tenant_resolver.py#L396-L414)): it reads
`company_uuid` from `eventNotifications[0]` or the top-level body, mapping to the
`provider_installations` row keyed `(provider='gusto', installation_id=company_uuid)`
([onboarding.py:118‑140](../../../services/ingest/integrations/gusto/onboarding.py#L118-L140)).

> **(inferred) Field-name mismatch worth noting.** The **tenant resolver** reads
> `company_uuid`, but the **handler** reads `companyId`
> ([handlers/gusto.py:395‑399](../../../services/ingest/ingestion/handlers/gusto.py#L395-L399),
> [handlers/gusto.py:421](../../../services/ingest/ingestion/handlers/gusto.py#L421)). The synthetic
> harness papers over this by sending **both** keys in every live event
> ([finance_router.py:562‑579](../../../services/app/gateway/finance_router.py#L562-L579)). A real Gusto
> payload using only one spelling would resolve the tenant *or* the handler's
> company, not necessarily both. Both sides carry an explicit `TODO(human)` to
> confirm the real field against Gusto's docs
> ([tenant_resolver.py:405‑406](../../../services/app/webhooks/tenant_resolver.py#L405-L406)).

Because Gusto (Intuit-style) webhooks carry only `id`+`operation`, not the full
entity body, the handler emits a **thin change observation** via
`_thin_change_draft` ([handlers/gusto.py:353‑392](../../../services/ingest/ingestion/handlers/gusto.py#L353-L392)):

- `kind="signal"`, `trust_tier="authoritative"`, `source_actor_ref=None`,
  `raw_payload=None`, `content.thin_change=True`;
- `external_id = "gusto:{co}:{kind}:{id}:chg:{version}"`, where `version` is the
  webhook's `lastUpdated` (or now) — a **distinct namespace** from the full-body
  `…:{SyncToken}` key ([idempotency:223‑229](../../../services/ingest/ingestion/idempotency/__init__.py#L223-L229)).

The next poll/backfill re-fetch then writes the authoritative full-body
observation, deduping by `SyncToken`. So the thin change is a low-fidelity
"something moved" marker, **not** the canonical record.

### 7.3 Kafka cutover (inline `gusto:object` when off)

Gusto is registered for the **202 cutover** path: provider `gusto` is in both the
data-plane provider set ([webhooks/router.py:139](../../../services/app/webhooks/router.py#L139)) and the
cutover-enabled set ([webhooks/router.py:178](../../../services/app/webhooks/router.py#L178)). When the
tenant's `ingestion.kafka_path_enabled=TRUE`, the webhook is published to Kafka
and returns `202` (skipping inline ingest). When the flag is **off**, the router
falls back to inline ingest on the `gusto:object` channel
(`_PROVIDER_CHANNEL["gusto"] = "gusto:object"`,
[webhooks/router.py:437](../../../services/app/webhooks/router.py#L437),
[webhooks/router.py:457](../../../services/app/webhooks/router.py#L457)) — the same handler the cutover
path would have invoked.

---

## 8. Reconciliation / gap detection (= the poll mechanism)

`reconcile_gusto` ([reconcilers/gusto.py:124‑162](../../../services/ingest/ingestion/reconcilers/gusto.py#L124-L162))
re-checks **completed** (`state='done'`) `gusto_entity` shards for new activity.
Per shard ([reconcilers/gusto.py:79‑121](../../../services/ingest/ingestion/reconcilers/gusto.py#L79-L121)):

1. Load the shard's `high_water_updated` from its persisted `shard_fetch` cursor
   ([reconcilers/gusto.py:68‑76](../../../services/ingest/ingestion/reconcilers/gusto.py#L68-L76)).
2. Issue a **cheap 1-row probe**: `query(entity_type, where="LastUpdatedTime >
   '<high_water>'", max_results=1)`.
3. If any row comes back, emit a **reshare** of the same `gusto_entity` shard at
   **`recency_score=1.5`**, warm-started via `updated_cursor=high_water` (so the
   reshared shard runs in INCREMENTAL/poll mode) and carrying
   `parent_shard_id` + `gap_baseline_updated`.

A failed probe is best-effort: it is logged and the shard is left clean rather
than failing the run ([reconcilers/gusto.py:100‑105](../../../services/ingest/ingestion/reconcilers/gusto.py#L100-L105)).
This is the mechanism that produces the **"poll" ingress kind**: there is no
separate Gusto poll driver — the reconciler's reshare + warm-started fetcher
*are* the incremental poll. The reconciler is registered as
`RECONCILER_DISPATCH["gusto"] = reconcile_gusto`
([reconcilers/gusto.py:165](../../../services/ingest/ingestion/reconcilers/gusto.py#L165)) and reads its
install from `gusto_installations` (skipping `disabled_at`)
([reconcilers/gusto.py:132‑143](../../../services/ingest/ingestion/reconcilers/gusto.py#L132-L143)).

Because `external_id` is `SyncToken`-versioned, an over-reshare is harmless: an
unchanged entity re-walked dedups against what backfill already wrote; only a
genuinely new/changed `SyncToken` produces a new observation. The reconciler can
over-reshare but **never under-reshares**
([reconcilers/gusto.py:9‑12](../../../services/ingest/ingestion/reconcilers/gusto.py#L9-L12)).

---

## 9. Revocation chokepoint — **absent** (recoverable-error behaviour)

Unlike GitHub (a 401/404 disables the install) or Notion (a revocation
chokepoint parks + disables on token revocation), **Gusto has no revocation
chokepoint**. There is no code path that flips `gusto_installations.disabled_at`
on an auth failure. Behaviour on a rejected/expired token:

- The client raises `GustoApiError(code="gusto_api_unauthorized")` on 401/403
  ([client.py:172‑176](../../../services/ingest/integrations/gusto/client.py#L172-L176),
  [client.py:239‑245](../../../services/ingest/integrations/gusto/client.py#L239-L245)).
- The **fetcher** only special-cases `gusto_api_rate_limited` (soft no-op,
  retry); **any other** `GustoApiError` — including `gusto_api_unauthorized` — is
  **re-raised** and fails the shard
  ([fetchers/gusto.py:156‑165](../../../services/ingest/ingestion/fetchers/gusto.py#L156-L165)).
- Recovery is **manual**: the operator re-runs `connect/finalize` with a fresh
  access token, which UPSERTs the install (`disabled_at = NULL`) and seeds a new
  trigger ([onboarding.py:60‑81](../../../services/ingest/integrations/gusto/onboarding.py#L60-L81)).

> **TODO(human) — token refresh seam.** Because there is no refresh loop (§2.1),
> an expired access token is a hard shard failure with no auto-recovery. Wiring
> either a refresh-on-401 exchange in the client or an `oauth_poller` is the
> documented fix ([client.py:8‑12](../../../services/ingest/integrations/gusto/client.py#L8-L12)).

---

## 10. End-to-end summary

```
                          ┌──────────────── BACKFILL / POLL (pull) ─────────────────┐
   OPERATOR PASTES        │  POST /integrations/gusto/connect/finalize               │
   company_uuid +         │    └─ company_info() verify → store tokens (encrypted)   │
   access_token           │    └─ gusto_installations + gusto_entities + trigger     │
                          │  planner: one gusto_entity shard per entity type          │
   FOUR ENTITY TYPES      │    (Invoice / Bill / BillPayment / Payment)               │
                          │  fetcher: GET /v3/company/{co}/query  SELECT * FROM <E>   │
                          │    FULL: STARTPOSITION paging                            │
                          │    POLL: WHERE LastUpdatedTime > '<high_water>'          │
                          │    └─ tag record {_fyralis_record_type, _co_uuid, entity}│
                          └──────────────────────────────────────────────────────┬──┘
                                                                                   │
                          ┌──────────────────── LIVE (push) ─────────────────────┐│
   any Gusto change ──────► HTTP POST /webhooks/gusto                            ││
                          │   verify HMAC-SHA256 over body (no ts → no replay win) ││
                          │   tenant ← company_uuid;  eventNotifications[…]        ││
                          │   id+operation only  →  THIN change observation        ││
                          │   (kafka_path_enabled ? 202→Kafka : inline gusto:object)││
                          └───────────────────────────────────────────────────────┘│
                                                                                   │
                                                  ┌────────────────────────────────▼─┐
                                                  │  handle_gusto_object               │
                                                  │  branch: webhook | flattened | tag │
                                                  │  external_id =                      │
                                                  │    gusto:{co}:{kind}:{id}:{SyncTok} │
                                                  │    (thin: …:{id}:chg:{version})     │
                                                  │  trust = authoritative              │
                                                  │  → ObservationDraft                 │
                                                  └─────────────────────────────────────┘
```

**Key invariants**

1. **One channel, one handler, one dedup namespace.** Backfill, poll, and webhook
   all route to `gusto:object` / `handle_gusto_object`. A full-body entity dedups
   on `gusto:{company}:{kind}:{id}:{SyncToken}`, so a backfilled object and its
   live/poll twin with the same `SyncToken` collapse to one observation.
2. **`SyncToken`-versioned external_id.** Gusto entities mutate (draft → paid →
   overdue); versioning by `SyncToken` makes each state change a **new**
   observation rather than a silently-dropped duplicate.
3. **Thin webhooks are markers, not records.** A live webhook carries only
   `id`+`operation`, so it emits a low-fidelity `…:chg:{version}` observation; the
   authoritative full body is filled in by the next poll re-fetch.
4. **OAuth access token, resolved once; no refresh, no revocation chokepoint.**
   An expired token fails the shard with no auto-recovery — recovery is a manual
   operator re-finalize. (Refresh + chokepoint are `TODO(human)` seams.)
5. **Poll = reconciler reshare.** There is no separate poll driver; the reconciler
   probes for `LastUpdatedTime > high_water` and reshares a warm-started shard at
   `recency_score=1.5`, which runs the same fetcher in INCREMENTAL mode.

---

## 11. Configuration & compliance

> Gusto ships as the OAuth / QuickBooks archetype with several **UNVERIFIED**
> seams (host, scopes, query surface, webhook signature, incremental filter).
> The checklist below marks what is *built and verified in code* vs *placeholder*.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `GUSTO_API_BASE_URL` | `https://api.gusto.com` ([endpoints.py:78](../../../lib/integrations/endpoints.py#L78), `TODO(human)` host) | overrides the Gusto API host (also overridable per-install via `base_url`) |
| `GUSTO_BACKFILL_PAGE_SIZE` | `100` (cap 1000) | `MAXRESULTS` page size for the query walk |
| `GUSTO_RL_MAX_ATTEMPTS` | `4` | rate-limit (429) retry budget |
| `GUSTO_RL_MAX_SLEEP_SEC` | `30` | max `Retry-After` backoff per attempt |

(No dedicated rate-limit-bucket env knob — Gusto has no client-side bucket; §3.2.)

### 11.2 Built & verified vs placeholder

- **Install** — operator-mediated `connect/preflight` + `connect/finalize`;
  creds verified via `company_info()` before any secret write. ✅ (built)
- **Read surface** — `/v3/company/{co}/query` + `companyinfo`, `minorversion=75`,
  `STARTPOSITION` offset paging. ✅ (built) / ⚠️ **(inferred** — this is the
  QuickBooks query shape; Gusto's real REST surface is a `TODO(human)`).
- **Dedup** — `SyncToken`-versioned `external_id`, distinct `…:chg:` namespace for
  thin webhooks. ✅ (built)
- **Webhook signing** — HMAC-SHA256 over the body, constant-time compare,
  multi-secret rotation, no replay window. ✅ (mechanism built) / ⚠️ **header
  name, encoding, prefix UNVERIFIED** (`TODO(human)`, §7.1).
- **Tenant resolution** — `company_uuid` from `eventNotifications`/top-level. ✅
  (built) / ⚠️ field name UNVERIFIED + handler/resolver key mismatch (§7.2).
- **OAuth bounce + token refresh** — ❌ **not built** (`TODO(human)`, §2.1).
- **Revocation chokepoint** — ❌ **not built**; recovery is manual re-finalize
  (§9).
- **Entity taxonomy** — `Invoice/Bill/BillPayment/Payment` (accounting). ⚠️
  comments claim payroll entities; **stale** (§4).

### 11.3 Dev / spammer mode

For local testing against the mock source servers, `build_gusto_client` detects
spammer mode and **presets the access token** to `"spam-gusto"` (skipping the
secret-store lookup) and **points the API base** at the local spammer's `/gusto`
sub-path via the endpoint resolver
([_clients.py:485‑512](../../../services/ingest/ingestion/fetchers/_clients.py#L485-L512),
[endpoints.py:163](../../../lib/integrations/endpoints.py#L163)). The mock server matches the
`/v3/company/.../query` path **suffix**
([mock_servers/gusto.py:18‑24](../../../services/ingest/synthetic/mock_servers/gusto.py#L18-L24)).

The dev finance panel (`finance_router`, `X-Tenant-Id` header, synthetic data)
generates **QBO-shaped** backfill records (`_fyralis_company_uuid` keyed) and live
`eventNotifications` bodies that deliberately carry **both** `company_uuid` and
`companyId` ([finance_router.py:551‑579](../../../services/app/gateway/finance_router.py#L551-L579)) — the
production-grade install surface is the Bearer-authed `connect/*` routes in §2.1.
