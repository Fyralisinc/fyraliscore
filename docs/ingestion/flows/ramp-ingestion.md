# Ramp Ingestion — How Fyralis Pulls Ramp Data

This document explains, in detail, **how Ramp data enters Fyralis**: which Ramp
APIs are called, with which token, and how Ramp's spend/card signal — modelled
here as a single **transaction** stream — is ingested across backfill, poll, and
live webhook.

It deliberately stops at the point where a Ramp change becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **⚠️ Status: cloned-from-archetype, largely UNVERIFIED.** Unlike the GitHub and
> Slack flows, the Ramp integration is a **structural clone of the QuickBooks
> (QBO) OAuth archetype**. The module headers themselves say so, and the real
> Ramp read surface (host, endpoints, query-vs-REST, OAuth scopes, webhook
> signature scheme, transaction-state vocabulary) is **not yet verified against
> Ramp's docs** — it is kept configurable behind archetype defaults with explicit
> `TODO(human)` markers in the code. This doc describes **what the code actually
> does today** (the QBO-shaped clone) and flags every place the behaviour is a
> placeholder. Where the code's behaviour diverges from the (stale) ground-truth
> comments, the code wins and the divergence is called out.

---

## 1. The three ways data arrives

Ramp data reaches Fyralis through **three paths that converge on one handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding | Fyralis *pulls* history via the cloned Ramp **query endpoint** (`GET /v3/company/{businessId}/query`) | [planners/ramp.py](../../../services/ingest/ingestion/planners/ramp.py), [fetchers/ramp.py](../../../services/ingest/ingestion/fetchers/ramp.py) |
| **Poll (incremental)** | Reconciliation / re-run | Same fetcher under `ingress_kind="poll"`, warm-started with the `LastUpdatedTime` high-water cursor | [fetchers/ramp.py:139‑150](../../../services/ingest/ingestion/fetchers/ramp.py#L139-L150) |
| **Live (real-time)** | A Ramp change event | Ramp *pushes* an HMAC-signed **webhook** to Fyralis | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/ramp.py](../../../services/app/webhooks/signatures/ramp.py), [handlers/ramp.py](../../../services/ingest/ingestion/handlers/ramp.py) |

All three converge on the **single** `ramp:transaction` handler
([handlers/ramp.py:454‑455](../../../services/ingest/ingestion/handlers/ramp.py#L454-L455)),
which `@register`s that channel. The channel mapping confirms the convergence —
all three `(provider, ingress_kind)` pairs map to one channel
([channel_mapping.py:183‑185](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L183-L185)):

```
("ramp","backfill") → "ramp:transaction"
("ramp","poll")     → "ramp:transaction"
("ramp","webhook")  → "ramp:transaction"
```

### 1.1 The dedup key — *as built in code* (versioned by state)

The ground-truth hint (and the `channel_mapping.py` comment at
[line 181](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L181))
describe the external_id as `ramp:{business}:txn:{id}:{state}`. **The code is more
specific than that.** The handler builds two distinct shapes:

```
backfill / poll, and live webhook WITH a full entity body:
    external_id = "ramp:{business_id}:txn:{entity_id}:{state_token}"
        where state_token = "{status_word}.{SyncToken}"   (SyncToken != "0")
                          = "{status_word}"                 (SyncToken == "0")

live webhook with NO entity body (thin notification):
    external_id = "ramp:{business_id}:txn:{entity_id}:chg:{version}"
        where version = entity lastUpdated, else now()
```

- The entity-draft form is built by `_txn_external_id` /`_entity_draft`
  ([handlers/ramp.py:61‑67](../../../services/ingest/ingestion/handlers/ramp.py#L61-L67),
  [337‑338](../../../services/ingest/ingestion/handlers/ramp.py#L337-L338)). The
  `state` segment is **not** a bare Ramp state string — it folds the `SyncToken`
  in (`paid.3`, `overdue.1`, …) so each in-place edit of a mutable record stays a
  distinct observation. (`status_word` itself comes from `_classify`, §7.)
- The thin-change form is built by `_change_external_id` /`_thin_change_draft`
  ([handlers/ramp.py:70‑74](../../../services/ingest/ingestion/handlers/ramp.py#L70-L74),
  [402‑417](../../../services/ingest/ingestion/handlers/ramp.py#L402-L417)).

**Dedup consequence.** A backfilled transaction and its live twin collapse into
**one** observation **only when the live webhook carries the full entity body**
(so both go through `_entity_draft` with the identical `state_token`). A
**body-less** live notification produces a `…:chg:{version}` observation that
does **not** dedup against the backfill twin — by design, the next poll re-fetch
carries the authoritative body and dedups then
([handlers/ramp.py:72‑74](../../../services/ingest/ingestion/handlers/ramp.py#L72-L74)).
This is a weaker invariant than GitHub/Slack's "one dedup namespace, always": for
Ramp it holds **for the entity-body path** and is eventually-consistent for the
thin-notification path.

> The handler is a **pure function** (no DB, no network) and branches on input
> shape to emit exactly one observation per call
> ([handlers/ramp.py:454‑523](../../../services/ingest/ingestion/handlers/ramp.py#L454-L523)) —
> the same single-handler discipline as GitHub's `github:webhook` and Jira's
> `jira:issue`.

---

## 2. Authentication & token model

Ramp is documented in the code as the **OAuth 2.0 / QuickBooks archetype**: a
short-lived **access token** (Bearer, ~hours) plus a rotating **refresh token**,
with every call scoped to a company `businessId`
([client.py:1‑8](../../../services/ingest/integrations/ramp/client.py#L1-L8),
[oauth.py:1‑16](../../../services/ingest/integrations/ramp/oauth.py#L1-L16)).
This is **3-legged OAuth in archetype shape, not client-credentials** — the
operator obtains the tokens from a Ramp OAuth app per company.

### 2.1 What the code actually implements

The read client carries **only the current access token** and reuses it for the
life of the client ([client.py:107‑129](../../../services/ingest/integrations/ramp/client.py#L107-L129)):

- The token is resolved **once** from the secret store via the install's
  `secret_ref` (or preset in spammer mode), guarded by an `asyncio.Lock`.
- Every request sends `Authorization: Bearer {token}` + `Accept: application/json`
  ([client.py:136‑141](../../../services/ingest/integrations/ramp/client.py#L136-L141)).
- The token / auth header are **never logged** (the module header states this as
  the redaction invariant, [client.py:31](../../../services/ingest/integrations/ramp/client.py#L31)).

> **There is no OAuth bounce and no token refresh.** Unlike GitHub's JWT→install
> token mint, the Ramp client never refreshes. On a `401`/`403` it raises
> `RampApiError(code="ramp_api_unauthorized")` with a "may need refresh" message
> ([client.py:178‑182](../../../services/ingest/integrations/ramp/client.py#L178-L182),
> [245‑251](../../../services/ingest/integrations/ramp/client.py#L245-L251)) and
> stops — it does **not** re-mint, retry, or disable the install (§10).
>
> **TODO(human):** implement Ramp OAuth token refresh (refresh-on-401 or a poller).
> *(Reason: the code header at [client.py:22‑24](../../../services/ingest/integrations/ramp/client.py#L22-L24)
> and [oauth.py:13‑16](../../../services/ingest/integrations/ramp/oauth.py#L13-L16)
> declares this the "QBO seam" — `refresh_secret_ref` + `token_expires_at` columns
> are persisted but nothing exchanges the refresh token. The exchange endpoint,
> grant flow, and rotation are unverified. Do not assume tokens never expire.)*

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| Access token | secret store, label `ramp_access_token:{business_id}` | encrypted at rest; only the opaque `secret_ref` reaches `ramp_installations.secret_ref` ([oauth.py:199‑201](../../../services/ingest/integrations/ramp/oauth.py#L199-L201)) |
| Refresh token | secret store, label `ramp_refresh_token:{business_id}` | optional; `refresh_secret_ref` column; "owned by the oauth_poller in production" but no poller exists ([oauth.py:202‑207](../../../services/ingest/integrations/ramp/oauth.py#L202-L207)) |
| Webhook verifier token | secret store, label `ramp_webhook_verifier:{business_id}` | optional; only set when live webhooks are wanted; drives the HMAC verify (§8) ([oauth.py:208‑213](../../../services/ingest/integrations/ramp/oauth.py#L208-L213)) |
| `business_id` | `ramp_installations.business_id`; `provider_installations.installation_id` for the webhook edge | the realm-id-equivalent scope on every call |

### 2.3 The install surface — operator-mediated, NOT a redirect bounce

`services/ingest/integrations/ramp/oauth.py` exposes a two-step **operator
credential-submission** wizard (Bearer-authed; tenant from `request.state.auth`),
not an OAuth redirect:

1. **`POST /integrations/ramp/connect/preflight`** — body `{business_id,
   access_token, base_url?}`; calls `RampClient.company_info()` to verify the
   token + business; on auth failure returns a structured `400` and **stores no
   secret** ([oauth.py:136‑161](../../../services/ingest/integrations/ramp/oauth.py#L136-L161)).
2. **`POST /integrations/ramp/connect/finalize`** — re-verifies creds **before any
   write**, persists the tokens encrypted, then `finalize_install()` upserts
   `ramp_installations` + one `ramp_entities` row per entity + an
   `onboarding_triggers` row (`source='ramp'`) so the backfill chain fires; if a
   webhook verifier token was supplied, `register_webhook_installation()` seeds the
   `provider_installations` row ([oauth.py:164‑250](../../../services/ingest/integrations/ramp/oauth.py#L164-L250)).

> **VERIFIED GAP — this production router is not mounted.** `route_mounts.py`
> mounts the jira / mercury / quickbooks install routers
> ([route_mounts.py:88‑103](../../../services/app/gateway/route_mounts.py#L88-L103))
> but **not** the Ramp one — there is no `include_router(ramp oauth router)`
> anywhere in the gateway. So in the running app the **only** way a Ramp install is
> actually created today is the dev **finance panel**
> (`finance_router`, gated by `settings.finance_panel_enabled`,
> [route_mounts.py:105‑109](../../../services/app/gateway/route_mounts.py#L105-L109)),
> which calls the same `finalize_install` / `register_webhook_installation` with a
> synthetic `business_id = "biz-{tenant.hex[:12]}"`
> ([finance_router.py:705‑719](../../../services/app/gateway/finance_router.py#L705-L719)).
> The `connect/preflight`+`connect/finalize` code path is reachable only if a
> future change mounts it.
>
> **TODO(human):** confirm whether the Ramp `connect` router should be mounted in
> `route_mounts.py` alongside mercury/quickbooks. *(Reason: the `oauth.py` header
> at [lines 18‑23](../../../services/ingest/integrations/ramp/oauth.py#L18-L23)
> calls this "the production surface the audit flagged as missing", but the import
> was never added — so production install of Ramp is currently impossible.)*

---

## 3. The API surface actually called

All reads funnel through `RampClient._request`
([client.py:131‑182](../../../services/ingest/integrations/ramp/client.py#L131-L182)),
which sets the Bearer header, retries `429` honouring `Retry-After` within a
bounded budget, maps any non-2xx to `RampApiError`, and increments per-outcome
counters in `metrics.py`.

| Endpoint (cloned QBO shape) | Wrapper | Purpose | Code |
|-----------------------------|---------|---------|------|
| `GET /v3/company/{businessId}/query?query=<SQL>&minorversion=75` | `RampClient.query(entity, …)` | one offset page of an entity stream | [client.py:188‑220](../../../services/ingest/integrations/ramp/client.py#L188-L220) |
| `GET /v3/company/{businessId}/companyinfo/{businessId}?minorversion=75` | `RampClient.company_info()` | connectivity / credential probe (preflight + reconciler client open) | [client.py:222‑227](../../../services/ingest/integrations/ramp/client.py#L222-L227) |

The query response is unwrapped from `{"QueryResponse": {"<Entity>": [...],
"maxResults": n}}` ([client.py:211‑220](../../../services/ingest/integrations/ramp/client.py#L211-L220)).

> **TODO(human):** confirm Ramp read endpoints + OAuth scopes. *(Reason: the
> client header at [lines 11‑20](../../../services/ingest/integrations/ramp/client.py#L11-L20)
> says Ramp is **likely a REST list API** under
> `https://api.ramp.com/developer/v1`, NOT a SQL-query API. If so, `query()` must
> become a `list + from_date` REST call. The host itself is unverified —
> [endpoints.py:75](../../../lib/integrations/endpoints.py#L75) carries a
> `# TODO(human): confirm host` on the default `ramp_api` base.)*

### 3.1 Pagination — offset based (`STARTPOSITION` / `MAXRESULTS`)

`query()` embeds `ORDERBY {field} STARTPOSITION {n} MAXRESULTS {m}` in the SQL and
returns `(rows, next_start_position)`; a short page (`maxResults < requested` or
empty) is terminal, signalled by `next_start_position is None`
([client.py:204‑220](../../../services/ingest/integrations/ramp/client.py#L204-L220)).
The fetcher persists `start_position` in the shard cursor and resumes next
invocation.

> **TODO(human):** confirm Ramp's real pagination scheme (offset/`STARTPOSITION`
> vs cursor/page-token) and whether a `from_date`/`since` filter exists. *(Reason:
> [fetchers/ramp.py:32‑42](../../../services/ingest/ingestion/fetchers/ramp.py#L32-L42)
> — the offset/limit shape is the QBO archetype default; if no incremental filter
> exists, the contract falls back to a full idempotent re-walk, which the
> versioned `external_id` makes safe.)*

### 3.2 Rate limits — **no dedicated bucket**

There is **no Ramp entry** in the ingestion token-bucket table
([rate_limit/buckets.py:88‑90](../../../services/ingest/ingestion/rate_limit/buckets.py#L88-L90)
lists only github/gmail/discord and the slack tiers — no `ramp`). Ramp's
back-pressure is **entirely client-side**: on a `429` the client sleeps for
`min(RAMP_RL_MAX_SLEEP_SEC, Retry-After)` and retries up to
`RAMP_RL_MAX_ATTEMPTS` times, then raises `ramp_api_rate_limited`
([client.py:142‑164](../../../services/ingest/integrations/ramp/client.py#L142-L164),
[258‑267](../../../services/ingest/integrations/ramp/client.py#L258-L267)). The
client does **not** call `acquire()` on any token bucket.

> **TODO(human):** confirm Ramp rate-limit signalling (`429 + Retry-After` vs
> `X-RateLimit-Reset`). *(Reason: [client.py:26‑29](../../../services/ingest/integrations/ramp/client.py#L26-L29)
> — the `429 + Retry-After` scheme is the Mercury/QBO default assumption.)*

---

## 4. Backfill scope — the shard families

The planner decomposes one install into **one shard per active entity type**, all
of `shard_kind = "ramp_entity"`
([planners/ramp.py:53‑82](../../../services/ingest/ingestion/planners/ramp.py#L53-L82)).
There is **no per-business fan-out**: a Ramp install is already scoped to one
`businessId`, so the family is keyed on entity type, not business.

`ctx.source_client` is `None` — the planner reads the active entity list from DB
state (the `ramp_entities` rows, JSON-aggregated into `ctx.install["entities"]` by
the onboarding loader) and stays stateless
([planners/ramp.py:18‑19](../../../services/ingest/ingestion/planners/ramp.py#L18-L19),
[37‑57](../../../services/ingest/ingestion/planners/ramp.py#L37-L57)).

Each shard carries `entity_type`, `business_id`, `installation_id`, and an
`updated_cursor` (the warm-start `LastUpdatedTime` high-water; `None` on first
sync), at a baseline `recency_score=1.0`
([planners/ramp.py:64‑76](../../../services/ingest/ingestion/planners/ramp.py#L64-L76)).

> **The entity taxonomy is a placeholder.** `DEFAULT_ENTITIES =
> ("Invoice","Bill","BillPayment","Payment")`
> ([client.py:283](../../../services/ingest/integrations/ramp/client.py#L283)) is
> the **QBO** taxonomy, kept only so the cloned synthetic loop stays
> self-consistent. The verified Ramp taxonomy per the blueprint is
> `{transaction, card, reimbursement}`.
>
> **TODO(human):** confirm the Ramp resource taxonomy + exact entity
> names/casing, then re-key `ramp_entities` + the generator + the handler decode
> together. *(Reason: declared in three coupled places —
> [client.py:275‑282](../../../services/ingest/integrations/ramp/client.py#L275-L282),
> [planners/ramp.py:13‑16](../../../services/ingest/ingestion/planners/ramp.py#L13-L16),
> [fetchers/ramp.py:11‑12](../../../services/ingest/ingestion/fetchers/ramp.py#L11-L12).
> Start with the transaction flow — highest signal.)*

---

## 5. The fetcher — one shard kind, two sync modes

`fetch_page_ramp` ([fetchers/ramp.py:128‑197](../../../services/ingest/ingestion/fetchers/ramp.py#L128-L197))
takes one `(install, shard_identifier, cursor)` triple and returns one page +
the next cursor; `ShardFetch` calls it in a loop, persisting the cursor.

### 5.1 Cursor

```python
class RampCursor(BaseModel):
    start_position: int = 1            # STARTPOSITION offset within this run (1-based)
    high_water_updated: str | None     # max Metadata.LastUpdatedTime seen — warm-start AND reconciler reference
    incremental_floor: str | None      # the `LastUpdatedTime >` lower bound frozen for this run (None in FULL)
    rows_seen: int = 0                 # diagnostic
    seeded: bool = False               # first-call setup ran
```

([fetchers/ramp.py:71‑91](../../../services/ingest/ingestion/fetchers/ramp.py#L71-L91)).

### 5.2 FULL vs INCREMENTAL

On the first call (`not seeded`), if the shard arrived with an `updated_cursor`
(warm start from the planner or reconciler), the fetcher freezes it as both the
`incremental_floor` and the `high_water_updated`
([fetchers/ramp.py:139‑144](../../../services/ingest/ingestion/fetchers/ramp.py#L139-L144)):

- **FULL** (initial backfill, no floor): `SELECT * FROM <Entity> ORDERBY
  Metadata.LastUpdatedTime STARTPOSITION n MAXRESULTS m`, offset-paged.
- **INCREMENTAL** (poll / warm start): the `where` clause adds
  `Metadata.LastUpdatedTime > '<floor>'` so only changed entities come back
  ([fetchers/ramp.py:146‑150](../../../services/ingest/ingestion/fetchers/ramp.py#L146-L150)).

Each row's `Metadata.LastUpdatedTime` bumps `high_water_updated`
([fetchers/ramp.py:117‑122](../../../services/ingest/ingestion/fetchers/ramp.py#L117-L122),
[179](../../../services/ingest/ingestion/fetchers/ramp.py#L179)). Page size is
`RAMP_BACKFILL_PAGE_SIZE` (default 100, capped at 1000)
([fetchers/ramp.py:64‑68](../../../services/ingest/ingestion/fetchers/ramp.py#L64-L68)).

### 5.3 Records — tagged for the one handler

Each row is emitted as `{"_fyralis_record_type": <entity_type lowercased>,
"_fyralis_business_id": <business>, "entity": <row>}`
([fetchers/ramp.py:172‑179](../../../services/ingest/ingestion/fetchers/ramp.py#L172-L179)).
The private `_fyralis_record_type` tag is how the one `ramp:transaction` handler
tells a backfill/poll record from a webhook event (§7).

### 5.4 Rate-limit soft-yield

If `query()` raises `ramp_api_rate_limited` (retry budget exhausted), the fetcher
**does not fail the shard** — it returns an empty page with the **unchanged**
cursor and `end_of_data=False`, so ShardFetch re-drives the same page later
([fetchers/ramp.py:161‑169](../../../services/ingest/ingestion/fetchers/ramp.py#L161-L169)).
Any other `RampApiError` (incl. `401`/`403`) propagates and **fails the shard** —
there is no recovery path (§10).

---

## 6. Live (real-time) ingestion via webhooks

A Ramp change event is **POSTed** to Fyralis's webhook edge. Live and backfill
both land on the **same** `ramp:transaction` handler — the router maps provider
`ramp` → inline channel `ramp:transaction`
([router.py:456](../../../services/app/webhooks/router.py#L456)).

### 6.1 Signature verification — HMAC, body-only, **scheme UNVERIFIED**

`RampVerifier.verify` ([signatures/ramp.py:53‑89](../../../services/app/webhooks/signatures/ramp.py#L53-L89))
computes `HMAC-SHA256(verifier_token, raw_body)` and constant-time-compares it
against the `x-ramp-signature` header. The digest **encoding and header are
module constants** ([signatures/ramp.py:42‑44](../../../services/app/webhooks/signatures/ramp.py#L42-L44)):

```python
_SIGNATURE_HEADER = "x-ramp-signature"
_DIGEST_ENCODING  = "base64"   # "base64" | "hex"
_SIGNATURE_PREFIX = ""         # e.g. "sha256=" for a GitHub/Jira-style hex header
```

The verifier loops over **all** active secrets, so a verifier-token rotation never
drops a delivery ([signatures/ramp.py:69‑76](../../../services/app/webhooks/signatures/ramp.py#L69-L76)).
The per-tenant verifier token is loaded from the `provider_installations`
(`provider='ramp'`) row by the webhook `secrets.py` machinery. Ramp is registered
in the verifier registry as `"ramp": ramp.verifier`
([signatures/__init__.py:56](../../../services/app/webhooks/signatures/__init__.py#L56)).

> **No replay window.** Like GitHub/Jira, the digest is over the **body alone** —
> there is no timestamp envelope (`signed_timestamp=None`,
> [signatures/ramp.py:88](../../../services/app/webhooks/signatures/ramp.py#L88)).
> Idempotency is the ingestion-layer `external_id` dedup, not a replay cache.
>
> **TODO(human):** confirm the Ramp webhook signature scheme (header name +
> base64-vs-hex + any prefix). *(Reason: [signatures/ramp.py:1‑13](../../../services/app/webhooks/signatures/ramp.py#L1-L13)
> — the default is the archetype's safe HMAC-SHA256/base64/`x-ramp-signature`. If
> Ramp uses the GitHub/Jira `sha256=`-prefixed hex shape, set `_DIGEST_ENCODING="hex"`
> and `_SIGNATURE_PREFIX="sha256="` — no other change needed.)*

### 6.2 Tenant resolution

The tenant is resolved from the webhook body by `_extract_ramp`
([tenant_resolver.py:375‑393](../../../services/app/webhooks/tenant_resolver.py#L375-L393)):
it reads `eventNotifications[0].business_id` (QBO-style envelope), falling back to
a top-level `business_id`, and looks up `provider_installations` for
`(provider='ramp', installation_id=business_id)`. Ramp is in the resolver's
provider set ([tenant_resolver.py:78](../../../services/app/webhooks/tenant_resolver.py#L78)).

> **TODO(human):** confirm the Ramp webhook tenant-id field (`business_id` vs an
> event-envelope path). *(Reason: [tenant_resolver.py:384‑385](../../../services/app/webhooks/tenant_resolver.py#L384-L385)
> — mirrors `_extract_quickbooks`; the synthetic harness sends a top-level
> `business_id`.)*

### 6.3 Kafka cutover vs inline

The webhook edge has two modes (the M5.3 cutover, shared by all finance sources):

- **Cutover ON** (`ingestion.kafka_path_enabled=TRUE` for the tenant; `ramp` is in
  `_CUTOVER_ENABLED_PROVIDERS`, [router.py:177](../../../services/app/webhooks/router.py#L177)):
  after signature verify + tenant resolve, the body is `shadow_write_raw`-n to S3 +
  published to `ingestion.raw` (Kafka), flushed durably, and the edge returns
  **202** ([router.py:241‑314](../../../services/app/webhooks/router.py#L241-L314)).
  The shadow source is `ramp` ([router.py:138](../../../services/app/webhooks/router.py#L138)).
- **Cutover OFF (or any failure)**: graceful fallback to **inline** `ingest()` on
  the `ramp:transaction` channel ([router.py:437‑456](../../../services/app/webhooks/router.py#L437-L456)).
  The inline channel is the `_PROVIDER_CHANNEL["ramp"]` value and **must** match
  the handler's `@register("ramp:transaction")`.

Either way the body reaches the same `handle_ramp_transaction` handler; the
difference is only whether it travels via Kafka (202) or inline (200/201).

---

## 7. The handler — shaping events into `ObservationDraft`

`handle_ramp_transaction` ([handlers/ramp.py:454‑523](../../../services/ingest/ingestion/handlers/ramp.py#L454-L523))
branches on input shape and emits exactly one `ObservationDraft`:

| Input shape | Detection | Builder | Notes |
|-------------|-----------|---------|-------|
| **Backfill / poll record** | `_fyralis_record_type` present | `_entity_draft` (full body) | normal path |
| **Webhook `eventNotifications` envelope** | `eventNotifications` list present | `_thin_change_draft` (body-less) | first entity only ([handlers/ramp.py:466‑486](../../../services/ingest/ingestion/handlers/ramp.py#L466-L486)) |
| **Flattened harness webhook** | top-level `name` + `id`, no `_fyralis_record_type` | `_entity_draft` if a full `entity` body is attached, else `_thin_change_draft` | dev/finance-panel convenience ([handlers/ramp.py:488‑506](../../../services/ingest/ingestion/handlers/ramp.py#L488-L506)) |

An unsupported entity `name`/`record_type` (not in `_ENTITY_NORMALISE`:
invoice/bill/billpayment/payment) raises `ValidationError`
([handlers/ramp.py:77‑82](../../../services/ingest/ingestion/handlers/ramp.py#L77-L82),
[475‑480](../../../services/ingest/ingestion/handlers/ramp.py#L475-L480)).

### 7.1 Handler → ObservationDraft

| Field | Entity-body draft | Thin-change draft |
|-------|-------------------|-------------------|
| `source_channel` | `ramp:transaction` | `ramp:transaction` |
| `external_id` | `ramp:{business}:txn:{Id}:{status}.{SyncToken}` | `ramp:{business}:txn:{id}:chg:{version}` |
| `occurred_at` | parse `Metadata.LastUpdatedTime`, else now | parse `lastUpdated`, else now |
| `kind` | `state_change` or `signal` (see §7.2) | always `signal` |
| `trust_tier` | **`authoritative`** | **`authoritative`** |
| `source_actor_ref` | `ramp:{role}:{ref.value}` (customer/vendor) or `None` | `None` |
| `entities_hint` | `{"type":"ramp_transaction","id":"{kind}:{Id}"}` + party | `{"type":"ramp_transaction","id":"{kind}:{id}"}` |
| `raw_payload` | the full entity | `None` |

(Builders: [handlers/ramp.py:319‑399](../../../services/ingest/ingestion/handlers/ramp.py#L319-L399),
[402‑440](../../../services/ingest/ingestion/handlers/ramp.py#L402-L440).)

**Trust tier.** `ramp:transaction` is **`authoritative`** ("Ramp is the
spend/card system of record", [handlers/ramp.py:34](../../../services/ingest/ingestion/handlers/ramp.py#L34)).
Note this channel is **not** in the static `CHANNEL_TRUST_MAP` literal
([handlers/__init__.py:41‑75](../../../services/ingest/ingestion/handlers/__init__.py#L41-L75)) —
the handler registers it at import time via
`CHANNEL_TRUST_MAP.setdefault("ramp:transaction","authoritative")`
([handlers/ramp.py:526](../../../services/ingest/ingestion/handlers/ramp.py#L526)).

### 7.2 Classification — `_classify` (kind = signal vs state_change)

([handlers/ramp.py:290‑312](../../../services/ingest/ingestion/handlers/ramp.py#L290-L312)).
The state predicate wins first; the QBO AR/AP-health rules are the fallback:

1. explicit Ramp state ∈ `{declined, disputed}` → `state_change` (status word =
   the state) ([handlers/ramp.py:58](../../../services/ingest/ingestion/handlers/ramp.py#L58),
   [300‑302](../../../services/ingest/ingestion/handlers/ramp.py#L300-L302));
2. invoice/bill with `Balance <= 0` → `state_change` "paid";
3. invoice/bill past `DueDate` with open balance → `state_change` "overdue";
4. everything else (created/updated, payments) → `signal`.

The explicit state is read from `state` / `status` / `TxnStatus`
([handlers/ramp.py:280‑287](../../../services/ingest/ingestion/handlers/ramp.py#L280-L287)).

> **TODO(human):** confirm the Ramp transaction-state vocabulary (`declined` /
> `disputed` / other terminal states) and the state **field name** + value casing.
> *(Reason: [handlers/ramp.py:56‑58](../../../services/ingest/ingestion/handlers/ramp.py#L56-L58)
> and [282](../../../services/ingest/ingestion/handlers/ramp.py#L282) — `_STATE_CHANGE_STATES`
> and `_explicit_state` are archetype guesses. The rest of `_classify` is pure
> QBO AR/AP semantics that may not map to Ramp card transactions at all.)*

The handler also extracts rich QBO-shaped fields (line items, linked txns, tax,
multi-currency, payment method, P&L dimensions) into `content` via `_entity_extras`
([handlers/ramp.py:196‑277](../../../services/ingest/ingestion/handlers/ramp.py#L196-L277)) —
again, the QBO field set, not verified Ramp fields.

> **TODO(human):** confirm the Ramp webhook payload shape — body-less notification
> vs full entity body. *(Reason: [handlers/ramp.py:402‑409](../../../services/ingest/ingestion/handlers/ramp.py#L402-L409)
> — the thin-change path exists precisely because the webhook shape is unknown;
> if Ramp ships full bodies, the entity-body dedup path is the only one needed.)*

---

## 8. Reconciliation — gap detection

`reconcile_ramp` ([reconcilers/ramp.py:124‑162](../../../services/ingest/ingestion/reconcilers/ramp.py#L124-L162))
re-checks completed (`state="done"`) entity shards for new activity. For each, it
loads the shard's stored `high_water_updated` cursor and issues **one cheap
1-row probe** ([reconcilers/ramp.py:79‑121](../../../services/ingest/ingestion/reconcilers/ramp.py#L79-L121)):

```sql
SELECT * FROM <Entity> WHERE Metadata.LastUpdatedTime > '<high_water>'
   ORDERBY ... STARTPOSITION 1 MAXRESULTS 1
```

If any row comes back, it reshares a `ramp_entity` shard at
**`recency_score=1.5`**, warm-started with `updated_cursor = high_water` (so the
re-walk runs in INCREMENTAL mode) and `parent_shard_id` recorded
([reconcilers/ramp.py:110‑121](../../../services/ingest/ingestion/reconcilers/ramp.py#L110-L121)).

- The reconciler resolves the install itself via a `set_pool_provider`-registered
  pool ([reconcilers/ramp.py:38‑52](../../../services/ingest/ingestion/reconcilers/ramp.py#L38-L52),
  [131‑143](../../../services/ingest/ingestion/reconcilers/ramp.py#L131-L143)) and
  **skips** if the install is `disabled_at`-set or absent.
- A probe failure is **best-effort**: logged and skipped, never errors the run
  ([reconcilers/ramp.py:100‑105](../../../services/ingest/ingestion/reconcilers/ramp.py#L100-L105)).
- "Pragmatic v1": it can **over-reshare but never under-reshares**; the versioned
  `external_id` makes the re-walk idempotent (changed entities → new observations,
  unchanged → dedup) ([reconcilers/ramp.py:9‑12](../../../services/ingest/ingestion/reconcilers/ramp.py#L9-L12)).

---

## 9. Revocation chokepoint — **absent**

Unlike GitHub (`_maybe_disable_on_revocation` on `401`/scoped-`404`) and Notion
(park + disable on token revocation), **Ramp has no revocation chokepoint**.

- On a `401`/`403` the client raises `RampApiError(code="ramp_api_unauthorized")`
  and stops ([client.py:178‑182](../../../services/ingest/integrations/ramp/client.py#L178-L182),
  [245‑251](../../../services/ingest/integrations/ramp/client.py#L245-L251)). It
  does **not** disable the install, set `disabled_at`, or invalidate any cache.
- The only place that clears `disabled_at` is `finalize_install` (re-enabling on
  re-install, [onboarding.py:78](../../../services/ingest/integrations/ramp/onboarding.py#L78)) —
  nothing **sets** it on the read/ingest path. (Grep for `disable`/`revok` over the
  Ramp integration returns only that one re-enable line.)
- A `401` during backfill therefore **fails the shard** (the error propagates out
  of `fetch_page_ramp`, §5.4) with **no recovery path** — there is no
  recoverable-401, no park, no re-OAuth chokepoint.

This is the flip side of the missing token refresh (§2.1): because nothing
refreshes and nothing disables, an expired access token just makes Ramp ingestion
fail until an operator re-finalizes the install with a fresh token.

> **TODO(human):** decide the Ramp revocation/expiry posture once token refresh
> lands — either a refresh-on-401 retry (preferred) or a disable-on-revocation
> chokepoint mirroring GitHub. *(Reason: this is the same "QBO seam" gap as §2.1;
> today a revoked/expired token is an unrecovered hard failure.)*

---

## 10. End-to-end summary

```
                          ┌──────────────────────── BACKFILL / POLL (pull) ─────────────────────┐
                          │  install: operator creds → ramp_installations + ramp_entities       │
   ONE BUSINESS,          │           (today: dev finance panel only — prod router not mounted)  │
   N ENTITY TYPES         │  planner: one ramp_entity shard per active entity type              │
                          │  fetcher: GET /v3/company/{biz}/query?query=SELECT * FROM <Entity>   │
                          │     FULL: ORDERBY LastUpdatedTime STARTPOSITION/MAXRESULTS (offset)  │
                          │     POLL: + WHERE LastUpdatedTime > '<high_water floor>'             │
                          │     └─► tag {_fyralis_record_type, _fyralis_business_id, entity}     │
                          └────────────────────────────────────────────────────────────────────┬┘
                                                                                                 │
                          ┌──────────────────────── LIVE (push) ─────────────────────────────┐  │
   Ramp change event ─────►  HMAC webhook ──HTTP POST──► /webhooks/ramp                       │  │
                          │     verify x-ramp-signature (HMAC-SHA256/base64, body-only, no ts)│  │
                          │     tenant ← eventNotifications[0].business_id | top-level         │  │
                          │     cutover ON → S3 + Kafka(ingestion.raw) → 202                   │  │
                          │     cutover OFF/fail → inline ingest("ramp:transaction") → 200/201 │  │
                          │     (body-less → thin-change observation; full body → entity draft)│  │
                          └───────────────────────────────────────────────────────────────────┘  │
                                                                                                 │
                                                            ┌────────────────────────────────────▼─┐
                                                            │  handle_ramp_transaction (pure fn)     │
                                                            │  branch on shape → ObservationDraft    │
                                                            │  external_id = ramp:{biz}:txn:{id}:    │
                                                            │     {status}.{SyncToken}   (entity)    │
                                                            │     chg:{version}          (thin)      │
                                                            │  trust_tier = authoritative            │
                                                            └────────────────────────────────────────┘
```

**Key invariants**

1. **One handler, mostly-one dedup namespace.** Backfill, poll, and full-body live
   webhooks all land on `ramp:transaction` via `_entity_draft` with the **same**
   versioned `external_id` (`ramp:{biz}:txn:{Id}:{status}.{SyncToken}`), so twins
   dedup. **Body-less** live notifications use a separate `…:chg:{version}` id and
   only converge once the next poll re-fetch carries the body — weaker than the
   GitHub/Slack always-converge guarantee.
2. **One credential model, no refresh.** A single per-business OAuth access token
   (Bearer) reads everything; the rotating refresh token + `token_expires_at` are
   persisted but **never exchanged** (the documented "QBO seam").
3. **One shard kind, two sync modes.** `ramp_entity` shards run FULL on first sync
   and INCREMENTAL (`LastUpdatedTime > floor`) on warm start; offset pagination via
   `STARTPOSITION`/`MAXRESULTS`; the `high_water_updated` cursor is both warm-start
   floor and reconciler reference.
4. **State-versioned external_id.** Folding `SyncToken` into the id segment makes
   each in-place mutation a distinct observation (the mutable-source dedup lesson) —
   the observations repo dedups on `(source_channel, external_id)` ignoring
   `occurred_at`.
5. **No per-source rate bucket, no revocation chokepoint.** Back-pressure is
   client-side `429 + Retry-After` only; a `401`/`403` is an unrecovered hard
   failure (no refresh, no disable, no park).
6. **Largely UNVERIFIED clone.** Host, endpoints, query-vs-REST, pagination, OAuth
   scopes, webhook signature scheme, tenant field, entity taxonomy, and
   transaction-state vocabulary are all QBO-archetype placeholders behind
   `TODO(human)` markers.

---

## 11. Configuration & compliance

> **Not yet verified against Ramp's official docs.** Every "compliant" claim below
> is **(inferred)** from the archetype and is contingent on the §-by-§
> `TODO(human)` confirmations.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `RAMP_API_BASE_URL` | `https://api.ramp.com/developer/v1` *(unverified host)* | production API base; overridable per-install via the `base_url` field ([endpoints.py:75](../../../lib/integrations/endpoints.py#L75), [132](../../../lib/integrations/endpoints.py#L132)) |
| `RAMP_BACKFILL_PAGE_SIZE` | `100` (cap 1000) | offset page size for the query endpoint ([fetchers/ramp.py:64‑68](../../../services/ingest/ingestion/fetchers/ramp.py#L64-L68)) |
| `RAMP_RL_MAX_ATTEMPTS` | `4` | `429` retry budget ([client.py:142](../../../services/ingest/integrations/ramp/client.py#L142)) |
| `RAMP_RL_MAX_SLEEP_SEC` | `30` | max sleep per `Retry-After` ([client.py:143](../../../services/ingest/integrations/ramp/client.py#L143)) |
| `SYNTHETIC_SOURCE_API_BASE` | — | when set, activates spammer mode (§11.3) ([_clients.py:50‑51](../../../services/ingest/ingestion/fetchers/_clients.py#L50-L51)) |
| `finance_panel_enabled` (setting) | — | mounts the dev `finance_router` that today is the only way to install Ramp ([route_mounts.py:105‑109](../../../services/app/gateway/route_mounts.py#L105-L109)) |

### 11.2 Verified-against-code checklist

- **Single OAuth access token, Bearer, never logged.** ✅ ([client.py:107‑141](../../../services/ingest/integrations/ramp/client.py#L107-L141))
- **Creds verified before any secret is written** (preflight + finalize re-verify). ✅ ([oauth.py:187‑196](../../../services/ingest/integrations/ramp/oauth.py#L187-L196))
- **Secrets encrypted at rest; only opaque refs in the install tables.** ✅ ([oauth.py:198‑213](../../../services/ingest/integrations/ramp/oauth.py#L198-L213))
- **Webhook HMAC over body, constant-time compare, multi-secret rotation.** ✅ — but scheme **(inferred)**, pending TODO §6.1 ([signatures/ramp.py:69‑89](../../../services/app/webhooks/signatures/ramp.py#L69-L89))
- **`external_id` versioned by state** → mutations land as new observations. ✅ ([handlers/ramp.py:337‑338](../../../services/ingest/ingestion/handlers/ramp.py#L337-L338))
- **Offset pagination + bounded `429` retry.** ✅ as coded — scheme **(inferred)**, pending TODOs §3.1/§3.2.
- **OAuth token refresh.** ❌ not implemented (TODO §2.1).
- **Production install router mounted.** ❌ not wired (TODO §2.3).
- **Revocation / expiry recovery.** ❌ absent (TODO §9).

### 11.3 Dev / spammer mode

For local testing, spammer mode is gated by the `SYNTHETIC_SOURCE_API_BASE` env
var ([_clients.py:50‑51](../../../services/ingest/ingestion/fetchers/_clients.py#L50-L51)).
`build_ramp_client` then **presets** the access token to `spam-ramp` (skipping the
secret-store resolve entirely) and points the API base at the local spammer's
`/ramp` sub-path via the endpoint resolver
([_clients.py:467‑481](../../../services/ingest/ingestion/fetchers/_clients.py#L467-L481),
[endpoints.py:162](../../../lib/integrations/endpoints.py#L162)).

A self-contained end-to-end harness lives at
[scripts/sandbox_ramp.py](../../../scripts/sandbox_ramp.py): it stands up a real
local mock of the archetype query endpoint
([synthetic/mock_servers/ramp.py](../../../services/ingest/synthetic/mock_servers/ramp.py),
which serves `/v3/company/{business}/query` and `/companyinfo/`) and drives
`RampClient → fetch_page_ramp → handle_ramp_transaction → ObservationDraft` with
QBO-shaped fixtures. The dev **finance panel** (`finance_router`,
[finance_router.py:705‑719](../../../services/app/gateway/finance_router.py#L705-L719))
seeds a synthetic install (`business_id = "biz-{tenant.hex[:12]}"`) and can emit
both backfill records and live `eventNotifications` events for the same business.
