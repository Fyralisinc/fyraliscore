# QuickBooks Ingestion — How Fyralis Pulls QuickBooks Data

This document explains, in detail, **how QuickBooks Online (QBO) data enters
Fyralis**: which Intuit Accounting APIs are called, with which token, and how the
four transactional accounting signals — **Invoice, Bill, BillPayment, and
Payment** — are each ingested.

It deliberately stops at the point where a QuickBooks change becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

---

## 1. The three ways data arrives

QuickBooks data reaches Fyralis through **three paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical PULL)** | Onboarding | Fyralis *pulls* history via the QBO **query endpoint** (`GET /v3/company/{realm}/query?query=SELECT…`) | [planners/quickbooks.py](../../../services/ingest/ingestion/planners/quickbooks.py), [fetchers/quickbooks.py](../../../services/ingest/ingestion/fetchers/quickbooks.py) |
| **Poll (incremental PULL)** | Reconciliation / scheduled re-run | Same query endpoint, warm-started with a `LastUpdatedTime >` floor | same fetcher, `ingress_kind="poll"` |
| **Live (real-time PUSH)** | Entity changes in QBO | Intuit *pushes* an `eventNotifications` **webhook** (HMAC-SHA256 `intuit-signature`) | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/quickbooks.py](../../../services/app/webhooks/signatures/quickbooks.py), [handlers/quickbooks.py](../../../services/ingest/ingestion/handlers/quickbooks.py) |

Unlike Slack/GitHub — where backfill and live carry the *same* record shape —
QuickBooks backfill and live carry **different** shapes (a full `SELECT *` entity
row vs. a thin `{name, id, operation}` change notification), but **both are
parsed by the single `quickbooks:object` handler**
([handlers/quickbooks.py:406-475](../../../services/ingest/ingestion/handlers/quickbooks.py#L406-L475)).
The channel mapping confirms all three ingress kinds collapse to one channel
([normalizer/channel_mapping.py:134-136](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L134-L136)):

```python
("quickbooks", "backfill"): "quickbooks:object",
("quickbooks", "poll"):     "quickbooks:object",
("quickbooks", "webhook"):  "quickbooks:object",
```

Both the full-body paths (backfill + poll) derive the **same versioned dedup
key**, and the thin webhook derives a *parallel* key
([idempotency/__init__.py:157-171](../../../services/ingest/ingestion/idempotency/__init__.py#L157-L171)):

```
backfill / poll (full entity, has SyncToken)
    external_id = "qbo:{realm}:{entity}:{id}:{SyncToken}"      # quickbooks_entity()
live webhook (thin change, no SyncToken)
    external_id = "qbo:{realm}:{entity}:{id}:chg:{ver}"        # quickbooks_change()
                                                               # ver = LastUpdatedTime
```

> **Verified nuance — the dedup story is not a clean one-key-per-object.** A QBO
> *entity* is **versioned by `SyncToken`** (Intuit bumps `SyncToken` on every
> field change), so `draft → sent → paid → overdue` each lands as a **new**
> observation, and a re-walk of an unchanged object collapses to one
> ([handlers/quickbooks.py:22-25, 281-284](../../../services/ingest/ingestion/handlers/quickbooks.py#L22-L25)).
> The **webhook is thin** — Intuit's `eventNotifications` carries only
> `{name, id, operation, lastUpdated}`, no `SyncToken` and no body — so a live
> delivery emits a *thin change observation* keyed `…:chg:{LastUpdatedTime}`,
> and the **next poll re-fetch fills the authoritative body** under the
> `…:{SyncToken}` key
> ([handlers/quickbooks.py:353-392](../../../services/ingest/ingestion/handlers/quickbooks.py#L353-L392)).
> So a live change and its eventual full re-fetch are **two distinct
> observations**, not one — the thin event is a low-latency "something moved"
> marker, the poll re-fetch is the system-of-record snapshot. This differs from
> the Slack/GitHub model where the live twin dedups *into* the backfilled record.

> QBO uses the **REST query endpoint** (a SQL-like `SELECT`) for history, and
> **Intuit `eventNotifications` HTTP webhooks** for real-time. There is no
> GraphQL and no streaming surface.

---

## 2. Authentication & token model

> **VERIFIED — the ground-truth hint about "Intuit OAuth refresh-token rotation"
> is only half-true for this repo.** QuickBooks Online *is* an OAuth 2.0 source
> (short-lived ~60 min access token + rotating refresh token, every call scoped
> to a company `realmId`). **But this repo deliberately does NOT implement the
> Intuit OAuth bounce** (authorize → callback → `code` exchange). The read client
> consumes a *current access token*; refresh-token rotation is **owned by an
> external `oauth_poller`**, not by any code in this layer
> ([client.py:1-19](../../../services/ingest/integrations/quickbooks/client.py#L1-L19),
> [oauth.py:1-38](../../../services/ingest/integrations/quickbooks/oauth.py#L1-L38)).

### 2.1 The credential the read client uses

`QuickBooksClient` authenticates **every** call with
`Authorization: Bearer {access_token}` and scopes the path to `{realmId}`
([client.py:118-128](../../../services/ingest/integrations/quickbooks/client.py#L118-L128)).
The access token is resolved **once** — either preset (Provider Lab/test) or read from
the secret store via `secret_ref` — and reused for the life of the client
([client.py:94-116](../../../services/ingest/integrations/quickbooks/client.py#L94-L116)).
There is **no in-client refresh**: a `401`/`403` maps to a
`quickbooks_api_unauthorized` `QuickBooksApiError` whose message says "the token
may be expired — refresh it" but does not itself refresh
([client.py:228-238](../../../services/ingest/integrations/quickbooks/client.py#L228-L238)).

### 2.2 Where credentials live

| Credential | Secret-store label | DB column | Notes |
|-----------|--------------------|-----------|-------|
| Access token | `quickbooks_access_token:{realm}` | `quickbooks_installations.secret_ref` | what the read client consumes |
| Refresh token | `quickbooks_refresh_token:{realm}` | `quickbooks_installations.refresh_secret_ref` | rotated by the external `oauth_poller` (inferred — no rotation code in this layer) |
| Webhook verifier token | `quickbooks_webhook_verifier:{realm}` | `quickbooks_installations.webhook_secret_ref` **and** `provider_installations.secret_ref` | HMAC key for inbound webhook verification |

Only **opaque refs** reach the DB; the tokens themselves are encrypted-at-rest in
the secret store ([oauth.py:192-207](../../../services/ingest/integrations/quickbooks/oauth.py#L192-L207)).
The access token / auth header are **never logged**
([client.py:18](../../../services/ingest/integrations/quickbooks/client.py#L18)).

### 2.3 The install flow — operator-mediated credential submission

Because there is no OAuth bounce, the **production install surface is the operator
pasting the creds they obtained from their own Intuit OAuth app**, which the
router verifies against the **real** QBO API before seeding anything
([oauth.py:1-38](../../../services/ingest/integrations/quickbooks/oauth.py#L1-L38)).
Two routes under `prefix="/integrations/quickbooks"`
([oauth.py:68](../../../services/ingest/integrations/quickbooks/oauth.py#L68)):

1. **`POST /integrations/quickbooks/connect/preflight`** (Bearer-authed) — takes
   `{realm_id, access_token, base_url?}`, calls `company_info()` to verify the
   token + realm, and returns the company name + default entity list. **No secret
   is written** ([oauth.py:130-155](../../../services/ingest/integrations/quickbooks/oauth.py#L130-L155)).
2. **`POST /integrations/quickbooks/connect/finalize`** (Bearer-authed) — takes
   `{realm_id, access_token, refresh_token?, base_url?, entities?,
   token_expires_at?, webhook_verifier_token?}`. It **re-verifies creds before
   any write** (an invalid token leaves no rows behind), persists the tokens to
   the secret store, then calls `finalize_install()`
   ([oauth.py:158-244](../../../services/ingest/integrations/quickbooks/oauth.py#L158-L244)).

`finalize_install()` does it all in **one tenant-scoped transaction**
([onboarding.py:37-115](../../../services/ingest/integrations/quickbooks/onboarding.py#L37-L115)):

- UPSERT `quickbooks_installations` keyed on `(tenant_id, realm_id)` (a re-finalize
  clears `disabled_at`),
- INSERT one `quickbooks_entities` row per entity type (`state='active'`),
- emit an `onboarding_triggers` row (`source='quickbooks'`, `trigger_kind='install'`)
  so the existing backfill chain fires.

Only when a `webhook_verifier_token` is supplied does
`register_webhook_installation()` seed the live edge — a `provider_installations`
row keyed `(provider='quickbooks', installation_id=realm_id)` whose `secret_ref`
is the verifier token ([oauth.py:221-230](../../../services/ingest/integrations/quickbooks/oauth.py#L221-L230),
[onboarding.py:118-140](../../../services/ingest/integrations/quickbooks/onboarding.py#L118-L140)).
**No webhook verifier ⇒ no live path** — backfill/poll still work.

> **TODO(human):** the docstrings call this the install surface "the audit flagged
> as missing" (previously reachable only through a dev `finance_router` panel).
> Confirm whether `connect/preflight` + `connect/finalize` are the *production*
> entry points end-to-end, or whether an `oauth_poller`-driven first-mint path
> also exists upstream. The *why* of "no OAuth bounce here" is stated in the
> docstring but the upstream token-origin story is not in this layer's code.

---

## 3. The QBO API surface actually called

All read calls funnel through `QuickBooksClient._request`
([client.py:118-169](../../../services/ingest/integrations/quickbooks/client.py#L118-L169)),
which:

- sets `Authorization: Bearer {token}` + `Accept: application/json`,
- honours `429` `Retry-After` within a bounded budget
  (`QUICKBOOKS_RL_MAX_ATTEMPTS`=4, `QUICKBOOKS_RL_MAX_SLEEP_SEC`=30),
- maps transport errors and any non-2xx to a coded `QuickBooksApiError`
  ([client.py:228-259](../../../services/ingest/integrations/quickbooks/client.py#L228-L259)).

The endpoints invoked for ingestion:

| QBO endpoint | Wrapper | Purpose | Code |
|--------------|---------|---------|------|
| `GET /v3/company/{realm}/query?query={SQL}&minorversion=75` | `QuickBooksClient.query()` | run a `SELECT * FROM {Entity}` page | [client.py:175-207](../../../services/ingest/integrations/quickbooks/client.py#L175-L207) |
| `GET /v3/company/{realm}/companyinfo/{realm}?minorversion=75` | `QuickBooksClient.company_info()` | connectivity / credential probe (preflight + reconciler open) | [client.py:209-214](../../../services/ingest/integrations/quickbooks/client.py#L209-L214) |

The `minorversion` pin is **`75`** on every call
([client.py:39](../../../services/ingest/integrations/quickbooks/client.py#L39)).
The query language is QBO's SQL-like dialect:
`SELECT * FROM {Entity} [WHERE …] ORDERBY {field} STARTPOSITION n MAXRESULTS m`,
default `ORDERBY Metadata.LastUpdatedTime`
([client.py:191-194](../../../services/ingest/integrations/quickbooks/client.py#L191-L194)).

### 3.1 Pagination — offset based (`STARTPOSITION` / `MAXRESULTS`)

QBO has **no cursor**. Each page requests `STARTPOSITION n MAXRESULTS m`; the
client computes `next_start = start_position + len(rows)` and treats a short page
(`maxResults < requested` or zero rows) as **terminal** (`next_start_position is
None`) ([client.py:203-207](../../../services/ingest/integrations/quickbooks/client.py#L203-L207)).
`query()` returns **one page** plus the next start position; the fetcher persists
that offset in the shard cursor and resumes next invocation. Default page size is
**100**, overridable up to **1000** via `QUICKBOOKS_BACKFILL_PAGE_SIZE`
([fetchers/quickbooks.py:49-53](../../../services/ingest/ingestion/fetchers/quickbooks.py#L49-L53)).

### 3.2 Rate limits

> **VERIFIED — there is NO dedicated QuickBooks token bucket.** `BUCKET_DEFAULTS`
> declares buckets only for slack / github / gmail / discord
> ([rate_limit/buckets.py:79-91](../../../services/ingest/ingestion/rate_limit/buckets.py#L79-L91));
> there is **no `("quickbooks", …)` entry**. QBO throttling is handled **purely
> in the client** via the `429` + `Retry-After` bounded retry (§3 above). QBO's
> documented limits are ~10 req/s and 120/min batch per realm
> ([client.py:15-16](../../../services/ingest/integrations/quickbooks/client.py#L15-L16)).

---

## 4. Backfill scope — the shard families

The planner decomposes one install into **one shard per active entity type**, all
of `shard_kind = "quickbooks_entity"`
([planners/quickbooks.py:47-76](../../../services/ingest/ingestion/planners/quickbooks.py#L47-L76)).
There is **no per-window or per-channel fan-out** (contrast Slack's per-channel
shards) — the realm + entity type fully identify a stream.

`ctx.source_client` is **None**; the active entity list is read from DB state
(`quickbooks_entities`, JSON-aggregated into `ctx.install["entities"]` by the
SourceOnboarding loader) so the planner stays stateless
([planners/quickbooks.py:1-13, 31-51](../../../services/ingest/ingestion/planners/quickbooks.py#L1-L13)).

The default entity set is the **four transactional entities**, in declared order
([client.py:262-264](../../../services/ingest/integrations/quickbooks/client.py#L262-L264)):

```python
DEFAULT_ENTITIES = ("Invoice", "Bill", "BillPayment", "Payment")
```

Each shard carries `entity_type`, `realm_id`, `installation_id`, and the
warm-start `updated_cursor` (the high-water `LastUpdatedTime`, `None` on first
sync), at a baseline `recency_score=1.0`
([planners/quickbooks.py:58-70](../../../services/ingest/ingestion/planners/quickbooks.py#L58-L70)).

---

## 5. The fetcher — one shard kind, two sync modes

`fetch_page_quickbooks` takes one `(install, shard_identifier, cursor)` triple and
returns one page of records + the next cursor; ShardFetch loops, persisting the
cursor between calls
([fetchers/quickbooks.py:113-182](../../../services/ingest/ingestion/fetchers/quickbooks.py#L113-L182)).

### 5.1 Cursor

```python
class QuickBooksCursor(BaseModel):
    start_position: int = 1          # QBO STARTPOSITION offset (1-based)
    high_water_updated: str | None   # max Metadata.LastUpdatedTime seen — warm-start
                                      #   floor AND the reconciler's gap reference
    incremental_floor: str | None    # the `LastUpdatedTime >` lower bound frozen
                                      #   for this run (None in FULL mode)
    rows_seen: int = 0               # diagnostic
    seeded: bool = False             # whether first-call setup ran
```

([fetchers/quickbooks.py:56-76](../../../services/ingest/ingestion/fetchers/quickbooks.py#L56-L76)).

### 5.2 FULL vs INCREMENTAL

On first call (`seeded=False`), if the shard carries a warm-start `updated_cursor`,
both `incremental_floor` and `high_water_updated` are set from it; otherwise the
run is FULL ([fetchers/quickbooks.py:124-129](../../../services/ingest/ingestion/fetchers/quickbooks.py#L124-L129)):

- **FULL** (initial backfill): `where = None` → `SELECT * FROM {Entity} ORDERBY
  Metadata.LastUpdatedTime STARTPOSITION n MAXRESULTS m`, offset-paginated.
- **INCREMENTAL** (poll / gap reshare): `where = "Metadata.LastUpdatedTime >
  '{incremental_floor}'"` so only changed entities come back
  ([fetchers/quickbooks.py:131-135](../../../services/ingest/ingestion/fetchers/quickbooks.py#L131-L135)).

For each row the fetcher reads `Metadata.LastUpdatedTime` (tolerating both
`MetaData` and `Metadata` casings) and advances `high_water_updated` to the max
seen ([fetchers/quickbooks.py:94-107, 164](../../../services/ingest/ingestion/fetchers/quickbooks.py#L94-L107)).

### 5.3 Records emitted

Each entity row is wrapped in a fetcher-tagged record — there is no "reshape into
the webhook shape" step (the live and backfill shapes genuinely differ; §1)
([fetchers/quickbooks.py:157-163](../../../services/ingest/ingestion/fetchers/quickbooks.py#L157-L163)):

```python
{
    "_fyralis_record_type": entity_type.lower(),   # invoice | bill | billpayment | payment
    "_fyralis_realm_id": realm_id,
    "entity": row,                                  # the full SELECT * body
}
```

### 5.4 Rate-limit graceful degrade

If `query()` raises `quickbooks_api_rate_limited` (the client's retry budget
already exhausted), the fetcher returns an **empty page with `end_of_data=False`**
and the unchanged cursor, so ShardFetch re-tries the same offset later rather than
failing the run ([fetchers/quickbooks.py:146-155](../../../services/ingest/ingestion/fetchers/quickbooks.py#L146-L155)).

---

## 6. The handler — shaping records into `ObservationDraft`

`handle_quickbooks_object` is a **pure function** (no DB / network) registered on
the single channel `quickbooks:object` via `@register(_CHANNEL)`
([handlers/quickbooks.py:43, 406-409](../../../services/ingest/ingestion/handlers/quickbooks.py#L43-L43)).
It branches on the input shape and produces **exactly one** observation per call.
The trust posture is **`authoritative`** — QuickBooks is the accounting
system-of-record ([handlers/quickbooks.py:44, 478](../../../services/ingest/ingestion/handlers/quickbooks.py#L44-L44)):
the handler stamps `_TRUST` directly and registers it in `CHANNEL_TRUST_MAP` via
`setdefault` ([handlers/quickbooks.py:478](../../../services/ingest/ingestion/handlers/quickbooks.py#L478-L478)).

The three input branches ([handlers/quickbooks.py:413-475](../../../services/ingest/ingestion/handlers/quickbooks.py#L413-L475)):

1. **Intuit `eventNotifications`** array → first entity's `{name, operation,
   lastUpdated}` → `_thin_change_draft` (no body).
2. **Flattened harness webhook** (`{name, id, …, entity?}`) → full body if `entity`
   present → `_entity_draft`; else thin.
3. **Backfill/poll tagged record** (`_fyralis_record_type`) → `_entity_draft`.

### 6.1 The branch → `ObservationDraft` table

| Input branch | Builder | `external_id` | `occurred_at` | `kind` | Trust |
|--------------|---------|---------------|---------------|--------|-------|
| Invoice/Bill **full** (backfill/poll/`entity` body) | `_entity_draft` | `qbo:{realm}:{kind}:{id}:{SyncToken}` | `Metadata.LastUpdatedTime` or now | `state_change` if **paid** (Balance≤0) or **overdue** (DueDate past + Balance>0); else `signal` | authoritative |
| Payment/BillPayment **full** | `_entity_draft` | `qbo:{realm}:{kind}:{id}:{SyncToken}` | `Metadata.LastUpdatedTime` or now | `signal` (cash event) | authoritative |
| Live thin change (any entity) | `_thin_change_draft` | `qbo:{realm}:{kind}:{id}:chg:{LastUpdatedTime}` | `lastUpdated` or now | `signal` | authoritative |

The entity `name`/`record_type` is normalised through `_ENTITY_NORMALISE`
(`billpayment → bill_payment`); an unknown entity raises `ValidationError`
([handlers/quickbooks.py:47-52, 427-432](../../../services/ingest/ingestion/handlers/quickbooks.py#L47-L52)).

### 6.2 Signal classification

`_classify` is the reasoning value ([handlers/quickbooks.py:250-266](../../../services/ingest/ingestion/handlers/quickbooks.py#L250-L266)):

- invoice/bill with **`Balance <= 0`** → `state_change`, status `paid` (AR
  collected / AP cleared),
- invoice/bill **past `DueDate` with open balance** → `state_change`, status
  `overdue` (cash-risk),
- everything else (open invoices/bills, all payments) → `signal`.

### 6.3 Content richness

`_entity_draft` builds a one-line `content_text` like
`"Invoice #1037 · Acme Corp · $4,200.00 · paid (bal $0.00) · 3 lines"` and a rich
`content` dict carrying status, totals, balance, currency, dates, party — plus
`_entity_extras`: flattened **line items**, **LinkedTxn** AR/AP graph edges, tax,
multi-currency normalization, payment channel, and P&L segmentation dimensions
([handlers/quickbooks.py:137-247, 291-337](../../../services/ingest/ingestion/handlers/quickbooks.py#L137-L247)).
`source_actor_ref` is `qbo:{customer|vendor}:{ref_id}` from the doc's
`CustomerRef`/`VendorRef`, or `None`
([handlers/quickbooks.py:101-115](../../../services/ingest/ingestion/handlers/quickbooks.py#L101-L115)).
`entities_hint` always includes `{"type":"quickbooks_object","id":"{kind}:{id}"}`.

---

## 7. Live (real-time) ingestion via Intuit webhooks

When entities change in QBO, Intuit **POSTs an `eventNotifications`** delivery to
Fyralis's webhook edge. The router maps provider `quickbooks` → channel
`quickbooks:object` ([webhooks/router.py:449](../../../services/app/webhooks/router.py#L449)).

### 7.1 Signature verification (HMAC-SHA256, **base64**, no timestamp)

`QuickBooksVerifier` verifies the raw body against the `intuit-signature` header:
`base64(HMAC-SHA256(verifier_token, raw_body))`, constant-time compared, trying
each active verifier secret in turn
([webhooks/signatures/quickbooks.py:37-73](../../../services/app/webhooks/signatures/quickbooks.py#L37-L73)).
It is registered in the verifier registry as `"quickbooks": quickbooks.verifier`
([webhooks/signatures/__init__.py:53](../../../services/app/webhooks/signatures/__init__.py#L53-L53)).

> **The one scheme difference:** Intuit's digest is **base64**, not the hex
> `sha256=…` of GitHub/Jira ([webhooks/signatures/quickbooks.py:1-7, 56](../../../services/app/webhooks/signatures/quickbooks.py#L1-L7)).
> Like GitHub, the digest is over the **body alone — no timestamp envelope, no
> replay window** ([webhooks/signatures/quickbooks.py:11-13](../../../services/app/webhooks/signatures/quickbooks.py#L11-L13),
> `signed_timestamp=None` at [:72](../../../services/app/webhooks/signatures/quickbooks.py#L68-L73)).
> Idempotency is enforced at the ingestion layer via the versioned `external_id`,
> not here.

> **No QuickBooks-specific replay cache.** Unlike GitHub (which keeps an
> `(installation, delivery)` replay cache), there is **no** equivalent for
> QuickBooks in this layer (inferred — none found in the integration package or
> router). The thin-change `…:chg:{LastUpdatedTime}` key is the only dedup backstop
> for repeated deliveries of the same change.

### 7.2 Tenant resolution

`_extract_quickbooks` reads the first notification's `realmId` (falling back to a
top-level `realmId` the synthetic harness sends), which is the
`provider_installations.installation_id`
([webhooks/tenant_resolver.py:331-345, 530](../../../services/app/webhooks/tenant_resolver.py#L331-L345)).
QuickBooks is in the resolver's known-provider set
([webhooks/tenant_resolver.py:78](../../../services/app/webhooks/tenant_resolver.py#L78-L78)).

### 7.3 Kafka cutover vs inline

QuickBooks participates in the data-plane cutover. It is in both
`_PROVIDER_TO_SHADOW_SOURCE` and `_CUTOVER_ENABLED_PROVIDERS`
([webhooks/router.py:131, 170](../../../services/app/webhooks/router.py#L131-L131)):

- **Cutover ON** (tenant `ingestion.kafka_path_enabled=TRUE`): skip the inline
  `ingest()`, publish the verified body to Kafka, return **202** (the QBO webhook
  fits the 202 contract — no synchronous-response-shape constraint like Discord).
- **Cutover OFF**: inline-ingest fallback onto the `quickbooks:object` channel
  ([webhooks/router.py:449](../../../services/app/webhooks/router.py#L449)).

> **No lifecycle-event branch.** GitHub routes `installation.*` deliveries to a
> lifecycle handler; QuickBooks has **no equivalent** here — all verified
> deliveries are entity-change notifications headed for the handler (inferred — no
> QuickBooks lifecycle dispatch found in the router).

---

## 8. Reconciliation — gap detection

`reconcile_quickbooks` re-checks **completed** (`state='done'`) entity shards for
new activity ([reconcilers/quickbooks.py:124-162](../../../services/ingest/ingestion/reconcilers/quickbooks.py#L124-L162)).
It loads the tenant's single non-disabled `quickbooks_installations` row, opens one
client, and for each done shard runs a **cheap 1-row probe**
([reconcilers/quickbooks.py:79-121](../../../services/ingest/ingestion/reconcilers/quickbooks.py#L79-L121)):

```sql
SELECT * FROM {Entity}
 WHERE Metadata.LastUpdatedTime > '{high_water}'
 STARTPOSITION 1 MAXRESULTS 1
```

The `high_water` comes from the completed shard's persisted cursor
(`cursor.high_water_updated`, loaded from `shard_fetch` state)
([reconcilers/quickbooks.py:68-76](../../../services/ingest/ingestion/reconcilers/quickbooks.py#L68-L76)).
If **any** row comes back, the reconciler reshares the entity-type shard at
`recency_score=1.5`, warm-started with `updated_cursor=high_water` (so the re-walk
runs INCREMENTAL), tagging `parent_shard_id` + `gap_baseline_updated`
([reconcilers/quickbooks.py:110-121](../../../services/ingest/ingestion/reconcilers/quickbooks.py#L110-L121)).

> **Pragmatic v1:** the probe can **over-reshare but never under-reshares**;
> `SyncToken` dedup makes re-walks idempotent
> ([reconcilers/quickbooks.py:10-12](../../../services/ingest/ingestion/reconcilers/quickbooks.py#L10-L12)).
> A probe failure is logged and treated as "no gap" (best-effort) rather than
> erroring ([reconcilers/quickbooks.py:100-105](../../../services/ingest/ingestion/reconcilers/quickbooks.py#L100-L105)).

---

## 9. Revocation chokepoint / recoverable-error behavior

> **VERIFIED — QuickBooks has NO revocation chokepoint in this layer, unlike
> GitHub.** GitHub's outbound client auto-disables an installation on documented
> `401`/`404` revocation signals via `_maybe_disable_on_revocation`. The
> QuickBooks client does **no such thing**: a `401`/`403` simply maps to a
> `quickbooks_api_unauthorized` `QuickBooksApiError`
> ([client.py:165-169, 228-238](../../../services/ingest/integrations/quickbooks/client.py#L165-L169)),
> and the only `disabled_at` write in the whole integration package is the
> `disabled_at = NULL` **re-enable** in `finalize_install`'s UPSERT
> ([onboarding.py:76](../../../services/ingest/integrations/quickbooks/onboarding.py#L76-L76)).
> There is **no automatic disable-on-auth-failure path** — recovery is to refresh
> the token (externally, via the `oauth_poller`) and re-`finalize`. The
> rate-limited fetcher degrade (§5.4) is the only recoverable-error softening.

> **TODO(human):** confirm where token-expiry-induced backfill failures are
> surfaced/alerted, given the read client can't refresh and there's no auto-disable.
> The intended operator recovery loop (oauth_poller refresh vs. manual re-finalize)
> is not expressed in this layer's code.

---

## 10. End-to-end summary

```
                          ┌──────────────────── BACKFILL / POLL (pull) ────────────────────┐
                          │  operator pastes realm_id + access_token (+ refresh, verifier) │
                          │     → /connect/preflight + /connect/finalize (verify-then-write)│
   FOUR ENTITY TYPES      │  finalize_install: quickbooks_installations + _entities         │
   Invoice Bill           │     + onboarding_triggers → planner                             │
   BillPayment Payment    │  planner: one quickbooks_entity shard per active entity type    │
                          │  fetcher: GET /v3/company/{realm}/query                          │
                          │     SELECT * FROM {Entity}                                       │
                          │       [WHERE Metadata.LastUpdatedTime > '{floor}']  (poll)       │
                          │       ORDERBY Metadata.LastUpdatedTime STARTPOSITION n MAXRES m  │
                          │     → tag {_fyralis_record_type, _fyralis_realm_id, entity}      │
                          └──────────────────────────────────────────────────────────────┬─┘
                                                                                           │
                          ┌──────────────────────── LIVE (push) ─────────────────────────┐│
   entity changes ────────►  Intuit eventNotifications ──HTTP POST──► /webhooks/quickbooks││
                          │     verify intuit-signature = base64(HMAC-SHA256(body)), no ts ││
                          │     resolve tenant by realmId → kafka cutover (202) OR inline  ││
                          │     thin {name,id,operation,lastUpdated} — NO body, NO SyncTok ││
                          └──────────────────────────────────────────────────────────────┘│
                                                                                           │
                                                           ┌───────────────────────────────▼─┐
                                                           │  handle_quickbooks_object        │
                                                           │  branch on shape:                │
                                                           │   full → _entity_draft           │
                                                           │     external_id qbo:{r}:{k}:{id}: │
                                                           │       {SyncToken}                 │
                                                           │   thin → _thin_change_draft       │
                                                           │     external_id …:chg:{LastUpd}   │
                                                           │  trust = authoritative           │
                                                           │  → ObservationDraft               │
                                                           └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one channel — but two key families.** All of backfill, poll, and
   webhook land on `quickbooks:object`. Full bodies dedup on
   `qbo:{realm}:{kind}:{id}:{SyncToken}`; thin webhook changes use a parallel
   `…:chg:{LastUpdatedTime}` key. A live change and its poll re-fetch are
   **distinct** observations (thin marker + system-of-record snapshot), not one.
2. **`SyncToken`-versioned dedup.** Because invoices/bills mutate
   (draft→sent→paid→overdue), each real change re-observes; an unchanged re-walk
   collapses. This is the mutable-source dedup lesson.
3. **One credential model, no in-client refresh.** A realm-scoped OAuth **access
   token** reads everything; refresh-token rotation is owned by an external
   `oauth_poller`, not this layer. There is no OAuth bounce — install is
   operator-mediated, verify-then-write.
4. **Offset pagination, no cursor.** `STARTPOSITION`/`MAXRESULTS`; a short page is
   terminal. `Metadata.LastUpdatedTime` is the incremental high-water for both poll
   and the reconciler.
5. **No replay window, no revocation chokepoint, no dedicated rate bucket.** The
   signature is body-only (idempotency via `external_id`); auth failures map to an
   error rather than auto-disabling; throttling is the client's `429`+`Retry-After`
   retry alone.

---

## 11. Configuration & compliance

Verified against the code (and cross-checked with the integration docstrings,
which cite Intuit's query/webhook/rate-limit behavior).

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `QUICKBOOKS_BACKFILL_PAGE_SIZE` | `100` (capped at `1000`) | `MAXRESULTS` per query page ([fetchers/quickbooks.py:49-53](../../../services/ingest/ingestion/fetchers/quickbooks.py#L49-L53)) |
| `QUICKBOOKS_RL_MAX_ATTEMPTS` | `4` | `429` retry budget in the client ([client.py:129](../../../services/ingest/integrations/quickbooks/client.py#L129-L129)) |
| `QUICKBOOKS_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:130](../../../services/ingest/integrations/quickbooks/client.py#L130-L130)) |
| `QUICKBOOKS_API_BASE_URL` | `https://quickbooks.api.intuit.com` | endpoint-resolver override (production host) ([lib/integrations/endpoints.py:62, 129](../../../lib/integrations/endpoints.py#L62-L62)) |
| `PROVIDER_LAB_URL` | — | test-only credential mode (§11.3); routing remains explicit |

> Note: `base_url` (production vs. Intuit `https://sandbox-quickbooks.api.intuit.com`)
> is **per-install** (`quickbooks_installations.base_url`), not an env var; the
> operator passes it at finalize time ([oauth.py:63-65](../../../services/ingest/integrations/quickbooks/oauth.py#L63-L65)).

### 11.2 Verified compliant

- **Auth** — OAuth 2.0 Bearer access token, realm-scoped path, `minorversion=75`
  pinned on every call; verify-then-write install (no secret on bad token). ✅
- **Webhook signing** — HMAC-SHA256 over raw body, **base64** in `intuit-signature`,
  constant-time compare, multi-secret rotation. ✅
- **Pagination** — offset `STARTPOSITION`/`MAXRESULTS`, short-page terminal. ✅
- **Incremental** — `WHERE Metadata.LastUpdatedTime > '{floor}'` high-water, used by
  both poll and the reconciler probe. ✅
- **Rate-limit etiquette** — `429 Retry-After` honoured within a bounded budget
  (no dedicated bucket — client-only). ✅
- **Least secret surface** — access/refresh/verifier tokens encrypted-at-rest; only
  opaque refs in the DB; tokens never logged. ✅

### 11.3 Dev / Provider Lab mode

For local testing, `build_quickbooks_client` detects `PROVIDER_LAB_URL`,
**presets** the access token to `spam-quickbooks`, and skips the secret store.
`QUICKBOOKS_API_BASE_URL=<lab>/quickbooks` supplies the route explicitly; the
production resolver never derives it from the lab origin.

> **TODO(human):** the ground-truth hint expected an exact backfill row count
> parity check (as Notion has "4200 exact"). No such fixture-count assertion was
> found in the QuickBooks code path; confirm the expected Provider Lab entity volumes
> if a parity SLO exists.
