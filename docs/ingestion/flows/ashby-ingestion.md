# Ashby Ingestion — How Fyralis Pulls Ashby Data

This document explains, in detail, **how Ashby data enters Fyralis**: which Ashby
RPC endpoints are called, with which credential, and how the Ashby recruiting
signal set — **candidates, applications, jobs, interviews, and offers** — is each
ingested.

Ashby is a recruiting **applicant-tracking system (ATS)**. It is built on the
*Gusto entity-model archetype* (one shard per entity type) but swaps the auth
model to a long-lived **API key** presented as HTTP Basic, and adds an
HMAC-signed webhook for live changes
([integrations/ashby/__init__.py:1‑23](../../../services/ingest/integrations/ashby/__init__.py#L1-L23)).

It deliberately stops at the point where an Ashby change becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

---

## 1. The ways data arrives

Ashby data reaches Fyralis through **three paths that converge on one handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding | Fyralis *pulls* history via the Ashby RPC API (`POST /<Category>.list`, cursor-paginated) | [planners/ashby.py](../../../services/ingest/ingestion/planners/ashby.py), [fetchers/ashby.py](../../../services/ingest/ingestion/fetchers/ashby.py) |
| **Poll (incremental)** | Reconciler / re-run | The *same* fetcher re-runs warm-started with the persisted Ashby `syncToken`, returning only entities changed since it was minted | [fetchers/ashby.py:8‑21](../../../services/ingest/ingestion/fetchers/ashby.py#L8-L21), [reconcilers/ashby.py](../../../services/ingest/ingestion/reconcilers/ashby.py) |
| **Live (real-time)** | A change in Ashby | Ashby *pushes* an HMAC-signed **webhook** to Fyralis | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/ashby.py](../../../services/app/webhooks/signatures/ashby.py), [handlers/ashby.py](../../../services/ingest/ingestion/handlers/ashby.py) |

All three paths route to the **single** `ashby:object` channel and are parsed by
the **single** `handle_ashby_object` handler
([handlers/ashby.py:329‑373](../../../services/ingest/ingestion/handlers/ashby.py#L329-L373)).
The normalizer maps all three `(source, ingress_kind)` pairs onto that one
channel
([normalizer/channel_mapping.py:304‑306](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L304-L306)):

```python
("ashby", "backfill"): "ashby:object",
("ashby", "poll"):     "ashby:object",
("ashby", "webhook"):  "ashby:object",
```

All three derive the **same** dedup key. Per the cross-agent CONTRACT the
`external_id` is **NOT** version-suffixed
([handlers/ashby.py:65‑77](../../../services/ingest/ingestion/handlers/ashby.py#L65-L77)):

```
external_id = "ashby:{org}:{entity_kind}:{id}"
```

So an offer that is backfilled, then re-walked by an incremental poll, then
delivered live as a webhook collapses into **one** observation. The
`entity_kind` discriminator means two recruiting entities that happen to share an
id (e.g. a `candidate` and an `application`) never collide. This is the central
design invariant of Ashby ingestion — backfill, poll, and webhook all feed one
handler and one dedup namespace.

Ashby entities **mutate** (a candidate advances stages, an offer is
sent/accepted). Because the `external_id` is not version-suffixed, the handler
relies on `occurred_at` (the entity's `updatedAt`) to represent the latest
state; a re-walk of an *unchanged* entity dedups, while a state change
re-observes via a newer `occurred_at`
([fetchers/ashby.py:24‑31](../../../services/ingest/ingestion/fetchers/ashby.py#L24-L31)).

> Ashby uses an **RPC-style** API — every read is an HTTP `POST` to
> `/<Category>.list` or `/<Category>.info`. There is no GraphQL and no REST-list
> path here. Real-time is HTTP **webhooks**; history is the **RPC API**.

---

## 2. Authentication — API key as HTTP Basic (no OAuth, no refresh)

Ashby ingestion uses a **single credential model: a long-lived API key**, the
*Brex/Jira archetype*. There are no per-user OAuth tokens and **no refresh
token** ([client.py:1‑26](../../../services/ingest/integrations/ashby/client.py#L1-L26)).

### 2.1 The Basic-auth scheme

The API key is presented as the HTTP Basic **username** with an **empty
password** — i.e. `Authorization: Basic <base64("KEY:")>` (note the trailing
colon, an empty password). This is the **CONFIRMED** Ashby posture per its
first-party API docs — the same shape as a Jira api-token, except Ashby uses an
empty password rather than `email:token`
([client.py:59‑63](../../../services/ingest/integrations/ashby/client.py#L59-L63)):

```python
def _basic_auth_value(api_key: str) -> str:
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return f"Basic {token}"
```

The key is resolved **once** from the secret store (or preset in Provider Lab mode)
and reused for the life of the client, guarded by an `asyncio.Lock`; there is no
token refresh ([client.py:113‑138](../../../services/ingest/integrations/ashby/client.py#L113-L138)).
The API key and the `Authorization` header are **never logged**
([client.py:25‑26](../../../services/ingest/integrations/ashby/client.py#L25-L26)).

### 2.2 Where credentials live

Ashby is **organization-scoped**: every API call is scoped to an `org_id`. The
install + secrets are stored as follows
([db/migrations/0106_ashby.sql:52‑69](../../../db/migrations/0106_ashby.sql#L52-L69)):

| Credential | Where | Notes |
|-----------|-------|-------|
| API key | `encrypted_secrets`, pointed to by `ashby_installations.secret_ref` | Basic username; empty password. **No** refresh token column. |
| Webhook HMAC secret | `encrypted_secrets`, pointed to by `ashby_installations.webhook_secret_ref` **and** `provider_installations.secret_ref` | App/install-level HMAC signing secret. |
| Org id | `ashby_installations.org_id` (and `provider_installations.installation_id = org_id`) | the scope id and the webhook tenant-resolution key. |
| API host | `ashby_installations.base_url` | canonical `https://api.ashbyhq.com` ([lib/integrations/endpoints.py:102‑105](../../../lib/integrations/endpoints.py#L102-L105)). |

The migration explicitly **omits** the `refresh_secret_ref` / `token_expires_at`
columns the OAuth sources carry, and **keeps** `webhook_secret_ref` (unlike
poll-only Carta) ([db/migrations/0106_ashby.sql:13‑17](../../../db/migrations/0106_ashby.sql#L13-L17),
[60‑65](../../../db/migrations/0106_ashby.sql#L60-L65)).

### 2.3 How an install gets registered

There is **no OAuth handshake** for Ashby — there is no `oauth.py` in
`services/ingest/integrations/ashby/` (contrast Slack/GitHub). Provisioning is a
direct DB seed in `onboarding.py`, in **two** independent registrations seeded
together ([onboarding.py:1‑21](../../../services/ingest/integrations/ashby/onboarding.py#L1-L21)):

1. **`finalize_install`** — in one tenant-scoped transaction it UPSERTs an
   `ashby_installations` row (keyed `(tenant_id, org_id)`), INSERTs one
   `ashby_entities` row per entity type to shard, and emits an
   `onboarding_triggers` row (`source='ashby'`) so the existing M6 backfill chain
   fires ([onboarding.py:39‑115](../../../services/ingest/integrations/ashby/onboarding.py#L39-L115)).
2. **`register_webhook_installation`** — UPSERTs the LIVE-path row in
   `provider_installations` (`provider='ashby'`, `installation_id=org_id`,
   `secret_ref=<webhook HMAC secret>`, `enabled=TRUE`) so the webhook edge can
   resolve the tenant and load the signing secret
   ([onboarding.py:118‑147](../../../services/ingest/integrations/ashby/onboarding.py#L118-L147)).

Backfill reads `ashby_installations`; live reads `provider_installations`. The
two are seeded together but stay independent
([onboarding.py:13‑18](../../../services/ingest/integrations/ashby/onboarding.py#L13-L18)).

> **TODO(human)** *(reproduced from the code, not fabricated).* In production
> Ashby webhook tenant-resolution is by the **receiving endpoint/secret**, NOT a
> body field: each per-tenant webhook is configured with a distinct URL +
> signing secret in Ashby's admin, so the secret that verifies is what binds the
> delivery to a tenant. The body `organizationId` used by `_extract_ashby` is the
> synthetic-gate stand-in; wire the real endpoint/secret-scoped resolution when
> entitled
> ([onboarding.py:129‑134](../../../services/ingest/integrations/ashby/onboarding.py#L129-L134),
> [tenant_resolver.py:503‑507](../../../services/app/webhooks/tenant_resolver.py#L503-L507)).

---

## 3. The Ashby API surface that is actually called

All reads funnel through `AshbyClient._rpc`
([client.py:140‑213](../../../services/ingest/integrations/ashby/client.py#L140-L213)),
which:

- POSTs to `{base}/<method_path>` with `Authorization: Basic …`,
  `Accept: application/json`, `Content-Type: application/json`,
- honours **`429` + `Retry-After`** within a bounded budget
  (`ASHBY_RL_MAX_ATTEMPTS`=4, `ASHBY_RL_MAX_SLEEP_SEC`=30 s),
- treats Ashby's `{"success": false, …}` envelope at HTTP 200 as an application
  error (bad cursor / scope) and maps it to `AshbyApiError`
  ([client.py:187‑207](../../../services/ingest/integrations/ashby/client.py#L187-L207)),
- maps any non-2xx to a typed `AshbyApiError` (`401/403` →
  `ashby_api_unauthorized`, `404` → `ashby_api_not_found`, `429` →
  `ashby_api_rate_limited`) ([client.py:282‑313](../../../services/ingest/integrations/ashby/client.py#L282-L313)).

The endpoints invoked for ingestion:

| Ashby endpoint | Wrapper | Purpose | Code |
|----------------|---------|---------|------|
| `POST /<Category>.list` | `list_entities(category, cursor=…, sync_token=…, limit=…)` | one cursor page of one entity category (backfill walk **or** incremental delta) | [client.py:219‑258](../../../services/ingest/integrations/ashby/client.py#L219-L258) |
| `POST /<Category>.info` | `get_entity(category, entity_id)` | one entity by id (detail / probe) | [client.py:260‑268](../../../services/ingest/integrations/ashby/client.py#L260-L268) |

`category` is the lowercase entity type — e.g. `candidate` POSTs to
`candidate.list` ([client.py:238‑246](../../../services/ingest/integrations/ashby/client.py#L238-L246)).

> Note: `get_entity` / `.info` exists on the client but the backfill/poll fetcher
> and reconciler **only call `.list`** — `.info` has no caller in the ingestion
> path today (it is a detail/probe seam).

### 3.1 Pagination — cursor based

`.list` paginates with a **cursor** (CONFIRMED from Ashby's first-party docs).
The request body carries `cursor` (and `syncToken` for incremental polls); the
response carries `results`, a `moreDataAvailable` bool, and `nextCursor`
([client.py:11‑17](../../../services/ingest/integrations/ashby/client.py#L11-L17)).
`list_entities` returns `(results, next_cursor, next_sync_token)` and derives
`next_cursor` **off `moreDataAvailable`** — it is `None` (terminal for the walk)
unless `moreDataAvailable` is true *and* a non-empty `nextCursor` string came
back ([client.py:248‑258](../../../services/ingest/integrations/ashby/client.py#L248-L258)).

### 3.2 The incremental `syncToken`

When `.list` is supplied a `syncToken`, only entities changed since the token was
minted are returned; each response carries a refreshed `syncToken` to persist for
the next incremental poll. `list_entities` surfaces this as the third return
value (`None` when the listing did not return one)
([client.py:226‑258](../../../services/ingest/integrations/ashby/client.py#L226-L258)).

### 3.3 Rate limits — no dedicated client-side bucket

There is **no Ashby entry** in the ingestion token-bucket table
(`services/ingest/ingestion/rate_limit/buckets.py` has no `ashby` key —
contrast Slack's per-method tiers and GitHub's `rest_authenticated` bucket).
Ashby relies **solely** on the `429 + Retry-After`-aware retry in the client
([client.py:181‑185](../../../services/ingest/integrations/ashby/client.py#L181-L185)).

> **TODO(human)** *(reproduced from the code, not fabricated).* The exact Ashby
> concurrent rate-limit numbers and rate-limit signal are **UNVERIFIED**. The
> default assumes `429 + Retry-After` (env knobs `ASHBY_RL_MAX_ATTEMPTS` /
> `ASHBY_RL_MAX_SLEEP_SEC`); Ashby's docs describe a burst/sustained quota whose
> concurrent number is not pinned here — tune the retry budget once the real
> limits are confirmed ([client.py:19‑23](../../../services/ingest/integrations/ashby/client.py#L19-L23)).

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one `ashby_entity` shard per active
entity type** ([planners/ashby.py:56‑86](../../../services/ingest/ingestion/planners/ashby.py#L56-L86)).
There is a **single** shard kind, `ashby_entity`
([planners/ashby.py:37](../../../services/ingest/ingestion/planners/ashby.py#L37)).

The planner is **stateless** — `ctx.source_client` is `None`. The active entity
list is read from the `ashby_entities` child table, which the SourceOnboarding
loader JSON-aggregates into `ctx.install["entities"]`
([planners/ashby.py:1‑22](../../../services/ingest/ingestion/planners/ashby.py#L1-L22),
loader SQL [source_onboarding.py:673‑691](../../../services/ingest/ingestion/workflows/source_onboarding.py#L673-L691)).

The seeded entity types are `candidate`, `application`, `job`, `interview`,
`offer` ([client.py:316‑319](../../../services/ingest/integrations/ashby/client.py#L316-L319)).
Each shard carries `entity_type`, `org_id`, `installation_id`, and the persisted
`sync_cursor` (the warm-start syncToken — `None` on first sync), at a baseline
`recency_score=1.0` ([planners/ashby.py:67‑80](../../../services/ingest/ingestion/planners/ashby.py#L67-L80)):

```python
shard_identifier = {
    "shard_kind": "ashby_entity",
    "entity_type": entity_type,        # candidate | application | job | interview | offer
    "org_id": org_id,
    "installation_id": install_id,
    "sync_cursor": ent.get("sync_cursor"),   # persisted Ashby syncToken; None on first sync
}
```

> **TODO(human)** *(reproduced from the code, not fabricated).* Confirm the Ashby
> resource taxonomy to shard. The planner is entity-type-agnostic (it reads the
> active list from `ashby_entities`); the seeded entities are `candidate`,
> `application`, `job`, `interview`, `offer`. Application + interview feedback are
> the highest-signal funnel entities; add the others as their read surface is
> confirmed ([planners/ashby.py:14‑19](../../../services/ingest/ingestion/planners/ashby.py#L14-L19)).

---

## 5. The fetcher — one shard kind, two sync modes

`fetch_page_ashby` ([fetchers/ashby.py:126‑195](../../../services/ingest/ingestion/fetchers/ashby.py#L126-L195))
takes one `(install, shard_identifier, cursor)` triple and returns one page of
records plus the next cursor. ShardFetch calls it in a loop, persisting the
cursor between calls.

An `ashby_entity` shard runs in one of two modes:

- **FULL** (initial backfill): `<Category>.list` walked by the response cursor —
  each call carries the prior `nextCursor`, terminating when `moreDataAvailable`
  is false (`nextCursor is None`).
- **INCREMENTAL** (poll): warm-started with a `sync_token` (the persisted
  syncToken threaded in by the planner as `sync_cursor`), the `.list` call passes
  it so only entities changed since the token are returned; the refreshed token
  is captured to persist for the next poll
  ([fetchers/ashby.py:8‑21](../../../services/ingest/ingestion/fetchers/ashby.py#L8-L21)).

### 5.1 The cursor

```python
class AshbyCursor(BaseModel):
    cursor: str | None = None              # Ashby nextCursor page token (None at start/terminal)
    sync_token: str | None = None          # incremental floor; refreshed from each response
    high_water_updated: str | None = None  # max entity updatedAt — the reconciler's gap reference
    rows_seen: int = 0                      # diagnostic
    seeded: bool = False                    # whether first-call setup ran
```

([fetchers/ashby.py:66‑87](../../../services/ingest/ingestion/fetchers/ashby.py#L66-L87)).
On the first call (`not seeded`) the fetcher promotes the shard's `sync_cursor`
into `cursor.sync_token` so an incremental poll warm-starts
([fetchers/ashby.py:136‑144](../../../services/ingest/ingestion/fetchers/ashby.py#L136-L144)).
After each page it persists the refreshed `next_sync_token` (keeping the prior
one if the response returned none) and advances `cursor.cursor`; the page is
terminal when `next_cursor is None`
([fetchers/ashby.py:176‑193](../../../services/ingest/ingestion/fetchers/ashby.py#L176-L193)).

### 5.2 Records — the fetcher tags, the handler shapes

Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` (the lowercased entity type) plus `_fyralis_org_id`; the
raw entity rides under `entity` ([fetchers/ashby.py:167‑174](../../../services/ingest/ingestion/fetchers/ashby.py#L167-L174)):

```python
records.append({
    "_fyralis_record_type": entity_type.lower(),
    "_fyralis_org_id": org_id,
    "entity": row,
})
```

The high-water mark advances over each row's `updatedAt` (falling back to
`createdAt`) — this is the reconciler's gap reference point
([fetchers/ashby.py:106‑119](../../../services/ingest/ingestion/fetchers/ashby.py#L106-L119)).

### 5.3 Rate-limit handling

A `429` (`ashby_api_rate_limited`) from the client is caught by the fetcher and
returned as an **empty page with the cursor preserved and `end_of_data=False`**,
so ShardFetch re-drives the same cursor next cycle. **Every other**
`AshbyApiError` — including `ashby_api_unauthorized` (401/403) — is **re-raised**,
failing the shard ([fetchers/ashby.py:156‑165](../../../services/ingest/ingestion/fetchers/ashby.py#L156-L165)).

---

## 6. The handler — shaping records into `ObservationDraft`

`handle_ashby_object` is a **pure function** (no DB / network) that branches on
the input shape to produce exactly one observation per call
([handlers/ashby.py:1‑29](../../../services/ingest/ingestion/handlers/ashby.py#L1-L29),
[329‑373](../../../services/ingest/ingestion/handlers/ashby.py#L329-L373)):

- **Backfill / poll** — records tagged with `_fyralis_record_type`. The handler
  normalises the kind and builds the draft from the `entity` body
  ([handlers/ashby.py:336‑346](../../../services/ingest/ingestion/handlers/ashby.py#L336-L346)).
- **Live webhook** — an Ashby event `{"action": …, "data": {…entity…},
  "organizationId": …}`. The handler resolves the entity kind from the `action`
  or the data shape ([handlers/ashby.py:309‑322](../../../services/ingest/ingestion/handlers/ashby.py#L309-L322)).
  If the event carries a full body it builds the *same* draft as backfill; if it
  carries only an id + action it emits a **thin change observation** keyed on the
  same `external_id` ([handlers/ashby.py:348‑368](../../../services/ingest/ingestion/handlers/ashby.py#L348-L368)).

Every path runs through `_entity_draft`
([handlers/ashby.py:209‑259](../../../services/ingest/ingestion/handlers/ashby.py#L209-L259)).
Branching on entity kind:

| Source shape | entity_kind | `external_id` | `occurred_at` | `kind` | Trust tier |
|--------------|-------------|---------------|---------------|--------|------------|
| backfill/poll record (`_fyralis_record_type`) | candidate / application / job / interview / offer | `ashby:{org}:{kind}:{id}` | entity `updatedAt`/`createdAt`, else now | `state_change` if terminal status, else `signal` | **authoritative** |
| webhook event with full body | from `action`/`resourceType` | `ashby:{org}:{kind}:{id}` | entity `updatedAt`/… | as above | **authoritative** |
| webhook event id-only | from `action` | `ashby:{org}:{kind}:{id}` | event `updatedAt`, else now | `signal` (thin) | **authoritative** |

Highlights:

- **State-change vs signal.** An object whose lifecycle status is terminal
  (`hired`, `accepted`/`offer_accepted`, `rejected`, `declined`, `withdrawn`,
  `archived`, `closed`, `cancelled`, `filled`) is a `state_change` — the
  hiring-funnel signal; everything else (new candidate, scheduled interview, open
  application) is a `signal`
  ([handlers/ashby.py:57‑62](../../../services/ingest/ingestion/handlers/ashby.py#L57-L62),
  [164‑175](../../../services/ingest/ingestion/handlers/ashby.py#L164-L175)).
  The status is read across kinds from `status` / `offerStatus` / `stage` /
  `state`, flattening Ashby's `{"id","title"}` refs
  ([handlers/ashby.py:116‑129](../../../services/ingest/ingestion/handlers/ashby.py#L116-L129)).
- **Trust posture.** Ashby is the recruiting system of record → **authoritative**
  ([handlers/ashby.py:28](../../../services/ingest/ingestion/handlers/ashby.py#L28),
  [44‑45](../../../services/ingest/ingestion/handlers/ashby.py#L44-L45)). This is
  registered in the `CHANNEL_TRUST_MAP` (`ashby:object` → `authoritative`)
  ([handlers/__init__.py:67](../../../services/ingest/ingestion/handlers/__init__.py#L67),
  set defensively at [handlers/ashby.py:376](../../../services/ingest/ingestion/handlers/ashby.py#L376)).
- **`source_actor_ref`** is `ashby:candidate:{id}` for the person on the object
  (from `name`/`candidateName` + `candidateId`/`id`, or a nested `candidate` ref)
  ([handlers/ashby.py:132‑148](../../../services/ingest/ingestion/handlers/ashby.py#L132-L148)).
- **`entities_hint`** carries an `ashby_object` typed ref (`{kind}:{id}`) plus a
  `person` hint when present
  ([handlers/ashby.py:232‑237](../../../services/ingest/ingestion/handlers/ashby.py#L232-L237)).
- An entity missing `org_id`/`id`, or a payload that is neither a tagged record
  nor a recognisable webhook event, is rejected with a `ValidationError`
  ([handlers/ashby.py:213‑216](../../../services/ingest/ingestion/handlers/ashby.py#L213-L216),
  [370‑373](../../../services/ingest/ingestion/handlers/ashby.py#L370-L373)).

> **TODO(human)** *(reproduced from the code, not fabricated).* The `external_id`
> constructor `ashby_entity(org_id, entity_kind, entity_id)` should, during the
> wiring phase, move to
> `services/ingest/ingestion/idempotency/__init__.py` (the canonical home,
> mirroring `carta_entity`); that module is a SHARED file this phase must not
> edit, so the format string lives in the handler for now and **MUST stay
> byte-identical** across the move
> ([handlers/ashby.py:70‑76](../../../services/ingest/ingestion/handlers/ashby.py#L70-L76)).

---

## 7. Live (real-time) ingestion via webhooks

When a change occurs in Ashby, Ashby **POSTs an HMAC-signed webhook** to
Fyralis's webhook edge (`/webhooks/{provider}/…`,
[router.py:749‑750](../../../services/app/webhooks/router.py#L749-L750)). The
router maps provider `ashby` → channel `ashby:object` for the inline-ingest
fallback ([router.py:469](../../../services/app/webhooks/router.py#L469)).

### 7.1 Signature verification (HMAC-SHA256, hex, over the RAW body)

The verifier is `AshbyVerifier`
([webhooks/signatures/ashby.py:53‑96](../../../services/app/webhooks/signatures/ashby.py#L53-L96)),
registered in `VERIFIERS["ashby"]`
([webhooks/signatures/__init__.py:67](../../../services/app/webhooks/signatures/__init__.py#L67)).
The scheme (CONFIRMED from Ashby's first-party webhook docs,
[signatures/ashby.py:1‑23](../../../services/app/webhooks/signatures/ashby.py#L1-L23)):

```
header:  Ashby-Signature
value:   "sha256=" + lowercase-hex( HMAC-SHA256(secret, raw_body) )
compare: constant-time
```

Verification runs over the **RAW unparsed body bytes** (`body` exactly as
received, no re-serialize), so it must run before any JSON parse — the router
passes the raw body through unchanged
([signatures/ashby.py:77‑80](../../../services/app/webhooks/signatures/ashby.py#L77-L80),
router `raw = await request.body()` then `verifier.verify(body=raw, …)`
[router.py:768](../../../services/app/webhooks/router.py#L768),
[835‑840](../../../services/app/webhooks/router.py#L835-L840)). The header value
must carry the `sha256=` prefix or it is rejected as
`malformed_signature_header` ([signatures/ashby.py:68‑73](../../../services/app/webhooks/signatures/ashby.py#L68-L73)).

The verifier loops over **all** active secrets so a rotation (two valid secrets
in flight) verifies, and returns `signed_timestamp=None`
([signatures/ashby.py:75‑96](../../../services/app/webhooks/signatures/ashby.py#L75-L96)).
The per-tenant signing secret(s) are loaded by
`services/app/webhooks/secrets.py::load_installation_secrets` from the
`provider_installations`
row (`provider='ashby'`) ([signatures/ashby.py:15‑18](../../../services/app/webhooks/signatures/ashby.py#L15-L18),
[router.py:829‑831](../../../services/app/webhooks/router.py#L829-L831)).

> **No replay window.** Like GitHub/Brex, Ashby signs the body alone (no
> timestamp envelope), so there is no replay window here; idempotency is enforced
> at the **ingestion layer** via the `external_id` dedup, not at signature
> verification ([signatures/ashby.py:20‑22](../../../services/app/webhooks/signatures/ashby.py#L20-L22)).

### 7.2 Tenant resolution

The tenant is resolved from the body `organizationId` →
`provider_installations` (`provider='ashby'`, `installation_id=org_id`)
([tenant_resolver.py:496‑508](../../../services/app/webhooks/tenant_resolver.py#L496-L508)).
As §2.3 notes, in production this is intended to be endpoint/secret-scoped — the
`organizationId` body field is the synthetic-gate stand-in (see the
`TODO(human)` above).

### 7.3 Kafka cutover (inline `ashby:object` when off)

Ashby webhooks fit the **202 cutover contract**. When the resolved tenant has
`ingestion.kafka_path_enabled=TRUE`, the router publishes the verified envelope
to Kafka and returns `202` (inline `ingest()` is skipped); when the flag is off
it falls through to the **inline** path, ingesting via channel `ashby:object`
([router.py:160‑191](../../../services/app/webhooks/router.py#L160-L191),
cutover branch [router.py:1037‑1095](../../../services/app/webhooks/router.py#L1037-L1095),
inline `channel = _PROVIDER_CHANNEL[provider]`
[router.py:1095](../../../services/app/webhooks/router.py#L1095)). There are **no
Ashby lifecycle events** in the router (no `installation`-style branch like
GitHub's) — every verified, tenant-resolved Ashby delivery is an observation.

---

## 8. Reconciliation — gap detection

`reconcile_ashby` ([reconcilers/ashby.py:127‑164](../../../services/ingest/ingestion/reconcilers/ashby.py#L127-L164))
re-checks completed (`state == "done"`) entity shards for new activity. For each
shard it runs **one cheap 1-row incremental probe** per entity type
([reconcilers/ashby.py:78‑124](../../../services/ingest/ingestion/reconcilers/ashby.py#L78-L124)):

1. Load the shard's persisted cursor; read `sync_token` and `high_water_updated`.
2. If there is **no** persisted `sync_token`, **skip** — without it there is no
   cheap incremental probe (a full re-walk would over-reshare every cycle)
   ([reconcilers/ashby.py:91‑96](../../../services/ingest/ingestion/reconcilers/ashby.py#L91-L96)).
3. Probe `list_entities(entity_type, sync_token=…, limit=1)`. If it returns any
   row, there is a gap.

On a gap it reshares an `ashby_entity` shard at **`recency_score=1.5`**,
warm-started at the persisted `sync_token` (incremental mode) and carrying
`gap_baseline_updated=high_water` + `parent_shard_id`
([reconcilers/ashby.py:112‑124](../../../services/ingest/ingestion/reconcilers/ashby.py#L112-L124)).
A probe error is logged and treated as "no gap" (best-effort)
([reconcilers/ashby.py:102‑107](../../../services/ingest/ingestion/reconcilers/ashby.py#L102-L107)).

`external_id` parity means re-walked entities dedup against what backfill already
wrote — only genuinely new/changed entities produce new observations. The
reconciler is explicitly "pragmatic v1": it can **over-reshare** but never
**under-reshares**, and dedup makes re-walks idempotent
([reconcilers/ashby.py:10‑15](../../../services/ingest/ingestion/reconcilers/ashby.py#L10-L15)).
The reconciler resolves the install row itself (`set_pool_provider` wires the
pool at startup) ([reconcilers/ashby.py:40‑54](../../../services/ingest/ingestion/reconcilers/ashby.py#L40-L54),
[127‑147](../../../services/ingest/ingestion/reconcilers/ashby.py#L127-L147), wired from
[workflows/reconciler.py:781‑806](../../../services/ingest/ingestion/workflows/reconciler.py#L781-L806)).

---

## 9. Revocation chokepoint — **absent**

There is **no revocation chokepoint** for Ashby. Unlike the GitHub client (which
disables an installation on documented `401`/`404` revocation signals via
`_maybe_disable_on_revocation`), the Ashby client has **no disable-on-revocation
path**: a `401`/`403` is mapped to `ashby_api_unauthorized`
([client.py:287‑292](../../../services/ingest/integrations/ashby/client.py#L287-L292))
and **re-raised** by the fetcher, failing the shard — it does **not** flip
`ashby_installations.disabled_at` or `provider_installations.enabled`
([fetchers/ashby.py:156‑165](../../../services/ingest/ingestion/fetchers/ashby.py#L156-L165);
the only `disabled_at` write is the install UPSERT clearing it to `NULL`,
[onboarding.py:72](../../../services/ingest/integrations/ashby/onboarding.py#L72)).
The install rows carry `disabled_at` / `enabled` columns and the loader filters
on `disabled_at IS NULL`, so disabling is **possible** but is not driven by an
auth-failure signal in this layer.

---

## 10. End-to-end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  onboarding: seed ashby_installations + ashby_entities +         │
                          │     onboarding_triggers(source='ashby')                          │
   API KEY (Basic, empty  │  planner: read active entity list from ashby_entities            │
   password) per org_id   │     └─► one `ashby_entity` shard per (org, entity_type)          │
                          │  fetcher: POST /<Category>.list  (cursor-paged)                  │
                          │     └─► tag record _fyralis_record_type + _fyralis_org_id        │
                          └──────────────────────────────────────────────────────────────┬──┘
                          ┌──────────────────────── POLL (incremental) ───────────────────┐│
                          │  reconciler: 1-row probe per entity (sync_token, limit=1)      ││
                          │     └─► gap → reshare `ashby_entity` @1.5, warm syncToken       ││
                          │  fetcher (poll mode): .list with syncToken → only changed rows ││
                          └────────────────────────────────────────────────────────────────┘│
                          ┌──────────────────────── LIVE (push) ──────────────────────────┐ │
   ANY Ashby change ──────►  Ashby webhook ──HTTP POST──► /webhooks/ashby                 │ │
                          │     verify Ashby-Signature: sha256=<hex(HMAC-SHA256(raw body))>│ │
                          │     tenant ← organizationId (prod: endpoint/secret-scoped)     │ │
                          │     kafka_path on → 202 ; off → inline ashby:object            │ │
                          └────────────────────────────────────────────────────────────────┘ │
                                                                                              │
                                                          ┌───────────────────────────────────▼─┐
                                                          │  handle_ashby_object                 │
                                                          │  backfill/poll record OR webhook event│
                                                          │  external_id = ashby:{org}:{kind}:{id}│
                                                          │  trust = authoritative               │
                                                          │  → ObservationDraft                  │
                                                          └──────────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, incremental poll, and webhook
   all route to `ashby:object` and dedup on
   `external_id = "ashby:{org}:{entity_kind}:{id}"` (NOT version-suffixed). A
   backfilled entity, its poll re-walk, and its live twin collapse into one
   observation.
2. **One credential model.** A single long-lived **API key** as HTTP Basic
   (username = key, **empty** password). No OAuth, no refresh token.
3. **Two surfaces, one client.** Backfill reads `ashby_installations`; live reads
   `provider_installations` (`installation_id=org_id`). They are seeded together
   but independent.
4. **Cursor pagination + syncToken incremental.** `.list` walks by `nextCursor`
   (terminal off `moreDataAvailable`); a persisted `syncToken` makes the poll +
   reconciler probe return only changed entities.
5. **Webhook signed over the RAW body, no replay window.** `Ashby-Signature:
   sha256=<hex>`, HMAC-SHA256, constant-time, all active secrets tried;
   idempotency is the `external_id` dedup.
6. **No client-side rate-limit bucket and no revocation chokepoint** — Ashby
   relies on the `429 + Retry-After` retry, and an auth failure fails the shard
   rather than disabling the install.

---

## 11. Configuration & compliance

The auth scheme (API-key-as-Basic, empty password), the RPC `.list`/`.info`
verbs, cursor pagination, the `syncToken` incremental, and the webhook signing
scheme are **CONFIRMED** from Ashby's first-party docs. The production
rate-limit numbers and the real webhook tenant-resolution are **UNVERIFIED** and
carry `TODO(human)` markers (§3.3, §2.3).

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `ASHBY_RL_MAX_ATTEMPTS` | `4` | `429 + Retry-After` retry budget ([client.py:161](../../../services/ingest/integrations/ashby/client.py#L161)) |
| `ASHBY_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:162](../../../services/ingest/integrations/ashby/client.py#L162)) |
| `ASHBY_BACKFILL_PAGE_SIZE` | `100` | `.list` page size, capped at 1000 ([fetchers/ashby.py:59‑63](../../../services/ingest/ingestion/fetchers/ashby.py#L59-L63)) |
| `ASHBY_API_BASE_URL` | `https://api.ashbyhq.com` | per-source API host override ([endpoints.py:140](../../../lib/integrations/endpoints.py#L140)) |
| `WEBHOOK_SECRET_ASHBY` | — | HMAC signing secret in dev/Provider Lab mode ([validation_runs/composition.py:142](../../../services/ingest/synthetic/validation_runs/composition.py#L142)) |

### 11.2 Verified compliant

- **API-key auth** — `Authorization: Basic base64("KEY:")` (empty password),
  key never logged, resolved once. ✅
- **RPC surface** — `POST /<Category>.list` / `.info`; `success=false` at HTTP
  200 mapped to a typed error. ✅
- **Pagination** — cursor via `nextCursor`, terminal off `moreDataAvailable`. ✅
- **Incremental** — `syncToken` request/response round-trip, persisted per
  entity type. ✅
- **Webhook signing** — HMAC-SHA256, lowercase hex, `Ashby-Signature: sha256=…`,
  over the RAW body, constant-time, rotation-aware. ✅
- **Trust posture** — recruiting system of record → `authoritative`. ✅
- **Rate limits** — `429 + Retry-After` retry only; concurrent quota
  **UNVERIFIED** (see §3.3 `TODO(human)`).

### 11.3 Dev / Provider Lab mode

For local testing against the mock source servers, `build_ashby_client` detects
Provider Lab mode and **presets** the API key to `spam-ashby`, skips the secret store,
and points the API base at Provider Lab's `/ashby` URL via the endpoint
resolver ([_clients.py:749‑776](../../../services/ingest/ingestion/fetchers/_clients.py#L749-L776),
Provider Lab path [endpoints.py:170](../../../lib/integrations/endpoints.py#L170)).
The in-process `MockAshbyClient` replaces the real client at the
`_open_ashby_client` seam, mirroring cursor pagination (decimal-offset tokens),
the syncToken floor, and the production `AshbyApiError` codes for fault injection
([synthetic/mock_clients/ashby.py:1‑31](../../../services/ingest/synthetic/mock_clients/ashby.py#L1-L31)).
The synthetic webhook generator signs with the `sha256=`+hex HMAC-SHA256 scheme
and sends the org id as top-level `organizationId`
([synthetic/validation_runs/composition.py:115‑118](../../../services/ingest/synthetic/validation_runs/composition.py#L115-L118),
[221‑224](../../../services/ingest/synthetic/validation_runs/composition.py#L221-L224)).
