# Carta Ingestion — How Fyralis Pulls Carta Data

This document explains, in detail, **how Carta data enters Fyralis**: which Carta
REST endpoints are called, with which token, and how the cap-table signal set —
**shareholders, share classes, SAFE notes, and option grants** — is each
ingested.

It deliberately stops at the point where a Carta cap-table object becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Archetype caveat (read this first).** The Carta integration is a clone of the
> Gusto OAuth2 archetype (itself a QuickBooks clone). Several read-surface
> specifics are still **unverified placeholders** carried over from that clone,
> and the code says so in `TODO(human)` / `CONFIRMED` markers. This doc
> reproduces those markers verbatim and labels everything inferred. The
> **verified** facts — auth is OAuth2 Bearer (no refresh grant), there is **no
> webhook** (poll-only), Carta is **not** in `VERIFIERS`, and the exact
> `external_id` shape — are called out explicitly where they appear.

---

## 1. The two ways data arrives

Carta data reaches Fyralis through **two independent paths that converge on one
handler** — and **neither path is an inbound webhook**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* the firm's cap-table entities via the Carta **REST API** (`/v1/firms/{firm_id}/...`) | [planners/carta.py](../../../services/ingest/ingestion/planners/carta.py), [fetchers/carta.py](../../../services/ingest/ingestion/fetchers/carta.py) |
| **Live (incremental)** | A cap-table object changes | Fyralis **polls** Carta on an interval and dispatches each detected change in-process (**no webhook**) | [integrations/carta/poll.py](../../../services/ingest/integrations/carta/poll.py) |

**Carta has no inbound webhook.** Unlike GitHub (HTTP push) or Slack (Events
API), there is no HTTP edge that Carta calls. The live edge is a **poll**: the
poll dispatcher re-runs the same cap-table change build under
`ingress_kind="poll"` ([poll.py:116](../../../services/ingest/integrations/carta/poll.py#L116)).
Because there is no signed HTTP body, **Carta is not registered in the webhook
`VERIFIERS` map** ([signatures/__init__.py:44‑68](../../../services/app/webhooks/signatures/__init__.py#L44-L68) — `carta` is absent), and `carta_installations`
deliberately has **no `webhook_secret_ref` column**
([0104_carta.sql:66](../../../db/migrations/0104_carta.sql#L66)). The trust
boundary is the authenticated OAuth poll connection itself, not an HMAC
([poll.py:23‑24](../../../services/ingest/integrations/carta/poll.py#L23-L24)).

Crucially, **both paths produce the exact same record shape** — a fetcher-tagged
record `{_fyralis_record_type, _fyralis_firm_id, entity}` — and both are parsed by
the **single** `carta:object` handler
([handlers/carta.py:252‑276](../../../services/ingest/ingestion/handlers/carta.py#L252-L276)).
The poll dispatcher's `build_change_record` emits the *identical* shape the
backfill fetcher emits ([poll.py:68‑88](../../../services/ingest/integrations/carta/poll.py#L68-L88)),
so a backfilled object and its live-poll twin derive the **same** dedup key:

```
external_id = "carta:{firm_id}:{entity_kind}:{entity_id}:{sync_token}"
```

([handlers/carta.py:61‑75](../../../services/ingest/ingestion/handlers/carta.py#L61-L75)).
Because cap-table objects **mutate** (a SAFE converts, an option grant vests), the
`external_id` is **versioned by `SyncToken`** and **discriminated by
`entity_kind`** — so a state change lands as a **new** observation, while an
unchanged re-walk collapses into one. This is the central design invariant of
Carta ingestion ([handlers/carta.py:21‑26](../../../services/ingest/ingestion/handlers/carta.py#L21-L26)).

The channel mapping confirms both ingress kinds route to one channel
([channel_mapping.py:279‑280](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L279-L280)):

```
("carta", "backfill") → "carta:object"
("carta", "poll")     → "carta:object"
```

---

## 2. Authentication & token model — OAuth2 Bearer, no refresh grant

Carta is authenticated with **OAuth 2.0 — a short-lived Bearer access token**,
every call scoped to a firm `firm_id` (the scope-id, analogous to Gusto's
`company_uuid` / QuickBooks' `realmId`)
([client.py:1‑8](../../../services/ingest/integrations/carta/client.py#L1-L8)).

### 2.1 What's verified vs unbuilt

**CONFIRMED in code** (`docs.carta.com/api-platform`,
[oauth.py:12‑18](../../../services/ingest/integrations/carta/oauth.py#L12-L18)):
Carta OAuth2 supports only **AUTHORIZATION_CODE** and **CLIENT_CREDENTIALS**
grants — there is **NO `refresh_token` grant**. Access tokens live ~1 hour; you
**re-mint** (re-run client_credentials, or re-exchange a fresh 60-second auth
code) rather than refresh. The API is versioned **`v1alpha1`** (alpha — expect
breaking changes).

**Not built yet.** The client resolves the access token **once** from the secret
store and reuses it for the life of the client; there is **no token refresh /
re-mint loop**. This is the documented-but-unbuilt seam:

> **TODO(human):** implement Carta OAuth token refresh — NONE exists yet
> ([client.py:9‑14](../../../services/ingest/integrations/carta/client.py#L9-L14)).
> The install row persists `refresh_secret_ref` + `token_expires_at`, but Carta
> has no refresh grant, so the seam must be a **re-mint-on-401** (client_credentials)
> or an `oauth_poller`, treating `refresh_secret_ref` as the client-credentials
> material, not an OAuth refresh token
> ([oauth.py:19‑25](../../../services/ingest/integrations/carta/oauth.py#L19-L25)).
> *(Consequence: today an expired token surfaces as a `carta_api_unauthorized`
> 401 with no recovery — see §9.)*

> **TODO(human):** ACCESS IS PARTNER-GATED — invite-only + SOC 2 Type 2 since
> 2025. The approved prod host/scopes must be obtained before real traffic; dev
> against `https://mock-api.carta.com`
> ([oauth.py:19‑25](../../../services/ingest/integrations/carta/oauth.py#L19-L25),
> [endpoints.py:92‑97](../../../lib/integrations/endpoints.py#L92-L97)).

### 2.2 Where credentials live

There is **no OAuth bounce** (authorize → callback → code exchange) implemented.
The production install surface is **operator-mediated credential submission**: an
operator pastes the `firm_id` + `access_token` (and optionally `refresh_token`)
they obtained from their Carta OAuth app, and the connect router verifies them
against the **real Carta API** before seeding the install
([oauth.py:1‑11](../../../services/ingest/integrations/carta/oauth.py#L1-L11)).

| Credential | Where | Notes |
|-----------|-------|-------|
| `firm_id` | `carta_installations.firm_id` (plaintext scope-id) | scopes every call; the poll tenant-resolution key ([0104_carta.sql:53‑55](../../../db/migrations/0104_carta.sql#L53-L55)) |
| Access token | `encrypted_secrets` behind `secret_ref` (label `carta_access_token:{firm_id}`) | OAuth Bearer; resolved once, reused ([oauth.py:199‑201](../../../services/ingest/integrations/carta/oauth.py#L199-L201)) |
| Refresh token | `encrypted_secrets` behind `refresh_secret_ref` (label `carta_refresh_token:{firm_id}`) | persisted but **unused** (no refresh grant) ([oauth.py:202‑207](../../../services/ingest/integrations/carta/oauth.py#L202-L207)) |
| `token_expires_at` | `carta_installations` column | persisted for a future poller's refresh schedule; not yet consulted ([0104_carta.sql:64‑65](../../../db/migrations/0104_carta.sql#L64-L65)) |
| Webhook secret | — | **none** — Carta is poll-only, no webhook edge ([0104_carta.sql:66](../../../db/migrations/0104_carta.sql#L66)) |

The access token / `Authorization` header are **never logged**
([client.py:27](../../../services/ingest/integrations/carta/client.py#L27)), and
`CartaApiError` keeps the token off its context so a credential failure never
echoes the secret back ([oauth.py:113‑115](../../../services/ingest/integrations/carta/oauth.py#L113-L115)).

### 2.3 The connect wizard (how an install gets registered)

`services/ingest/integrations/carta/oauth.py` implements a two-step credential
wizard. **Note (inferred):** the router is **not wired into the app** anywhere in
this repo — no module imports `carta.oauth.router`. It exists as the install
surface but is not yet mounted.

1. **`POST /integrations/carta/connect/preflight`** (Bearer-authed) — verifies
   the token + firm by calling `CartaClient.firm_info()`; an auth failure returns
   a structured `400` with **no secret stored**
   ([oauth.py:137‑162](../../../services/ingest/integrations/carta/oauth.py#L137-L162)).
2. **`POST /integrations/carta/connect/finalize`** (Bearer-authed) — re-verifies
   the creds **before any write**, stores the access (and optional refresh) token
   encrypted-at-rest, then calls `finalize_install`
   ([oauth.py:165‑231](../../../services/ingest/integrations/carta/oauth.py#L165-L231)).
   `finalize_install` UPSERTs a `carta_installations` row keyed on
   `(tenant_id, firm_id)`, INSERTs one `carta_entities` row per entity type, and
   emits an `onboarding_triggers` row (`source='carta'`, `trigger_kind='install'`)
   so the M6 backfill chain fires — all in one tenant-scoped transaction
   ([onboarding.py:37‑112](../../../services/ingest/integrations/carta/onboarding.py#L37-L112)).
3. Because Carta is poll-only, the wizard registers **no
   `provider_installations` row** and accepts **no webhook verifier token**
   ([oauth.py:27‑31](../../../services/ingest/integrations/carta/oauth.py#L27-L31)).

---

## 3. The Carta REST API surface that is actually called

All read calls funnel through `CartaClient._request`
([client.py:160‑211](../../../services/ingest/integrations/carta/client.py#L160-L211)),
which:

- sets `Authorization: Bearer {token}` and `Accept: application/json`,
- honours `429 Retry-After` within a bounded retry budget
  (`CARTA_RL_MAX_ATTEMPTS`=4, `CARTA_RL_MAX_SLEEP_SEC`=30),
- maps transport errors and any non-2xx to `CartaApiError` with a stable `code`
  (`carta_api_unauthorized` / `_not_found` / `_rate_limited` / `_error`)
  ([client.py:273‑304](../../../services/ingest/integrations/carta/client.py#L273-L304)).

The endpoints invoked for ingestion:

| Carta endpoint | Wrapper | Purpose | Code |
|----------------|---------|---------|------|
| `GET /v1/firms/{firm_id}/query` | `CartaClient.query()` | run one paged `SELECT` against an entity type | [client.py:217‑252](../../../services/ingest/integrations/carta/client.py#L217-L252) |
| `GET /v1/firms/{firm_id}/firminfo/{firm_id}` | `CartaClient.firm_info()` | connectivity / credential probe (used by the connect wizard) | [client.py:254‑259](../../../services/ingest/integrations/carta/client.py#L254-L259) |

> **UNVERIFIED — this is the Gusto/QuickBooks clone's placeholder surface.** The
> code flags both the endpoint shape and the query language as unconfirmed:
>
> **TODO(human):** confirm Carta API host + read endpoints + OAuth scopes. The
> read surface above clones the Gusto/QuickBooks `query` endpoint as a
> placeholder; Carta's real read surface is REST collections under
> `/v1/firms/{firm_id}/...` (shareholders, share_classes, safes, option_grants)
> ([client.py:16‑23](../../../services/ingest/integrations/carta/client.py#L16-L23)).
>
> **TODO(human):** confirm Carta's real list/pagination shape. `query` builds a
> `SELECT * FROM <Entity> [WHERE ...] ORDERBY <f> STARTPOSITION n MAXRESULTS m`
> string and reads a `QueryResponse` envelope — the QuickBooks query language,
> **not** Carta's real REST shape
> ([client.py:229‑234](../../../services/ingest/integrations/carta/client.py#L229-L234)).

### 3.1 Pagination — offset (placeholder) vs token (the real shape)

As **implemented**, `query` is **offset-paginated**: it issues `STARTPOSITION n
MAXRESULTS m`, advances `start_position` by the rows returned, and treats a short
page (`maxResults < requested` or empty) as terminal — returning
`next_start_position is None`
([client.py:236‑252](../../../services/ingest/integrations/carta/client.py#L236-L252)).

But the code records the **real** shape it must move to:

> **CONFIRMED (docs.carta.com/api-platform):** Carta's API is `v1alpha1` and uses
> **Google-AIP-style token pagination** — `pageSize` (int) + `pageToken` (string;
> response returns the next page token), **not** the offset/STARTPOSITION the
> Gusto clone uses. Swap `client.query(...)`'s offset bookkeeping to
> `pageSize`/`pageToken`
> ([fetchers/carta.py:29‑34](../../../services/ingest/ingestion/fetchers/carta.py#L29-L34)).

Page size is env-overridable via `CARTA_BACKFILL_PAGE_SIZE` (capped at 1000,
default 100) ([fetchers/carta.py:62‑66](../../../services/ingest/ingestion/fetchers/carta.py#L62-L66)).

### 3.2 Rate limits

> **CONFIRMED (docs.carta.com/api-platform):** rate-limit signal is **429**
> (burst 10/s, sustained 300/min)
> ([fetchers/carta.py:33‑34](../../../services/ingest/ingestion/fetchers/carta.py#L33-L34)).

There is **no dedicated client-side token bucket** for Carta — the source does
not appear in `rate_limit/buckets.py`. Backpressure is the **server-driven
`429` + `Retry-After`** loop in the client (§3) plus the env knobs
`CARTA_RL_MAX_ATTEMPTS` / `CARTA_RL_MAX_SLEEP_SEC`
([client.py:24‑25](../../../services/ingest/integrations/carta/client.py#L24-L25)).

---

## 4. Backfill scope — one shard per entity type

The planner decomposes one firm install into **one `carta_entity` shard per
active cap-table entity type**
([planners/carta.py:54‑83](../../../services/ingest/ingestion/planners/carta.py#L54-L83)).

`ctx.source_client` is **None** for Carta — the planner reads the entity list
from DB state, not the API
([planners/carta.py:19](../../../services/ingest/ingestion/planners/carta.py#L19),
[source_onboarding.py:910‑916](../../../services/ingest/ingestion/workflows/source_onboarding.py#L910-L916)).
The SourceOnboarding loader JSON-aggregates the `carta_entities` child table onto
`ctx.install["entities"]` (each `{entity_type, updated_cursor}`), so the planner
stays stateless
([source_onboarding.py:625‑643](../../../services/ingest/ingestion/workflows/source_onboarding.py#L625-L643)).

The seeded entity set is `("Shareholder", "ShareClass", "SafeNote",
"OptionGrant")` ([client.py:307‑309](../../../services/ingest/integrations/carta/client.py#L307-L309)).
Each shard carries `entity_type`, `firm_id`, `installation_id`, and the warm-start
`updated_cursor` (the high-water `LastUpdatedTime`, `None` on first sync), at a
baseline `recency_score=1.0`
([planners/carta.py:65‑77](../../../services/ingest/ingestion/planners/carta.py#L65-L77)).

> **TODO(human):** confirm the Carta resource taxonomy to shard. The planner is
> entity-type-agnostic (it reads the active list from `carta_entities`), but the
> seeded set is `shareholders / share_classes / safes / option_grants`, each
> path-scoped under `/v1/firms/{firm_id}/...`. Confirm the high-signal entity set
> and add the others as their read surface is confirmed
> ([planners/carta.py:12‑17](../../../services/ingest/ingestion/planners/carta.py#L12-L17)).

---

## 5. Fetching — one shard kind, two sync modes

`fetch_page_carta` ([fetchers/carta.py:126‑195](../../../services/ingest/ingestion/fetchers/carta.py#L126-L195))
takes one `(install, shard_identifier, cursor)` triple and returns one page of
records + the next cursor. ShardFetch calls it in a loop, persisting the cursor
between calls. A `carta_entity` shard streams **one** cap-table entity type and
runs in one of two modes:

- **FULL (initial backfill)** — `SELECT * FROM <Entity> ORDERBY
  Metadata.LastUpdatedTime STARTPOSITION n MAXRESULTS m`, offset-paginated
  ([fetchers/carta.py:14‑15](../../../services/ingest/ingestion/fetchers/carta.py#L14-L15)).
- **INCREMENTAL (poll/reconcile re-walk)** — when warm-started with an
  `updated_cursor`, the `WHERE` clause adds `Metadata.LastUpdatedTime >
  '<cursor>'` so only changed entities come back
  ([fetchers/carta.py:144‑148](../../../services/ingest/ingestion/fetchers/carta.py#L144-L148)).

### 5.1 Cursor

```python
class CartaCursor(BaseModel):
    start_position: int = 1            # CARTA STARTPOSITION offset (1-based)
    high_water_updated: str | None     # max Metadata.LastUpdatedTime seen — warm-start
                                       #   bound AND the reconciler's gap reference
    incremental_floor: str | None      # the `LastUpdatedTime >` lower bound (None in FULL)
    rows_seen: int = 0                 # diagnostic
    seeded: bool = False               # first-call setup ran
```

([fetchers/carta.py:69‑89](../../../services/ingest/ingestion/fetchers/carta.py#L69-L89)).
On the first call the fetcher freezes any warm `updated_cursor` into both
`incremental_floor` and `high_water_updated`
([fetchers/carta.py:137‑143](../../../services/ingest/ingestion/fetchers/carta.py#L137-L143)). Each row's
`Metadata.LastUpdatedTime` bumps `high_water_updated` to the max seen
([fetchers/carta.py:107‑120](../../../services/ingest/ingestion/fetchers/carta.py#L107-L120),
[177](../../../services/ingest/ingestion/fetchers/carta.py#L177)).

> **TODO(human):** confirm the per-entity "updated since" filter field name.
> Carta is poll-only and most tables refresh ~daily, so a **full re-walk**
> (idempotent via the versioned `external_id`) is an acceptable fallback if no
> incremental filter exists
> ([fetchers/carta.py:35‑40](../../../services/ingest/ingestion/fetchers/carta.py#L35-L40)).

### 5.2 The record shape

Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` (the entity type, lowercased) plus `_fyralis_firm_id`
([fetchers/carta.py:170‑177](../../../services/ingest/ingestion/fetchers/carta.py#L170-L177)):

```python
records.append({
    "_fyralis_record_type": entity_type.lower(),   # shareholder | shareclass | safenote | optiongrant
    "_fyralis_firm_id": firm_id,
    "entity": row,                                 # the full Carta object
})
```

This is the byte-identical shape the poll dispatcher emits (§7), which is what
gives cross-path dedup.

### 5.3 Rate-limit soft-pause

If a page hits `carta_api_rate_limited`, the fetcher returns an **empty page with
the same cursor and `end_of_data=False`** so ShardFetch retries the shard later
rather than failing the run; any other `CartaApiError` propagates
([fetchers/carta.py:159‑168](../../../services/ingest/ingestion/fetchers/carta.py#L159-L168)).

---

## 6. The handler — shaping a cap-table object into `ObservationDraft`

`handle_carta_object` ([handlers/carta.py:252‑276](../../../services/ingest/ingestion/handlers/carta.py#L252-L276))
is a pure function (no DB / network). It branches on the input shape and produces
**exactly one** observation per call. Carta is poll-only, so there is **no
webhook-envelope branch** — there is a single branch on `_fyralis_record_type`
that serves both backfill and live poll
([handlers/carta.py:259‑271](../../../services/ingest/ingestion/handlers/carta.py#L259-L271)). An untagged payload is
rejected with a `ValidationError`
([handlers/carta.py:273‑276](../../../services/ingest/ingestion/handlers/carta.py#L273-L276)).

`_entity_draft` ([handlers/carta.py:177‑238](../../../services/ingest/ingestion/handlers/carta.py#L177-L238))
derives the draft:

| Field | Source | Notes |
|-------|--------|-------|
| `source_channel` | `"carta:object"` | the single channel |
| `external_id` | `carta:{firm_id}:{entity_kind}:{entity_id}:{sync_token}` | versioned by `SyncToken`, discriminated by `entity_kind` ([handlers/carta.py:185‑186](../../../services/ingest/ingestion/handlers/carta.py#L185-L186)) |
| `occurred_at` | `Metadata.LastUpdatedTime` (ISO) or now | ([handlers/carta.py:188‑189](../../../services/ingest/ingestion/handlers/carta.py#L188-L189)) |
| `kind` | `state_change` or `signal` | by lifecycle status (§6.1) |
| `trust_tier` | `authoritative` | Carta is the cap-table system of record ([handlers/carta.py:43‑44](../../../services/ingest/ingestion/handlers/carta.py#L43-L44)) |
| `source_actor_ref` | `carta:stakeholder:{id}` or `None` | from the object's `StakeholderRef`/`HolderRef` ([handlers/carta.py:116‑129](../../../services/ingest/ingestion/handlers/carta.py#L116-L129)) |
| `entities_hint` | `{type:"carta_object", id:"{kind}:{id}"}` + the stakeholder hint | ([handlers/carta.py:208‑213](../../../services/ingest/ingestion/handlers/carta.py#L208-L213)) |

The record-type → canonical kind map is
`{shareholder→shareholder, shareclass→share_class, safenote→safe_note,
optiongrant→option_grant}` ([handlers/carta.py:47‑52](../../../services/ingest/ingestion/handlers/carta.py#L47-L52)).
A `firm_id`/`Id`-less entity raises a `ValidationError`
([handlers/carta.py:180‑184](../../../services/ingest/ingestion/handlers/carta.py#L180-L184)).

### 6.1 Lifecycle states → `state_change`

Cap-table objects mutate through lifecycle states, and `_classify` maps the
object's `Status` to a `kind`
([handlers/carta.py:139‑149](../../../services/ingest/ingestion/handlers/carta.py#L139-L149)):

| `Status` (lowercased) | `kind` |
|-----------------------|--------|
| `converted`, `exercised`, `cancelled`/`canceled`, `terminated`, `repurchased`, `expired`, `forfeited` | **`state_change`** |
| anything else (e.g. a new shareholder, an open grant) | **`signal`** |

The status set lives in `_STATE_CHANGE_STATUSES`
([handlers/carta.py:55‑58](../../../services/ingest/ingestion/handlers/carta.py#L55-L58)). Because the
`external_id` is versioned by `SyncToken`, a status transition (a SAFE that
converts, a grant that is exercised) lands as a **new** observation rather than
overwriting the prior one.

---

## 7. Live (incremental) ingestion via the poll edge

**Carta has no inbound webhook.** Its live edge is a **poll**: the dispatcher
re-lists changed cap-table objects on an interval and dispatches each detected
change directly through the ingestion pipeline — the Telegram-gateway
`handle_update` analog, but driven by a poller instead of a persistent connection
([poll.py:1‑13](../../../services/ingest/integrations/carta/poll.py#L1-L13)).

A Carta poller holds **one firm's install = one tenant's install**, so the tenant
is known by construction (carried on `PollDeps`) — there is **no per-change tenant
resolution** ([poll.py:9‑11](../../../services/ingest/integrations/carta/poll.py#L9-L11),
[47‑66](../../../services/ingest/integrations/carta/poll.py#L47-L66)).

`handle_polled_change` ([poll.py:144‑185](../../../services/ingest/integrations/carta/poll.py#L144-L185)) per change:

1. **Build the canonical record.** `build_change_record` produces the **same**
   `_fyralis_record_type`-tagged shape the backfill fetcher emits → identical
   `external_id` → cross-path dedup
   ([poll.py:68‑88](../../../services/ingest/integrations/carta/poll.py#L68-L88)).
2. **Cutover branch (kafka-first default).** If `tenant_flags.kafka_path_enabled`
   for the tenant (and the producer + S3 client are wired), `shadow_write_raw`
   publishes the record to `ingestion.raw.carta` with **`ingress_kind="poll"`**
   and returns — the normalizer + observation_writer produce the observation,
   concurrently with any in-flight backfill
   ([poll.py:106‑127](../../../services/ingest/integrations/carta/poll.py#L106-L127),
   [150‑163](../../../services/ingest/integrations/carta/poll.py#L150-L163)).
3. **Fallback / inline.** Otherwise `core.ingest("carta:object", record, …)`,
   then a best-effort M2 shadow-write audit when `SHADOW_WRITE_ENABLED`
   ([poll.py:165‑185](../../../services/ingest/integrations/carta/poll.py#L165-L185)). A failed cutover
   degrades gracefully into the inline path so a change is never dropped
   ([poll.py:159‑163](../../../services/ingest/integrations/carta/poll.py#L159-L163)).

There is **no HMAC signature gate and no HTTP status** — the trust boundary is
the authenticated OAuth poll connection itself
([poll.py:23‑24](../../../services/ingest/integrations/carta/poll.py#L23-L24)).

> **Note (inferred): there is no wired poll loop/scheduler in this repo.**
> `handle_polled_change` is the dispatch primitive, but the only caller is the
> synthetic live generator (`synthetic/live_generators/carta_poll.py`) that drives
> it in-process for testing. A production interval driver (the analog of an
> `oauth_poller` re-list loop) is not present — consistent with the §2.1
> "documented-but-unbuilt" posture.

---

## 8. Reconciliation — gap detection

`reconcile_carta` ([reconcilers/carta.py:125‑163](../../../services/ingest/ingestion/reconcilers/carta.py#L125-L163))
re-checks **done** shards for new activity. It loads the (single, non-disabled)
`carta_installations` row for the tenant, opens a client, and for each completed
`carta_entity` shard runs `_check_one_shard_for_gap`
([reconcilers/carta.py:80‑122](../../../services/ingest/ingestion/reconcilers/carta.py#L80-L122)):

1. Load the shard's `high_water_updated` from its persisted fetch cursor
   ([reconcilers/carta.py:69‑77](../../../services/ingest/ingestion/reconcilers/carta.py#L69-L77)); skip if `None`.
2. Issue a **cheap 1-row probe** — `query(entity_type, where="Metadata.LastUpdatedTime
   > '<high_water>'", max_results=1)`
   ([reconcilers/carta.py:94‑100](../../../services/ingest/ingestion/reconcilers/carta.py#L94-L100)).
3. If any row comes back, reshare a `carta_entity` shard at
   **`recency_score=1.5`**, warm-started at the high-water (`updated_cursor` +
   `gap_baseline_updated`) so the re-walk runs in incremental mode
   ([reconcilers/carta.py:108‑122](../../../services/ingest/ingestion/reconcilers/carta.py#L108-L122)).

A probe error is logged and treated as "no gap" (best-effort)
([reconcilers/carta.py:101‑106](../../../services/ingest/ingestion/reconcilers/carta.py#L101-L106)). The design
is deliberately **over-reshare-safe**: it can re-walk entities that did not
change, but `external_id` parity (versioned by `SyncToken`) makes re-walks
idempotent, so it never under-reshares
([reconcilers/carta.py:9‑13](../../../services/ingest/ingestion/reconcilers/carta.py#L9-L13)). The reconciler
is registered and its pool provider is wired at service startup
([reconciler.py:804](../../../services/ingest/ingestion/workflows/reconciler.py#L804)).

---

## 9. Revocation / recoverable-error behavior

**There is no revocation chokepoint and no recoverable-401 recovery for Carta**
(in contrast to GitHub's `_maybe_disable_on_revocation` or Notion's recoverable-401
+ re-enable seam).

- A `401`/`403` maps to `CartaApiError(code="carta_api_unauthorized")` with a
  message noting the token "may need refresh", but **nothing acts on it** — there
  is no install-disable and no re-mint
  ([client.py:207‑208](../../../services/ingest/integrations/carta/client.py#L207-L208),
  [276‑283](../../../services/ingest/integrations/carta/client.py#L276-L283)). In the fetcher, a 401 is **not**
  one of the soft-paused codes (only `carta_api_rate_limited` is), so it
  **propagates and fails the shard**
  ([fetchers/carta.py:159‑168](../../../services/ingest/ingestion/fetchers/carta.py#L159-L168)).
- The only place a credential failure is handled gracefully is the **connect
  wizard**, which returns a structured `400` and asks the operator to refresh the
  token in their Carta OAuth app
  ([oauth.py:113‑134](../../../services/ingest/integrations/carta/oauth.py#L113-L134)).

The remediation path today is **operator re-submission via the connect
finalize**: paste a fresh access token, which UPSERTs the install
(`disabled_at = NULL`, refreshed `secret_ref`) and re-fires the backfill trigger
([onboarding.py:66‑78](../../../services/ingest/integrations/carta/onboarding.py#L66-L78)). The automated
re-mint-on-401 seam is the §2.1 `TODO(human)`.

---

## 10. End-to-end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  OAuth2 Bearer access token (resolved once from secret_ref)      │
                          │     firm_id scopes every call: /v1/firms/{firm_id}/...            │
   ACTIVE ENTITY TYPES    │  planner: read carta_entities from DB (source_client = None)      │
   shareholder/shareclass │     └─► one carta_entity shard per (firm, entity_type)            │
   safenote/optiongrant   │  fetcher: GET /v1/firms/{firm}/query (SELECT * FROM <Entity> …)   │
                          │     FULL or INCREMENTAL (LastUpdatedTime > high_water)            │
                          │     └─► tag {_fyralis_record_type, _fyralis_firm_id, entity}      │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────── LIVE (POLL — no webhook, NOT in VERIFIERS) ────┐│
   cap-table change ──────►  interval re-list → build_change_record (SAME tagged shape)    ││
                          │     kafka_path_enabled? → raw.carta ingress_kind="poll"          ││
                          │       else → core.ingest("carta:object", record)                 ││
                          │     trust boundary = OAuth poll connection (NO HMAC, no HTTP)    ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_carta_object             │
                                                            │  branch on _fyralis_record_type  │
                                                            │  external_id =                   │
                                                            │   carta:{firm}:{kind}:{id}:{sync}│
                                                            │  Status → state_change | signal  │
                                                            │  trust = authoritative           │
                                                            │  → ObservationDraft               │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill and live poll both emit the
   identical `_fyralis_record_type`-tagged record, so `carta:object` treats them
   identically. A backfilled object and its live-poll twin dedup to one
   observation via `external_id`.
2. **`external_id` is versioned and discriminated** —
   `carta:{firm_id}:{entity_kind}:{entity_id}:{sync_token}`. `SyncToken`
   versioning makes a cap-table mutation a **new** observation; `entity_kind`
   stops different entity types sharing an id from colliding.
3. **One credential model, no refresh grant.** A single OAuth2 Bearer access
   token (re-minted hourly in principle, but the re-mint loop is unbuilt) reads
   everything, scoped by `firm_id`. No webhook secret.
4. **The live edge is a poll, not a webhook.** No HMAC verification, no HTTP
   status; Carta is **not** in the webhook `VERIFIERS` map and registers no
   `provider_installations` row.
5. **One shard per entity type.** The planner emits one `carta_entity` shard per
   active type; the fetcher pages it (FULL then INCREMENTAL via the
   `LastUpdatedTime` high-water cursor).
6. **Over-reshare-safe reconciliation.** A cheap 1-row probe per entity type can
   over-reshare but never under-reshares; `external_id` parity makes re-walks
   idempotent.

---

## 11. Configuration & compliance

> **Compliance caveat.** Unlike the GitHub/Slack docs, the Carta read surface is
> **not yet verified against Carta's official API**. The `CONFIRMED` notes below
> are the code's own annotations (`docs.carta.com/api-platform`); the `query`
> endpoint, query language, and offset pagination are a Gusto/QuickBooks
> **placeholder** pending the §3 `TODO(human)`s.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `CARTA_API_BASE_URL` | resolver default `https://api.carta.com` | per-env API host override ([endpoints.py:138](../../../lib/integrations/endpoints.py#L138)) |
| `CARTA_BACKFILL_PAGE_SIZE` | `100` (capped at 1000) | `MAXRESULTS` / page size per query ([fetchers/carta.py:62‑66](../../../services/ingest/ingestion/fetchers/carta.py#L62-L66)) |
| `CARTA_RL_MAX_ATTEMPTS` | `4` | `429`-retry budget ([client.py:171](../../../services/ingest/integrations/carta/client.py#L171)) |
| `CARTA_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:172](../../../services/ingest/integrations/carta/client.py#L172)) |

Per-install `base_url` (stored on `carta_installations`) is also honoured, and
wins over the env default in production
([client.py:120‑121](../../../services/ingest/integrations/carta/client.py#L120-L121)).

### 11.2 Status checklist

- **Auth** — OAuth2 Bearer access token, scoped by `firm_id`, never logged. ✅
- **Token refresh / re-mint** — Carta has no refresh grant; the re-mint-on-401
  loop is **not built**. ❌ (`TODO(human)`, §2.1)
- **No webhook** — Carta is poll-only; live edge is `ingress_kind="poll"`; not in
  `VERIFIERS`; no `webhook_secret_ref`. ✅
- **Rate limits** — server-driven `429` + `Retry-After` (no dedicated bucket). ✅
- **Read surface (endpoint + query language + pagination)** — Gusto/QuickBooks
  **placeholder**, unconfirmed against Carta. ❌ (`TODO(human)`, §3 / §5)
- **Partner access** — invite-only + SOC 2; prod host/scopes not yet entitled. ❌
  (`TODO(human)`, §2.1)
- **Tenant isolation** — `carta_installations` / `carta_entities` ENABLE + FORCE
  RLS on `app.current_tenant` ([0104_carta.sql:108‑132](../../../db/migrations/0104_carta.sql#L108-L132)). ✅

### 11.3 Dev / Provider Lab mode

For local testing against the mock source servers, `build_carta_client` detects
Provider Lab mode and **preseeds** the access token with `spam-carta`, skipping any
real secret-store resolution, and points the API base at Provider Lab's
`/carta` sub-path via the endpoint resolver
([_clients.py:622‑649](../../../services/ingest/ingestion/fetchers/_clients.py#L622-L649),
[endpoints.py:168](../../../lib/integrations/endpoints.py#L168)). The synthetic
live path is driven by `CartaPollGenerator`, which calls
`integrations.carta.poll.handle_polled_change` in-process — exercising the same
`shadow_write_raw(source="carta", ingress_kind="poll")` → `ingestion.raw.carta`
→ normalizer → observation_writer chain as production
([live_generators/carta_poll.py:1‑27](../../../services/ingest/synthetic/live_generators/carta_poll.py#L1-L27)).
