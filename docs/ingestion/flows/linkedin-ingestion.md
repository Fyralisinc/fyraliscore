# LinkedIn Ingestion — How Fyralis Pulls LinkedIn Data

This document explains, in detail, **how LinkedIn data enters Fyralis**: which
LinkedIn REST surface is called, with which token, and how the LinkedIn
organization signal set — **shares, social actions, and follower statistics** —
is each ingested.

It deliberately stops at the point where a LinkedIn object becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Heads-up: this is a partner-gated, archetype-cloned source.** LinkedIn's
> organization/recruiting data (Marketing Developer Platform / Talent Solutions)
> is **invite-only**. The integration package was cloned **wholesale from the
> Carta OAuth2 archetype** (itself a Gusto/QuickBooks clone), and the read
> surface, pagination shape, and OAuth refresh are **placeholders** flagged with
> `TODO(human)` markers throughout the code. Where the real LinkedIn behaviour is
> unverified, this doc reproduces those markers rather than inventing facts. The
> *control-flow* (how a record becomes an observation, dedup, poll-only live edge,
> reconciliation) is real and verified; the *transport details* (exact endpoints,
> field names, scopes) are not. Such claims are labelled **(inferred)** or carry a
> `TODO(human)` callout.

---

## 1. The two ways data arrives

LinkedIn data reaches Fyralis through **two independent paths that converge on
one handler** — but, unlike Slack and GitHub, **neither path is an inbound
webhook**. LinkedIn is **POLL-ONLY**: there is no webhook edge at all.

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* history via the LinkedIn **REST API** (per entity type, offset-paginated) | [`planners/linkedin.py`](../../../services/ingest/ingestion/planners/linkedin.py), [`fetchers/linkedin.py`](../../../services/ingest/ingestion/fetchers/linkedin.py) |
| **Live (poll, not webhook)** | A poll cycle detects a changed object | Fyralis *re-lists* changed objects on an interval and dispatches each change through the pipeline under `ingress_kind="poll"` | [`integrations/linkedin/poll.py`](../../../services/ingest/integrations/linkedin/poll.py) |

There is **no `services/app/webhooks` verifier for LinkedIn**, and LinkedIn is
deliberately **absent from the `VERIFIERS` registry**
([signatures/\_\_init\_\_.py:63‑68](../../../services/app/webhooks/signatures/__init__.py#L63-L68)):

> ```
> # People/Recruiting: HiBob (...) + Ashby (...). LinkedIn is poll-only
> # (no webhook) so it has NO verifier here.
> ```

The poll dispatcher emits the **same fetcher-shaped record** the backfill fetcher
emits, so both paths are parsed by the **single** `linkedin:object` handler
([handlers/linkedin.py](../../../services/ingest/ingestion/handlers/linkedin.py),
registered at [handlers/linkedin.py:246](../../../services/ingest/ingestion/handlers/linkedin.py#L246))
and both derive the **same** dedup key:

```
external_id = "linkedin:{organization_urn}:{entity_kind}:{entity_id}"
              entity_kind ∈ {share, social_action, follower_stat}
```

The `external_id` is **discriminated by `entity_kind`, NOT versioned by a sync
token** — the observations repo dedups on `(source_channel, external_id)`
*ignoring* `occurred_at`. LinkedIn organization objects are append/stat-shaped (a
share is published once; a follower-stat snapshot has a window-keyed id), so a
fresh id per object is the natural dedup key, and the `entity_kind` discriminator
keeps multi-entity fixtures that happen to share an `id` from ever colliding
([handlers/linkedin.py:17‑27](../../../services/ingest/ingestion/handlers/linkedin.py#L17-L27),
[60‑74](../../../services/ingest/ingestion/handlers/linkedin.py#L60-L74)).

So an object that is both backfilled *and* re-listed by a live poll collapses
into **one** observation. This is the central design invariant of LinkedIn
ingestion — the poll dispatcher exists precisely to **re-use the backfill record
shape exactly** so the one handler treats both identically
([poll.py:68‑88](../../../services/ingest/integrations/linkedin/poll.py#L68-L88)).

> **There is genuinely no live-webhook path to document.** The recruitment APIs
> are partner-gated in production with **no webhook entitlement**, so the live
> edge is a poll. Do not look for an HMAC verifier, a signature secret, or a
> `/webhooks/linkedin` route — none exist, by design.

> **TODO(human): move the `external_id` constructor to its canonical home.**
> `linkedin_entity(...)` lives in the handler today, but the code carries a
> standing note that during the wiring phase it should move to
> `services/ingest/ingestion/idempotency/__init__.py` (mirroring `carta_entity`)
> — and the format string **must stay byte-identical** across the move
> ([handlers/linkedin.py:67‑73](../../../services/ingest/ingestion/handlers/linkedin.py#L67-L73)).

---

## 2. Authentication & token model

LinkedIn authenticates with **OAuth 2.0**: a short-lived **Bearer access token**
plus a rotating **refresh token**, with every call scoped to an
`organization_urn` (the scope-id, analogous to Carta's `firm_id` / Gusto's
`company_uuid` / QuickBooks' `realmId`)
([client.py:1‑8](../../../services/ingest/integrations/linkedin/client.py#L1-L8)).
The access token is resolved **once** from the secret store (or preset in spammer
mode) and reused for the life of the client
([client.py:142‑164](../../../services/ingest/integrations/linkedin/client.py#L142-L164)).

### 2.1 What is verified vs. unverified

| Aspect | Status |
|--------|--------|
| Auth model is **OAuth 2.0 Bearer** | **Verified** — `Authorization: Bearer {token}` on every call ([client.py:171‑179](../../../services/ingest/integrations/linkedin/client.py#L171-L179)) |
| Token **refresh** | **Not implemented** — the seam exists but no refresh exchange or `oauth_poller` is wired (see callout below) |
| OAuth **scopes** | **Unverified** — candidates only |
| Standard OAuth **bounce** (authorize → callback → code exchange) | **Deliberately absent** — install is operator-mediated credential submission (§2.3) |

> **TODO(human): implement LinkedIn OAuth token refresh — NONE exists yet.** The
> install row persists `refresh_secret_ref` + `token_expires_at`, but no
> refresh-on-401 exchange (nor an `oauth_poller`) is built — this is the
> documented-but-unbuilt seam the Carta/Gusto/QuickBooks archetype ships with.
> LinkedIn access tokens are ~60 days and refresh tokens ~1 year (confirm against
> the partner entitlement); the client must **not** assume tokens never expire
> ([client.py:10‑16](../../../services/ingest/integrations/linkedin/client.py#L10-L16)).

> **TODO(human): confirm the exact OAuth scopes.** The organization read scopes
> are **not verified**; candidates noted in code are `r_organization_social`,
> `rw_organization_admin`, `r_organization_followers`, `r_basicprofile`
> ([oauth.py:12‑20](../../../services/ingest/integrations/linkedin/oauth.py#L12-L20),
> [\_\_init\_\_.py:18‑22](../../../services/ingest/integrations/linkedin/__init__.py#L18-L22)).

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| Access token (Bearer) | `encrypted_secrets`, pointed to by `linkedin_installations.secret_ref` | label `linkedin_access_token:{organization_urn}` ([oauth.py:200‑204](../../../services/ingest/integrations/linkedin/oauth.py#L200-L204)) |
| Refresh token (rotating) | `encrypted_secrets`, pointed to by `linkedin_installations.refresh_secret_ref` | label `linkedin_refresh_token:{organization_urn}`; **owned by the (unbuilt) oauth_poller** ([oauth.py:205‑211](../../../services/ingest/integrations/linkedin/oauth.py#L205-L211), [0107_linkedin.sql:64‑70](../../../db/migrations/0107_linkedin.sql#L64-L70)) |
| `organization_urn` | `linkedin_installations.organization_urn` | the scope-id on every call **and** the poll tenant-resolution key |
| Webhook secret | **none** | LinkedIn is poll-only — the migration explicitly has **NO `webhook_secret_ref` column** ([0107_linkedin.sql:71](../../../db/migrations/0107_linkedin.sql#L71)) and registers **no `provider_installations` row** ([oauth.py:22‑26](../../../services/ingest/integrations/linkedin/oauth.py#L22-L26)) |

The access token / `Authorization` header are **never logged**
([client.py:32](../../../services/ingest/integrations/linkedin/client.py#L32)),
and `LinkedinApiError` keeps the token off its `context`
([client.py:56‑62](../../../services/ingest/integrations/linkedin/client.py#L56-L62)).

### 2.3 The install flow — operator-mediated credential submission

Because the OAuth bounce is not implemented, the production install surface is an
**admin connect wizard** that takes pasted credentials and verifies them against
the **real LinkedIn API** before seeding the install
([oauth.py:1‑43](../../../services/ingest/integrations/linkedin/oauth.py#L1-L43)):

1. **`POST /integrations/linkedin/connect/preflight`** (Bearer-authed) — body
   `{organization_urn, access_token, base_url?}`. Builds a throwaway
   `LinkedinClient` and calls `org_info()` to verify the token + organization. An
   auth/connectivity failure returns a structured `400` (`linkedin_auth_failed` /
   `linkedin_api_error`) and **no secret is stored**
   ([oauth.py:134‑161](../../../services/ingest/integrations/linkedin/oauth.py#L134-L161)).
2. **`POST /integrations/linkedin/connect/finalize`** (Bearer-authed) — body adds
   `refresh_token?`, `entities?`, `token_expires_at?`. It **re-verifies the creds
   before any write** ([oauth.py:186‑197](../../../services/ingest/integrations/linkedin/oauth.py#L186-L197)),
   then stores the access token (and refresh token, if given) in the secret store
   and calls `finalize_install`
   ([oauth.py:199‑223](../../../services/ingest/integrations/linkedin/oauth.py#L199-L223)).
3. `finalize_install` ([onboarding.py:37‑112](../../../services/ingest/integrations/linkedin/onboarding.py#L37-L112))
   does **one tenant-scoped transaction**: UPSERT `linkedin_installations`
   (idempotent on `(tenant_id, organization_urn)`, clearing `disabled_at`), INSERT
   one `linkedin_entities` row per entity type (default `share`, `social_action`,
   `follower_stat`), and emit an `onboarding_triggers` row (`source='linkedin'`,
   `trigger_kind='install'`) so the existing M6 backfill chain fires.
4. There is **no webhook registration** — no verifier token is accepted and no
   `provider_installations` row is created (§2.2).

---

## 3. The LinkedIn API surface that is actually called

All read calls funnel through `LinkedinClient._request`
([client.py:166‑220](../../../services/ingest/integrations/linkedin/client.py#L166-L220)),
which:

- sets `Authorization: Bearer {token}` and `Accept: application/json`,
- honours `Retry-After` on **`429`** within a bounded budget
  (`LINKEDIN_RL_MAX_ATTEMPTS`=4, `LINKEDIN_RL_MAX_SLEEP_SEC`=30),
- maps transport errors and any non-2xx to `LinkedinApiError` with a stable
  `code` (`linkedin_api_unauthorized` / `_not_found` / `_rate_limited` /
  `_error`) ([client.py:293‑324](../../../services/ingest/integrations/linkedin/client.py#L293-L324)).

The two endpoints invoked:

| Endpoint (placeholder shape) | Wrapper | Purpose | Code |
|------------------------------|---------|---------|------|
| `GET /v1/organizations/{org}/query?query=SELECT…&minorversion=1` | `LinkedinClient.query()` | one page of one entity type's rows | [client.py:226‑265](../../../services/ingest/integrations/linkedin/client.py#L226-L265) |
| `GET /v1/organizations/{org}/orginfo/{org}` | `LinkedinClient.org_info()` | connectivity / token-verify probe (used by preflight + reconciler) | [client.py:267‑279](../../../services/ingest/integrations/linkedin/client.py#L267-L279) |

> **TODO(human): confirm LinkedIn's real read surface, pagination, host paths, and
> protocol headers.** The `query()` / `org_info()` shapes above are **cloned from
> the Carta/Gusto/QuickBooks query-language placeholder** (`SELECT * FROM <Entity>
> [WHERE …] ORDERBY <f> STARTPOSITION n MAXRESULTS m`). LinkedIn's real REST
> surface is page/cursor-based collections scoped by an `organization` URN query
> param — shares/posts (`/rest/posts?q=author`),
> `organizationalEntityShareStatistics` / `socialActions`, and
> `organizationFollowerStatistics` — paginated via start/count or an opaque page
> token, under `/rest/...` or `/v2/...`
> ([client.py:18‑27](../../../services/ingest/integrations/linkedin/client.py#L18-L27),
> [238‑248](../../../services/ingest/integrations/linkedin/client.py#L238-L248)).
> It is also unconfirmed whether the entitled surface requires the versioned
> protocol headers `X-Restli-Protocol-Version: 2.0.0` and a dated `LinkedIn-Version`
> ([client.py:176‑178](../../../services/ingest/integrations/linkedin/client.py#L176-L178)).
> The host **`https://api.linkedin.com` is confirmed**; the REST base path is not
> ([endpoints.py:106‑112](../../../lib/integrations/endpoints.py#L106-L112)).

### 3.1 Pagination — offset (`STARTPOSITION`), placeholder shape

`query()` pages by 1-based offset: it requests `STARTPOSITION n MAXRESULTS m`,
returns `(rows, next_start_position)`, and treats a **short page** (`returned <
max_results` or empty) as terminal (`next_start_position is None`)
([client.py:261‑265](../../../services/ingest/integrations/linkedin/client.py#L261-L265)).
Page size is env-overridable via `LINKEDIN_BACKFILL_PAGE_SIZE` (default 100,
capped at 1000) ([fetchers/linkedin.py:58‑62](../../../services/ingest/ingestion/fetchers/linkedin.py#L58-L62)).
This offset model is **(inferred)** to be wrong for real LinkedIn (see the §3
`TODO(human)`); it is the Carta placeholder.

### 3.2 Rate limits — **no dedicated client-side bucket**

Unlike Slack (`SLACK_API_TIER`) and GitHub (a per-app token bucket), LinkedIn has
**no entry in `services/ingest/ingestion/rate_limit/buckets.py`** — there is no
declared client-side token bucket for it. Rate limiting is handled **reactively
only**: the client honours `429 Retry-After` within the
`LINKEDIN_RL_MAX_ATTEMPTS` / `LINKEDIN_RL_MAX_SLEEP_SEC` budget
([client.py:199‑203](../../../services/ingest/integrations/linkedin/client.py#L199-L203)),
and the backfill fetcher converts a `linkedin_api_rate_limited` exception into a
**non-terminal empty page** so the shard retries later rather than failing
([fetchers/linkedin.py:155‑164](../../../services/ingest/ingestion/fetchers/linkedin.py#L155-L164)).

---

## 4. Backfill scope — the shard families

The planner decomposes one install into **one shard per active entity type**, all
of `shard_kind = "linkedin_entity"`
([planners/linkedin.py:56‑88](../../../services/ingest/ingestion/planners/linkedin.py#L56-L88)).
There is exactly **one shard family**, parameterised by `entity_type`.

The active entity types are read from **DB state** — `ctx.source_client` is
`None`; entities come from the `linkedin_entities` child table, JSON-aggregated
into `ctx.install["entities"]` by the SourceOnboarding loader
([planners/linkedin.py:21](../../../services/ingest/ingestion/planners/linkedin.py#L21),
[source_onboarding.py:700‑715](../../../services/ingest/ingestion/workflows/source_onboarding.py#L700-L715)).
The seeded set is `share`, `social_action`, `follower_stat`
([client.py:327‑331](../../../services/ingest/integrations/linkedin/client.py#L327-L331)).

Each shard carries `entity_type`, `organization_urn`, `installation_id`, and the
warm-start `updated_cursor` (the per-entity high-water `LastUpdatedTime`, `None`
on first sync), at a baseline `recency_score=1.0`
([planners/linkedin.py:70‑82](../../../services/ingest/ingestion/planners/linkedin.py#L70-L82)).

> **TODO(human): confirm the LinkedIn resource taxonomy to shard.** The planner is
> entity-type-agnostic (it shards whatever active entities the DB lists), but the
> seeded set `{share, social_action, follower_stat}` is the placeholder. **Access
> is partner-gated** — confirm the high-signal entity set against the approved
> entitlement and add others as their read surface is confirmed
> ([planners/linkedin.py:13‑19](../../../services/ingest/ingestion/planners/linkedin.py#L13-L19)).

---

## 5. The backfill/poll fetcher — one shard kind, two sync modes

`fetch_page_linkedin` ([fetchers/linkedin.py:122‑191](../../../services/ingest/ingestion/fetchers/linkedin.py#L122-L191))
takes one `(install, shard_identifier, cursor)` triple, fetches one page, advances
the cursor, and emits each row as a tagged record. The same shard kind runs in two
modes ([fetchers/linkedin.py:8‑19](../../../services/ingest/ingestion/fetchers/linkedin.py#L8-L19)):

- **FULL (initial backfill):** `SELECT * FROM <Entity> ORDERBY
  Metadata.LastUpdatedTime STARTPOSITION n MAXRESULTS m`, offset-paginated.
- **INCREMENTAL (poll/reconcile reshare):** when warm-started with an
  `updated_cursor`, the `WHERE` clause adds `Metadata.LastUpdatedTime >
  '<cursor>'` so only changed entities come back.

### 5.1 Cursor — per-entity updated high-water

```python
class LinkedinCursor:
    start_position: int = 1            # 1-based STARTPOSITION offset within this run
    high_water_updated: str | None     # max Metadata.LastUpdatedTime seen — incremental
                                       #   lower bound AND the reconciler's gap reference
    incremental_floor: str | None      # the `LastUpdatedTime >` bound frozen for this run
    rows_seen: int = 0                 # diagnostic
    seeded: bool = False               # whether first-call setup ran
```

([fetchers/linkedin.py:65‑85](../../../services/ingest/ingestion/fetchers/linkedin.py#L65-L85)).
On the first call the fetcher seeds `incremental_floor` and `high_water_updated`
from the shard's warm `updated_cursor` (if any)
([fetchers/linkedin.py:133‑138](../../../services/ingest/ingestion/fetchers/linkedin.py#L133-L138)),
then bumps `high_water_updated` over every row's `Metadata.LastUpdatedTime`
([fetchers/linkedin.py:111‑116](../../../services/ingest/ingestion/fetchers/linkedin.py#L111-L116),
[173](../../../services/ingest/ingestion/fetchers/linkedin.py#L173)).

### 5.2 The tagged record (the cross-path dedup contract)

Each entity row is emitted as **one record** tagged with the private
`_fyralis_record_type` (the entity type, lowercased) plus `_fyralis_org_urn`
([fetchers/linkedin.py:166‑173](../../../services/ingest/ingestion/fetchers/linkedin.py#L166-L173)):

```python
records.append({
    "_fyralis_record_type": entity_type.lower(),   # share | social_action | follower_stat
    "_fyralis_org_urn": organization_urn,
    "entity": row,                                 # the full LinkedIn object
})
```

The poll dispatcher builds the **identical shape** (§7), so a polled change and
its backfill twin produce the same `external_id` and dedup.

> **TODO(human): confirm the real list/pagination shape + the "updated since"
> filter field name.** The `STARTPOSITION`/offset model and the
> `Metadata.LastUpdatedTime >` filter are the Carta/Gusto placeholder; wire
> against the approved prod host once entitled
> ([fetchers/linkedin.py:29‑36](../../../services/ingest/ingestion/fetchers/linkedin.py#L29-L36)).

---

## 6. The handler — shaping records into `ObservationDraft`

`handle_linkedin_object` ([handlers/linkedin.py:246‑270](../../../services/ingest/ingestion/handlers/linkedin.py#L246-L270))
is a **pure function** (no DB / network). It branches on the input shape and
produces **exactly one** observation per call. Both backfill and poll arrive
tagged with `_fyralis_record_type`, so there is **one branch for both** — there is
**no webhook-envelope branch** because LinkedIn has no webhook
([handlers/linkedin.py:1‑27](../../../services/ingest/ingestion/handlers/linkedin.py#L1-L27)).
An untagged payload is rejected with a `ValidationError`
([handlers/linkedin.py:267‑270](../../../services/ingest/ingestion/handlers/linkedin.py#L267-L270)).

The shaper `_entity_draft` ([handlers/linkedin.py:173‑232](../../../services/ingest/ingestion/handlers/linkedin.py#L173-L232))
derives every field. The **`kind` branches on the object's `Status`**: a
lifecycle-transition status (`deleted` / `removed` / `archived` / `edited` /
`expired`) yields `kind=state_change`; anything else is an open `kind=signal`
([handlers/linkedin.py:55‑57](../../../services/ingest/ingestion/handlers/linkedin.py#L55-L57),
[131‑140](../../../services/ingest/ingestion/handlers/linkedin.py#L131-L140)).

| Record (`_fyralis_record_type`) | `external_id` | `occurred_at` | `kind` | Trust tier |
|---------------------------------|---------------|---------------|--------|------------|
| `share` | `linkedin:{org}:share:{Id}` | `Metadata.LastUpdatedTime`, else now | `state_change` if `Status` ∈ lifecycle set, else `signal` | **authoritative** |
| `social_action` | `linkedin:{org}:social_action:{Id}` | ″ | ″ | **authoritative** |
| `follower_stat` | `linkedin:{org}:follower_stat:{Id}` | ″ | ″ | **authoritative** |

Other derived fields:

- **`source_actor_ref`** is `linkedin:member:{value}` from the object's
  `AuthorRef` / `MemberRef`, else `None`
  ([handlers/linkedin.py:108‑121](../../../services/ingest/ingestion/handlers/linkedin.py#L108-L121)).
- **`content_text`** is a compact label like `"Share UGC-12 · <author> · 500 ·
  active"` (label · author · impression/like/follower count · status)
  ([handlers/linkedin.py:124‑128](../../../services/ingest/ingestion/handlers/linkedin.py#L124-L128),
  [188‑201](../../../services/ingest/ingestion/handlers/linkedin.py#L188-L201)).
- **`entities_hint`** carries a `linkedin_object` ref (`{entity_kind}:{entity_id}`)
  plus the author `person` hint
  ([handlers/linkedin.py:203‑207](../../../services/ingest/ingestion/handlers/linkedin.py#L203-L207)).
- **Trust posture:** LinkedIn is the organization system of record for its own
  shares/stats → the channel `linkedin:object` maps to **`authoritative`** in
  `CHANNEL_TRUST_MAP` ([handlers/\_\_init\_\_.py:68](../../../services/ingest/ingestion/handlers/__init__.py#L68);
  the handler also `setdefault`s it at [handlers/linkedin.py:273](../../../services/ingest/ingestion/handlers/linkedin.py#L273)).

> **TODO(human): confirm the real LinkedIn organization field names.** The
> `_entity_extras` map (`LikeCount`/`CommentCount`/`ShareCount`/`FollowerCount`/…)
> reflects the placeholder fixture shape; the entitled REST surface exposes
> `likeCount`/`commentCount`/`shareCount` under `socialActions` and
> `organicFollowerCount`/`paidFollowerCount` under `followerStatistics`
> ([handlers/linkedin.py:143‑151](../../../services/ingest/ingestion/handlers/linkedin.py#L143-L151)).

---

## 7. Live ingestion — the POLL edge (no webhook)

LinkedIn's live edge is a **poller**, not a webhook
([poll.py:1‑25](../../../services/ingest/integrations/linkedin/poll.py#L1-L25)).
A poller holds **one organization's install = one tenant's install**, so the
tenant is known by construction (carried on `PollDeps`) — there is **no
per-change tenant resolution** ([poll.py:47‑66](../../../services/ingest/integrations/linkedin/poll.py#L47-L66)).

`handle_polled_change` ([poll.py:144‑185](../../../services/ingest/integrations/linkedin/poll.py#L144-L185)):

1. **Build the canonical change record.** `build_change_record` takes
   `{"entity_type": …, "entity": {…}}` and produces the **same**
   `_fyralis_record_type`-tagged shape the backfill fetcher emits → identical
   `external_id` → cross-path dedup
   ([poll.py:68‑88](../../../services/ingest/integrations/linkedin/poll.py#L68-L88)).
2. **Cutover branch (kafka-first default).** If `ingestion.kafka_path_enabled` for
   the tenant *and* both the kafka producer and S3 raw client are wired,
   shadow-write the record to `ingestion.raw.linkedin` with
   **`ingress_kind="poll"`** and return; the normalizer + observation_writer then
   produce the observation, concurrently with any in-flight backfill
   ([poll.py:106‑163](../../../services/ingest/integrations/linkedin/poll.py#L106-L163)).
3. **Inline fallback.** Otherwise (or if the cutover publish fails) call
   `core.ingest("linkedin:object", record, …)` directly, then a best-effort M2
   shadow audit when `SHADOW_WRITE_ENABLED`
   ([poll.py:165‑185](../../../services/ingest/integrations/linkedin/poll.py#L165-L185)).

There is **no HMAC gate and no HTTP status** — the trust boundary is the
authenticated OAuth poll connection itself
([poll.py:23‑25](../../../services/ingest/integrations/linkedin/poll.py#L23-L25)).
The `("linkedin", "poll")` → `linkedin:object` channel mapping is declared
alongside `("linkedin", "backfill")`
([channel_mapping.py:307‑318](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L307-L318)).

---

## 8. Reconciliation — gap detection

`reconcile_linkedin` ([reconcilers/linkedin.py:124‑162](../../../services/ingest/ingestion/reconcilers/linkedin.py#L124-L162))
re-checks completed entity shards for new activity. For each `done` shard it loads
the persisted cursor's `high_water_updated`, then probes the **live** organization
with a **cheap 1-row incremental query**
(`Metadata.LastUpdatedTime > '<high_water>'`, `max_results=1`)
([reconcilers/linkedin.py:79‑105](../../../services/ingest/ingestion/reconcilers/linkedin.py#L79-L105)).
If a row comes back, it reshares a `linkedin_entity` shard at
**`recency_score=1.5`**, warm-started at the high-water (incremental mode)
([reconcilers/linkedin.py:110‑121](../../../services/ingest/ingestion/reconcilers/linkedin.py#L110-L121)).

`external_id` parity (discriminated by `entity_kind`) means re-walked entities
**dedup against what backfill already wrote** — only genuinely new entities
produce new observations. The reconciler is pragmatic-v1: it can over-reshare but
never under-reshares, and dedup makes re-walks idempotent
([reconcilers/linkedin.py:1‑13](../../../services/ingest/ingestion/reconcilers/linkedin.py#L1-L13)).
A probe error is logged and skipped (best-effort), not raised
([reconcilers/linkedin.py:100‑105](../../../services/ingest/ingestion/reconcilers/linkedin.py#L100-L105)).
The reconciler is wired with a pool provider at service startup
([workflows/reconciler.py:782‑807](../../../services/ingest/ingestion/workflows/reconciler.py#L782-L807)).

---

## 9. Revocation chokepoint — **not implemented**

Unlike GitHub (which disables an installation on documented `401`/`404`
revocation signals) and Notion (which has a revocation chokepoint with a
recoverable-401 path), **LinkedIn has no revocation chokepoint**. A `401`/`403`
surfaces as `LinkedinApiError(code="linkedin_api_unauthorized")`
([client.py:216‑217](../../../services/ingest/integrations/linkedin/client.py#L216-L217),
[297‑303](../../../services/ingest/integrations/linkedin/client.py#L297-L303))
and propagates — there is no code path that flips `linkedin_installations.disabled_at`
on an auth failure during fetch.

This is a direct consequence of the missing token-refresh seam (§2.1): the
archetype's intent is that an expired token is **recovered by refresh**, not by
disabling the install — but neither the refresh exchange nor a disable-on-revocation
chokepoint is built yet.

> **TODO(human): decide and wire LinkedIn's auth-failure behaviour.** Either
> (a) a refresh-on-401 loop in the client (exchange refresh token → persist
> rotated token → retry once), or (b) an `oauth_poller` + a disable chokepoint —
> the `disabled_at` column and `refresh_secret_ref`/`token_expires_at` plumbing
> already exist for it ([client.py:10‑16](../../../services/ingest/integrations/linkedin/client.py#L10-L16),
> [0107_linkedin.sql:64‑73](../../../db/migrations/0107_linkedin.sql#L64-L73)).
> The connect wizard's user-facing copy already tells the operator to refresh and
> re-submit the token on auth failure
> ([oauth.py:120‑128](../../../services/ingest/integrations/linkedin/oauth.py#L120-L128)).

---

## 10. End-to-end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  OAuth Bearer access token (from secret_ref; no refresh yet)     │
   ONE ORGANIZATION       │  planner: read active entities from linkedin_entities (DB state) │
   (organization_urn)     │     └─► one linkedin_entity shard per entity_type                │
   share/social_action/   │  fetcher: GET /…/query  SELECT * FROM <Entity> STARTPOSITION n    │
   follower_stat          │     └─► tag each row {_fyralis_record_type, _fyralis_org_urn, …}  │
                          │     FULL first run; INCREMENTAL = WHERE LastUpdatedTime > cursor  │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────── LIVE (POLL — no webhook!) ────────────────────┐│
   poll cycle detects ────►  re-list changed objects on an interval                       ││
   a changed object       │     build_change_record → SAME tagged shape as backfill        ││
                          │     cutover → shadow_write_raw(source=linkedin,                ││
                          │                                ingress_kind="poll") OR inline   ││
                          │     NO HMAC, NO HTTP status — trust = authenticated OAuth poll  ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_linkedin_object          │
                                                            │  (one channel: linkedin:object)  │
                                                            │  branch on _fyralis_record_type  │
                                                            │  external_id =                   │
                                                            │    linkedin:{org}:{kind}:{id}    │
                                                            │  kind: state_change|signal       │
                                                            │  trust: authoritative            │
                                                            │  → ObservationDraft              │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill and the live poll both produce
   the same `_fyralis_record_type`-tagged record, so `linkedin:object` treats them
   identically. A backfilled object and its live-poll twin dedup to one
   observation via `external_id = "linkedin:{org}:{kind}:{id}"`, which is
   **discriminated by `entity_kind`, never versioned by a sync token**.
2. **Poll-only live edge.** LinkedIn is **not** in `VERIFIERS`, has **no
   webhook**, **no `webhook_secret_ref`**, and **no `provider_installations`
   row**. The live edge is a poller that re-runs the backfill record build under
   `ingress_kind="poll"`. This is forced by the **partner-gated** APIs (no webhook
   entitlement).
3. **One shard family, two sync modes.** `linkedin_entity` shards, one per entity
   type, page by offset in FULL mode and add a `LastUpdatedTime > high_water`
   filter in INCREMENTAL mode (warm-start / reconciler reshare).
4. **OAuth Bearer, single scope-id.** Every call is `Authorization: Bearer` scoped
   to `organization_urn`. The token is resolved once and reused; it is never
   logged.
5. **Archetype clone with unverified transport.** The control-flow is real; the
   endpoints, pagination, field names, scopes, token-refresh, and revocation
   chokepoint are **Carta-archetype placeholders** to be wired once the LinkedIn
   partner entitlement lands (see the `TODO(human)` callouts).

---

## 11. Configuration & compliance

> **Compliance status:** *not* verified against LinkedIn's official docs — the
> read surface is a Carta-archetype placeholder, and access is partner-gated. The
> checklist below records what the *code* guarantees, not LinkedIn-API conformance.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `LINKEDIN_API_BASE_URL` | `https://api.linkedin.com` (built-in) | API host override (highest precedence) ([endpoints.py:112](../../../lib/integrations/endpoints.py#L112), [141](../../../lib/integrations/endpoints.py#L141)) |
| `LINKEDIN_BACKFILL_PAGE_SIZE` | `100` (cap 1000) | per-page `MAXRESULTS` ([fetchers/linkedin.py:58‑62](../../../services/ingest/ingestion/fetchers/linkedin.py#L58-L62)) |
| `LINKEDIN_RL_MAX_ATTEMPTS` | `4` | `429` retry budget ([client.py:180](../../../services/ingest/integrations/linkedin/client.py#L180)) |
| `LINKEDIN_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:181](../../../services/ingest/integrations/linkedin/client.py#L181)) |
| `SYNTHETIC_SOURCE_API_BASE` (+ spammer mode) | — | single-host spammer base; LinkedIn sub-path `/linkedin` ([endpoints.py:171](../../../lib/integrations/endpoints.py#L171)) |

Per-install `base_url` (stored on the install row) also overrides the host for
that install.

### 11.2 What the code guarantees

- **OAuth Bearer auth** — `Authorization: Bearer {token}` on every call; token
  resolved once, never logged. ✅
- **Poll-only live edge** — no webhook verifier, no signature secret, no
  `provider_installations` row; live edge is `ingress_kind="poll"`. ✅
- **Cross-path dedup** — backfill + poll emit the identical tagged record →
  identical `entity_kind`-discriminated `external_id`. ✅
- **Reactive rate-limit handling** — `429 Retry-After` honoured within a bounded
  budget; rate-limit during backfill yields a non-terminal empty page (retry, not
  fail). ✅
- **Least secret surface** — access + refresh tokens in `encrypted_secrets`; **no
  webhook secret** column at all. ✅
- **Token refresh** — ❌ not implemented (`TODO(human)`, §2.1).
- **Revocation chokepoint** — ❌ not implemented (§9).
- **Verified API conformance (endpoints / pagination / scopes / protocol headers)**
  — ❌ partner-gated placeholders (`TODO(human)`, §3).

### 11.3 Dev / spammer mode

For local testing against the mock source servers, `build_linkedin_client` detects
spammer mode and **presets the access token to `spam-linkedin`**, skipping the
real secret-store resolution, and points the API base at the local spammer's
`/linkedin` sub-path via the endpoint resolver
([_clients.py:779‑809](../../../services/ingest/ingestion/fetchers/_clients.py#L779-L809),
[endpoints.py:171](../../../lib/integrations/endpoints.py#L171)).

- The backfill read seam is served by `MockLinkedinClient`, which implements only
  `query()` + `org_info()` with **offset pagination matching the real client
  exactly** and the same `LinkedinApiError` codes for fault injection
  ([synthetic/mock_clients/linkedin.py:1‑40](../../../services/ingest/synthetic/mock_clients/linkedin.py#L1-L40)).
- The live poll is driven in-process by `LinkedinPollGenerator`, which mints a
  fresh organization change (entity id ≥ 1,000,000, a 2026-06 timestamp) and
  dispatches it through the **production** `handle_polled_change` path, resolving
  the real `linkedin_installations.organization_urn` so the live `external_id` is
  namespaced identically to backfill
  ([synthetic/live_generators/linkedin_poll.py:1‑29](../../../services/ingest/synthetic/live_generators/linkedin_poll.py#L1-L29),
  [115‑177](../../../services/ingest/synthetic/live_generators/linkedin_poll.py#L115-L177)).

> Because the mock mirrors the **placeholder** Carta/Gusto read surface, a green
> spammer run proves the *Fyralis-side control-flow* (sharding, cursoring,
> dedup, poll cutover, reconciliation) — **not** conformance to the real LinkedIn
> REST API, which is unverified pending partner entitlement.
