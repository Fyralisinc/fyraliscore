# Figma Ingestion — How Fyralis Pulls Figma Data

This document explains, in detail, **how Figma data enters Fyralis**: which Figma
REST APIs are called, with which token, and how a single design-source signal set
— **file changes modelled as a pure "event" stream** (named versions + comments
merged into one stream) — is ingested.

It deliberately stops at the point where a Figma change becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Status: design vertical, partially UNVERIFIED.** Figma was built by cloning
> the **Brex Bearer-token archetype** (which itself clones Mercury). Several
> external-API details — the exact read endpoints, the pagination scheme, and
> historically the webhook auth scheme — are **cloned and unverified** against
> Figma's real API. The code carries explicit `TODO(human): confirm …` markers,
> reproduced verbatim below. Where this doc states a behaviour that the code
> itself flags as unverified, it is labelled **(UNVERIFIED in code)**; genuine
> doc-author inferences are labelled **(inferred)**.

---

## 1. The three ways data arrives

Figma data reaches Fyralis through **three ingress kinds that converge on one
handler and one channel** — `figma:event`. This is the Brex/Miro "pure event
stream" archetype: a *historical query surface* (backfill), an *incremental
re-run* of that same surface (poll), and a *live push surface* (webhook), all
routed to a single channel:

| Path | `ingress_kind` | Trigger | Mechanism | Code |
|------|----------------|---------|-----------|------|
| **Backfill (historical)** | `backfill` | Onboarding | Fyralis *pulls* a file's event stream via the Figma **REST API** | [planners/figma.py](../../../services/ingest/ingestion/planners/figma.py), [fetchers/figma.py](../../../services/ingest/ingestion/fetchers/figma.py) |
| **Poll (incremental)** | `poll` | Reconciler / re-run | Same fetcher, warm-started from the per-file high-water cursor (`start=<date>`) | [fetchers/figma.py:166‑179](../../../services/ingest/ingestion/fetchers/figma.py#L166-L179), [reconcilers/figma.py](../../../services/ingest/ingestion/reconcilers/figma.py) |
| **Live (real-time)** | `webhook` | A change in a watched file | Figma *pushes* a **Webhooks V2** delivery to Fyralis | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/figma.py](../../../services/app/webhooks/signatures/figma.py), [handlers/figma.py](../../../services/ingest/ingestion/handlers/figma.py) |

All three `(figma, *)` pairs map to the single channel `figma:event`
([normalizer/channel_mapping.py:244‑246](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L244-L246)):

```python
("figma", "backfill"): "figma:event",
("figma", "poll"):     "figma:event",
("figma", "webhook"):  "figma:event",
```

All three are parsed by the **single** `figma:event` handler
([handlers/figma.py](../../../services/ingest/ingestion/handlers/figma.py),
`@register("figma:event")`), which produces **exactly one observation per
record/event**. Both backfill and live derive the **same** dedup key:

```
external_id = "figma:{team_id}:event:{event_id}:{version}"
```

So a change that is both backfilled *and* delivered live collapses into **one**
observation. This is the central design invariant of Figma ingestion
([handlers/figma.py:52‑66](../../../services/ingest/ingestion/handlers/figma.py#L52-L66)).
Two properties of this key matter:

- **Namespaced by `team_id`** (a Figma-global id), because the global
  observations index is `UNIQUE(source_channel, external_id, occurred_at)` with
  **no `tenant_id` column** — so two tenants' identical synthetic event ids must
  not collapse ([handlers/figma.py:22‑28](../../../services/ingest/ingestion/handlers/figma.py#L22-L28)).
- **Versioned by `version`**, so a *re-published* version or *edited* comment
  lands a **new** observation rather than silently deduping, while an identical
  re-fetch still collapses ([handlers/figma.py:97‑109](../../../services/ingest/ingestion/handlers/figma.py#L97-L109)).

> **UNVERIFIED in code.** The fetcher's docstring records that **real Figma has
> no single `/events` list endpoint** — backfill must derive events from
> `GET /v1/files/{key}/versions` + `GET /v1/files/{key}/comments` and merge them
> into one event stream. The current code calls a single (cloned)
> `GET /v1/files/{key}/events` instead. See the `TODO(human)` blocks in §4 and §5.

---

## 2. Authentication & token model

Figma ingestion uses **one credential model: a long-lived org/team access
token**, presented as an HTTP **Bearer** token. This is the Brex Bearer
archetype; **there is no OAuth refresh flow in v1**
([client.py:1‑35](../../../services/ingest/integrations/figma/client.py#L1-L35),
[integrations/figma/__init__.py](../../../services/ingest/integrations/figma/__init__.py)):

- The token is resolved **once** from the secret store (via the install row's
  `secret_ref`), or **preset** in spammer mode, and reused for the life of the
  client. There is no per-call mint and no token cache eviction
  ([client.py:149‑171](../../../services/ingest/integrations/figma/client.py#L149-L171)).
- The auth header is `Authorization: Bearer {token}`
  ([client.py:173‑178](../../../services/ingest/integrations/figma/client.py#L173-L178)).
- The token and the auth header are **never logged**
  ([client.py:34](../../../services/ingest/integrations/figma/client.py#L34)).

> **TODO(human) reproduced verbatim** ([client.py:10‑21](../../../services/ingest/integrations/figma/client.py#L10-L21)):
> *"confirm Figma API host + read endpoints/scopes. … the read surface below
> (`/v1/teams/{id}/files`, `/v1/files/{key}`, `/v1/files/{key}/events`) is CLONED
> from Brex's account/transaction shape and is UNVERIFIED for Figma … There is
> NO single `/events` list endpoint in the real API … The required OAuth scopes
> (`file_content:read`, `file_versions:read`, `file_comments:read`,
> `projects:read`) must be confirmed …"* — left as a visible marker because the
> verified read surface is not yet implemented.

> **TODO(human) reproduced verbatim** ([client.py:173‑178](../../../services/ingest/integrations/figma/client.py#L173-L178)):
> *"confirm Figma's bearer header. The REST API historically also accepted
> `X-Figma-Token: {token}` for personal access tokens; OAuth uses
> `Authorization: Bearer {token}`. We send Bearer (Brex parity)."*

### 2.1 What the host docs *do* confirm

Two comments in the codebase are explicitly marked **CONFIRMED** against
`developers.figma.com`:

- **REST host** `https://api.figma.com` (base `/v1`; webhooks management `/v2`)
  ([endpoints.py:89‑91](../../../lib/integrations/endpoints.py#L89-L91),
  [oauth.py:56‑62](../../../services/ingest/integrations/figma/oauth.py#L56-L62)).
- Auth is a **personal access token via `X-Figma-Token`** *or* **OAuth2 Bearer**
  (authorize `https://www.figma.com/oauth`; token/refresh under
  `/v1/oauth/*`; read scopes `file_content:read`, `file_metadata:read`,
  `file_versions:read`) ([oauth.py:56‑62](../../../services/ingest/integrations/figma/oauth.py#L56-L62)).

So the *code* sends `Bearer`, but the *real* API also accepts the `X-Figma-Token`
PAT header — the unresolved choice the `client.py` TODO above flags.

### 2.2 Where credentials live

Figma follows the Brex **dedicated-table** shape, **not** the
`provider_installations` OAuth-bot-token path
([db/migrations/0103_figma.sql:1‑19](../../../db/migrations/0103_figma.sql#L1-L19)):

| Credential | Where | Notes |
|-----------|-------|-------|
| Access token | `encrypted_secrets`, referenced by `figma_installations.secret_ref` | label `figma_api_token:{base_url}` ([oauth.py:188‑190](../../../services/ingest/integrations/figma/oauth.py#L188-L190)) |
| Webhook secret / passcode | `encrypted_secrets`, referenced by `figma_installations.webhook_secret_ref` **and** `provider_installations.secret_ref` | label `figma_webhook_secret:{base_url}` ([oauth.py:191‑196](../../../services/ingest/integrations/figma/oauth.py#L191-L196)) |
| API base URL | `figma_installations.base_url` (per-install) | overridable per-env via `FIGMA_API_BASE_URL` ([endpoints.py:137](../../../lib/integrations/endpoints.py#L137)) |
| Team id | `figma_installations.team_id` | namespaces every `external_id` **and** is the webhook tenant key |

**Backfill** reads `figma_installations` / `figma_files`; the **live webhook
edge** reads `provider_installations` (`provider='figma'`,
`installation_id=team_id`). The two are seeded together but stay independent
([onboarding.py:13‑24](../../../services/ingest/integrations/figma/onboarding.py#L13-L24)).

### 2.3 The connect wizard (how an install is registered)

There is **no OAuth redirect handshake** — instead an admin **connect wizard**
posts the access token directly ([oauth.py:1‑33](../../../services/ingest/integrations/figma/oauth.py#L1-L33)):

1. **`POST /integrations/figma/connect/preflight`** (Bearer-authed) — calls
   `FigmaClient.list_files()` to verify the token and enumerate files for a
   selector UI. On auth failure it returns a structured `400` and **stores no
   secret** ([oauth.py:131‑151](../../../services/ingest/integrations/figma/oauth.py#L131-L151)).
2. **`POST /integrations/figma/connect/finalize`** — re-verifies the token
   **before any write**, resolves the file set (all enumerated, or the
   `file_keys` subset), persists the token (+ optional webhook secret) to the
   secret store, then calls `finalize_install()`
   ([oauth.py:154‑232](../../../services/ingest/integrations/figma/oauth.py#L154-L232)).
3. `finalize_install()` **UPSERTs** `figma_installations` (keyed
   `(tenant_id, base_url)`), inserts one `figma_files` row per file, and emits an
   `onboarding_triggers` row (`source='figma'`, `trigger_kind='install'`) so the
   existing M6 backfill chain fires — all in one tenant-scoped transaction
   ([onboarding.py:42‑131](../../../services/ingest/integrations/figma/onboarding.py#L42-L131)).
4. If **both** a `team_id` and a `webhook_secret` were supplied,
   `register_webhook_installation()` seeds the `provider_installations` row the
   webhook edge resolves the tenant + signing secret from
   ([onboarding.py:134‑156](../../../services/ingest/integrations/figma/onboarding.py#L134-L156)). Without them, only backfill is wired.

---

## 3. The Figma REST API surface that is actually called

All read calls funnel through `FigmaClient._request`
([client.py:180‑240](../../../services/ingest/integrations/figma/client.py#L180-L240)), which:

- sets `Authorization: Bearer {token}` and `Accept: application/json`,
- honours `Retry-After` on **`429`** within a bounded budget
  (`FIGMA_RL_MAX_ATTEMPTS`=4, `FIGMA_RL_MAX_SLEEP_SEC`=30),
- maps transport errors and any non-2xx to `FigmaApiError`
  ([client.py:304‑335](../../../services/ingest/integrations/figma/client.py#L304-L335)).

The endpoints invoked for ingestion:

| Figma endpoint | Wrapper | Purpose | Code |
|----------------|---------|---------|------|
| `GET /v1/files` | `list_files()` | enumerate files visible to the token (seed/install + selector UI) | [client.py:246‑258](../../../services/ingest/integrations/figma/client.py#L246-L258) |
| `GET /v1/files/{key}/meta` | `get_file()` | one file's lightweight metadata | [client.py:260‑262](../../../services/ingest/integrations/figma/client.py#L260-L262) |
| `GET /v1/files/{key}/events` | `list_events()` | one page of a file's event stream (backfill/poll + reconciler probe) | [client.py:264‑290](../../../services/ingest/integrations/figma/client.py#L264-L290) |

> **UNVERIFIED in code.** All three endpoint shapes are **cloned from Brex** and
> flagged unverified. In particular `GET /v1/files` (cloned from Brex
> `list_accounts`) is a single-call stand-in for what real Figma does as
> teams → projects → files; and `GET /v1/files/{key}/events` (cloned from Brex
> `list_transactions`) stands in for the real `/versions` + `/comments` merge
> ([client.py:246‑290](../../../services/ingest/integrations/figma/client.py#L246-L290)).
> `get_file()` is defined but **not** called on the ingestion hot path (inferred;
> the planner/fetcher/reconciler use only `list_files` + `list_events`).

### 3.1 Pagination — offset / limit

`list_events` pages with `offset` + `limit`, returning
`(events, next_offset, total)`; `next_offset is None` is terminal (when
`offset + len(events) >= total` or the page is empty)
([client.py:264‑290](../../../services/ingest/integrations/figma/client.py#L264-L290)).
The page size defaults to `100`, capped at `500`, overridable via
`FIGMA_BACKFILL_PAGE_SIZE` ([fetchers/figma.py:63‑67](../../../services/ingest/ingestion/fetchers/figma.py#L63-L67)).

> **TODO(human) reproduced verbatim** ([client.py:29‑32](../../../services/ingest/integrations/figma/client.py#L29-L32)):
> *"Pagination: `list_events` returns `(items, next_offset, total)`, `next_offset
> is None` terminal — offset/limit, CLONED from Brex and UNVERIFIED for Figma
> (the real file-list endpoints' pagination shape — cursor vs full list — is
> unverified …)."*

### 3.2 Rate limits

There is **no dedicated client-side token bucket for Figma** — `rate_limit/buckets.py`
has **no `figma` entry** (verified: a grep for `figma` in that file returns
nothing). Rate limiting is therefore handled **only** by the in-client
`Retry-After`-aware `429` retry loop above; contrast GitHub's per-app bucket and
Slack's per-method buckets.

> **TODO(human) reproduced verbatim** ([client.py:23‑27](../../../services/ingest/integrations/figma/client.py#L23-L27)):
> *"confirm Figma rate-limit signalling. Defaults to 429 + `Retry-After` (Brex's
> scheme) … Figma uses a leaky-bucket scheme with three endpoint tiers; Tier-1
> file reads are ~10-20/min (Dev/Full seat) and as low as ~6/MONTH on
> View/Collab seats — the token identity MUST be Dev/Full-seat."* — a real
> compliance hazard: the chosen token's seat type bounds throughput.

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one shard per file**, all of
`shard_kind = "figma_file_events"`
([planners/figma.py:55‑90](../../../services/ingest/ingestion/planners/figma.py#L55-L90)).
There is exactly **one shard family** (no Class-A/Class-B split like GitHub).

`ctx.source_client` is **None** — the planner is stateless and reads only DB
state. The active file list is pre-aggregated into `ctx.install["files"]` by the
SourceOnboarding loader's `json_agg` over `figma_files`
([source_onboarding.py:599‑619](../../../services/ingest/ingestion/workflows/source_onboarding.py#L599-L619)),
so the planner does no DB I/O ([planners/figma.py:55‑60](../../../services/ingest/ingestion/planners/figma.py#L55-L60)).

Each shard carries `file_key`, `file_name`, `team_id`, `installation_id`, and the
warm-start `event_cursor` (the per-file high-water `createdAt`; `None` on first
sync), at a baseline `recency_score=1.0`
([planners/figma.py:70‑84](../../../services/ingest/ingestion/planners/figma.py#L70-L84)).

> **TODO(human) reproduced verbatim** ([planners/figma.py:16‑20](../../../services/ingest/ingestion/planners/figma.py#L16-L20)):
> *"confirm Figma resource taxonomy to shard on. This clones Brex's
> one-shard-per-account model keyed on `figma_files.file_key`. Real Figma backfill
> enumerates teams → projects → files and derives an event stream per file from
> `/versions` + `/comments`; start with one shard per file and extend the
> companion-call fan-out once the surface is confirmed."*

---

## 5. The fetcher — one shard, two sync modes

A `figma_file_events` shard streams one file's events (named versions + comments
collapsed into an "event" stream). `fetch_page_figma` takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor ([fetchers/figma.py:142‑215](../../../services/ingest/ingestion/fetchers/figma.py#L142-L215)).

### 5.1 The cursor

```python
class FigmaCursor(BaseModel):
    offset: int = 0                       # list-events pagination offset
    high_water_created: str | None = None # max event createdAt (ISO) seen
    incremental_floor: str | None = None  # the `start=` lower bound (None in FULL)
    events_seen: int = 0                  # diagnostic
    seeded: bool = False                  # first-call setup ran?
```

([fetchers/figma.py:70‑91](../../../services/ingest/ingestion/fetchers/figma.py#L70-L91)).
It round-trips through the opaque `workflow_states.state_data` dict.

### 5.2 Two modes

- **FULL (initial backfill):** walk `GET /v1/files/{key}/events` from `offset=0`,
  paginated, until `next_offset is None`.
- **INCREMENTAL (poll):** when the shard is warm-started with an `event_cursor`,
  first-call setup copies it into `incremental_floor` + `high_water_created`, and
  the fetcher passes `start=<date>` (the date portion of the ISO timestamp,
  Figma `start` being date-granular) so only recent events come back; the overlap
  re-fetch dedups via the versioned `external_id`
  ([fetchers/figma.py:128‑179](../../../services/ingest/ingestion/fetchers/figma.py#L128-L179)).

### 5.3 One file → N event records

Unlike the Brex archetype (which also emits a per-shard balance snapshot), the
Figma fetcher emits **only `event` records — exactly one per event** — so a
fixture of N events yields N backfill observations per tenant (the synthetic
gate's expected-backfill count keys on this 1:1)
([fetchers/figma.py:18‑34](../../../services/ingest/ingestion/fetchers/figma.py#L18-L34)).
Each record is tagged with private routing keys the handler branches on
([fetchers/figma.py:191‑198](../../../services/ingest/ingestion/fetchers/figma.py#L191-L198)):

```python
records.append({
    "_fyralis_record_type": "event",
    "_fyralis_file_key": file_key,
    "_fyralis_team_id": team_id,
    "event": event,
})
```

The `team_id` is resolved preferentially from the install row, falling back to
the `shard_identifier` (so a unit test can drive the fetcher without a full
install row) ([fetchers/figma.py:110‑125](../../../services/ingest/ingestion/fetchers/figma.py#L110-L125)).

### 5.4 Rate-limit soft-stop

If `list_events` raises a `figma_api_rate_limited` error, the fetcher returns the
records gathered so far with `end_of_data=False` (so ShardFetch re-runs it later)
rather than failing the run ([fetchers/figma.py:180‑189](../../../services/ingest/ingestion/fetchers/figma.py#L180-L189)).
Any other `FigmaApiError` propagates.

> **TODO(human) reproduced verbatim** ([fetchers/figma.py:35‑41](../../../services/ingest/ingestion/fetchers/figma.py#L35-L41)):
> *"confirm Figma events API pagination (offset vs cursor) + created/posted
> filter. This fetcher clones the Brex offset/limit + `start=` date-filter
> contract (UNVERIFIED for Figma). Real Figma has no single `/events` list
> endpoint — backfill derives events from `/versions` + `/comments`; if so,
> replace the single `list_events` call with the two companion walks and merge
> them into the event stream."*

---

## 6. The handler — shaping events into `ObservationDraft`

`handle_figma_event` ([handlers/figma.py:226‑273](../../../services/ingest/ingestion/handlers/figma.py#L226-L273))
is a pure function (no DB / network) that branches on the input shape and emits
exactly one observation:

- **LIVE WEBHOOK path** — the raw Figma Webhooks V2 body carries the event type in
  `event_type` (or `type`); the fields are inline on the body. The handler strips
  the routing keys (`event_type`, `type`, `passcode`, the `_fyralis_*` tags) and
  treats the remainder as a flat event object
  ([handlers/figma.py:233‑254](../../../services/ingest/ingestion/handlers/figma.py#L233-L254)).
  A `PING` event is rejected as "not an observation" (a `ValidationError`)
  ([handlers/figma.py:238‑239](../../../services/ingest/ingestion/handlers/figma.py#L238-L239)).
- **BACKFILL / POLL path** — records arrive tagged `_fyralis_record_type="event"`;
  the event type defaults to `FILE_UPDATE` when absent
  ([handlers/figma.py:256‑268](../../../services/ingest/ingestion/handlers/figma.py#L256-L268)).
- A payload that is neither shape raises a `ValidationError`
  ([handlers/figma.py:270‑273](../../../services/ingest/ingestion/handlers/figma.py#L270-L273)).

Both paths feed the **same** `_event_draft` builder
([handlers/figma.py:125‑196](../../../services/ingest/ingestion/handlers/figma.py#L125-L196)):

| Field | Source | Notes |
|-------|--------|-------|
| `source_channel` | constant | `figma:event` |
| `external_id` | `figma:{team_id}:event:{event_id}:{version}` | namespaced + versioned; missing `team_id`/`id` → `ValidationError` ([handlers/figma.py:128‑134](../../../services/ingest/ingestion/handlers/figma.py#L128-L134)) |
| `occurred_at` | `createdAt` / `created_at` / `timestamp`, else `now()` | ISO-parsed ([handlers/figma.py:136‑141](../../../services/ingest/ingestion/handlers/figma.py#L136-L141)) |
| `kind` | `state_change` for `FILE_DELETE` or a dev-mode revert; else `signal` | see §6.1 |
| `trust_tier` | **`authoritative`** | Figma is the first-party system of record for our own design data ([handlers/figma.py:48‑49](../../../services/ingest/ingestion/handlers/figma.py#L48-L49)) |
| `source_actor_ref` | **`None`** | actor (handle/user/author) is carried in `content`/`entities_hint`, **not** as `source_actor_ref` ([handlers/figma.py:151‑168](../../../services/ingest/ingestion/handlers/figma.py#L151-L168)) |
| `content_text` | `"{event_type words}: {label}[ · by {actor}]"` | label from `label`/`description`/`message`/`file_name`/`file_key` ([handlers/figma.py:143‑161](../../../services/ingest/ingestion/handlers/figma.py#L143-L161)) |
| `entities_hint` | `figma_file`, `figma_team`, and (if present) `person` (role=actor) | ([handlers/figma.py:163‑168](../../../services/ingest/ingestion/handlers/figma.py#L163-L168)) |

### 6.1 The `version` discriminator and state-change events

`_event_version` picks the first present of `version` / `version_id` /
`updated_at` / `updated` / `modified_at`, falling back to `createdAt`, else the
literal `"none"` (an immutable single-shot event collapses on re-fetch)
([handlers/figma.py:97‑109](../../../services/ingest/ingestion/handlers/figma.py#L97-L109)).

`_is_state_change` returns `True` for `FILE_DELETE`, and for a
`DEV_MODE_STATUS_UPDATE` whose status is one of `in_progress` / `not_ready` /
`reverted` — a design-lifecycle reversal (ready-for-dev rollback). Everything
else (version/comment/publish/file update) is a forward `signal`
([handlers/figma.py:68‑118](../../../services/ingest/ingestion/handlers/figma.py#L68-L118)).

### 6.2 A note on the idempotency helper

The external-id constructor prefers a shared `idempotency.figma_event(...)` if it
exists, else falls back to an inline f-string that produces the **identical**
string — so swapping to the shared constructor is a no-op
([handlers/figma.py:52‑66](../../../services/ingest/ingestion/handlers/figma.py#L52-L66)).
A parallel `TODO` notes that `FigmaApiError` is likewise defined locally pending
promotion to `lib/shared/errors.py`
([client.py:64‑68](../../../services/ingest/integrations/figma/client.py#L64-L68)).

---

## 7. Live (real-time) ingestion via webhooks

When a watched file changes, Figma **POSTs a Webhooks V2 delivery** to Fyralis's
webhook edge. The generic webhook router (`POST /webhooks/{provider}`) maps
provider `figma` → channel `figma:event`
([webhooks/router.py:437‑463](../../../services/app/webhooks/router.py#L437-L463),
`_PROVIDER_CHANNEL["figma"]="figma:event"`), and `VERIFIERS["figma"]` is the
Figma verifier ([signatures/__init__.py:62](../../../services/app/webhooks/signatures/__init__.py#L62)).

### 7.1 Signature verification — passcode-in-body (the real scheme)

This is the one place the doc must flag a divergence between **what real Figma
does** and **what the synthetic gate exercises**. The verifier
([signatures/figma.py](../../../services/app/webhooks/signatures/figma.py))
implements **both** schemes behind a flag, and the flag selects the **real
passcode-in-body** scheme:

```python
_USE_PASSCODE_IN_BODY = True   # the real, CONFIRMED scheme
```

- **Real Figma (selected, `_USE_PASSCODE_IN_BODY = True`):** Webhooks V2
  authenticate with a **plaintext `passcode` carried as a top-level JSON field in
  the body** — **there is no signature header**. The verifier parses the body,
  reads `body["passcode"]`, and **constant-time-compares** it against each active
  secret (rotation-safe), raising a `400`-class `WebhookVerificationError` on a
  missing/mismatched passcode. This is **CONFIRMED** against
  `developers.figma.com/docs/rest-api/webhooks-security`
  ([signatures/figma.py:1‑25](../../../services/app/webhooks/signatures/figma.py#L1-L25),
  [44‑115](../../../services/app/webhooks/signatures/figma.py#L44-L115)).
  The code's own comment is explicit that *"the third-party
  `figma-signature`/HMAC schemes some blogs describe do NOT exist in the official
  docs."*
- **Legacy HMAC-header fallback (`_USE_PASSCODE_IN_BODY = False`, NOT selected):**
  a `Figma-Signature: sha256=<hex HMAC-SHA256(secret, body)>` header, retained
  **only** as a stand-in for the synthetic gate's older HMAC shape
  ([signatures/figma.py:48‑55](../../../services/app/webhooks/signatures/figma.py#L48-L55),
  [117‑148](../../../services/app/webhooks/signatures/figma.py#L117-L148)).

> **Reconciling the "HMAC-shaped stand-in" hint.** Several *other* code comments
> (the channel-mapping note, the tenant-resolver note, the integration `__init__`,
> the migration header) still describe Figma as an "HMAC-shaped stand-in for the
> gate / Brex archetype." Those comments are **stale relative to the verifier**:
> the verifier ships with `_USE_PASSCODE_IN_BODY = True`, so the **live path
> actually verifies a body passcode, not an HMAC header**. The synthetic
> `HmacWebhookGenerator` drives Figma by **embedding the passcode in the body**
> (a wrong passcode is its tamper-rejection probe)
> ([signatures/figma.py:12‑14](../../../services/app/webhooks/signatures/figma.py#L12-L14)).

The per-tenant secret(s) are loaded by `services/app/webhooks/secrets.py` from the
`provider_installations` row (`provider='figma'`); the verifier loops over all
active secrets so a rotation (two valid passcodes in flight) verifies
([signatures/figma.py:17‑21](../../../services/app/webhooks/signatures/figma.py#L17-L21)).

### 7.2 Replay handling

Like GitHub/Jira/Brex, the digest/passcode is over the **body alone — no
timestamp envelope** — so there is **no replay window** here; `signed_timestamp`
is returned as `None` ([signatures/figma.py:22‑24](../../../services/app/webhooks/signatures/figma.py#L22-L24),
[110‑115](../../../services/app/webhooks/signatures/figma.py#L110-L115)).
Idempotency is enforced **at the ingestion layer** via the versioned
`external_id`, not here. The `(installation, delivery)` **replay cache in the
router is GitHub-specific** — there is no equivalent for Figma (verified: the
router's replay cache is `github_replay_cache`, keyed on the GitHub delivery
header only — [webhooks/router.py:94‑116](../../../services/app/webhooks/router.py#L94-L116)).

### 7.3 Tenant resolution

The tenant is resolved from the webhook body's top-level `team_id` (or `teamId`)
→ the `provider_installations` row for `(provider='figma', installation_id=team_id)`
([tenant_resolver.py:465‑478](../../../services/app/webhooks/tenant_resolver.py#L465-L478)).
Figma carries no installation id in the URL path, so the team id in the body is
the resolution key.

> **TODO(human) reproduced verbatim** ([tenant_resolver.py:473‑474](../../../services/app/webhooks/tenant_resolver.py#L473-L474)):
> *"confirm figma webhook tenant-id field against Figma Webhooks V2 docs (the body
> field carrying team_id)."*

### 7.4 Kafka cutover vs inline ingest

Figma is in **both** `_WEBHOOK_PROVIDER_SOURCE` and `_CUTOVER_ENABLED_PROVIDERS`
([webhooks/router.py:145](../../../services/app/webhooks/router.py#L145),
[185](../../../services/app/webhooks/router.py#L185)). After verification +
tenant resolution, the router checks the tenant's `ingestion.kafka_path_enabled`
flag ([webhooks/router.py:1037‑1070](../../../services/app/webhooks/router.py#L1037-L1070)):

- **Flag ON:** publish the envelope to Kafka and return **`202 accepted`**; the
  writer pool produces the observation (the 202 cutover contract fits Figma — no
  synchronous-response-shape constraint like Discord).
- **Flag OFF (or Kafka publish failed):** fall through to **inline `ingest()`**
  on the `figma:event` channel ([webhooks/router.py:1092‑1109](../../../services/app/webhooks/router.py#L1092-L1109)).
  A failed publish records a `fallback` metric (graceful degradation, not
  gate-relaxation) and serves the user-visible 200/201 contract.

---

## 8. Reconciliation — gap detection

`reconcile_figma` ([reconcilers/figma.py:129‑166](../../../services/ingest/ingestion/reconcilers/figma.py#L129-L166))
re-checks **completed** (`state="done"`) file shards for new activity. It loads
the single active `figma_installations` row for the tenant, opens one client, and
probes each shard ([reconcilers/figma.py:78‑126](../../../services/ingest/ingestion/reconcilers/figma.py#L78-L126)):

1. Read the shard's `high_water_created` from its persisted cursor. If `None` (an
   empty file / no reference point), skip.
2. Issue a **cheap probe** — `list_events(file_key, limit=1, offset=0,
   start=high_water[:10])`.
3. If an event with `createdAt` **strictly greater** than the high-water exists,
   emit a `ResharedShard` warm-started at the high-water (`event_cursor=high_water`,
   so the reshared walk runs in incremental mode) at **`recency_score=1.5`**.

The probe is best-effort: a probe exception is logged and treated as "no gap"
([reconcilers/figma.py:96‑101](../../../services/ingest/ingestion/reconcilers/figma.py#L96-L101)).
Because `external_id` parity is versioned, re-walked events dedup against what
backfill already wrote — the design is *"can over-reshare but never
under-reshares, and dedup makes re-walks idempotent"*
([reconcilers/figma.py:1‑11](../../../services/ingest/ingestion/reconcilers/figma.py#L1-L11)).
The reconciler needs a pool provider registered at startup
(`set_pool_provider`) ([reconcilers/figma.py:40‑52](../../../services/ingest/ingestion/reconcilers/figma.py#L40-L52)).

---

## 9. Revocation chokepoint — ABSENT

**There is no revocation chokepoint for Figma.** Unlike GitHub (whose client
disables an installation on `401 Bad credentials` / scoped `404`) and Notion
(which parks + disables on token revocation), the Figma client **maps `401`/`403`
to `figma_api_unauthorized` and raises — it does not disable the install row**
(verified: a grep for `disable`/`revok`/`chokepoint` across
`services/ingest/integrations/figma/` finds only the `disabled_at = NULL`
*re-enable* in `finalize_install`'s UPSERT — [onboarding.py:82](../../../services/ingest/integrations/figma/onboarding.py#L82)).
A rejected token therefore surfaces as a `FigmaApiError`
([client.py:236‑240](../../../services/ingest/integrations/figma/client.py#L236-L240),
[304‑335](../../../services/ingest/integrations/figma/client.py#L304-L335)); recovery is
operator-driven — re-run the connect wizard with a fresh token (which re-UPSERTs
the install and clears `disabled_at`).

> **(inferred)** Because the access token is resolved once and never refreshed
> (§2), and there is no per-error disable path, a revoked or expired token will
> cause every shard read to raise `figma_api_unauthorized` until an operator
> re-finalizes the install. This is a deliberate v1 gap, consistent with the
> "OAuth refresh out of v1 scope" posture stated throughout.

---

## 10. End-to-end summary

```
                          ┌──────────────────────── BACKFILL / POLL (pull) ─────────────────────┐
                          │  long-lived Bearer access token (resolved once from secret store)    │
   FILES VISIBLE TO TOKEN │  planner: files pre-aggregated from figma_files (source_client=None) │
                          │     └─► one figma_file_events shard per file (recency 1.0)           │
   one shard, two modes   │  fetcher: GET /v1/files/{key}/events  (offset/limit)                 │
                          │     FULL  : offset=0 → drain pages                                   │
                          │     POLL  : warm-start high-water → start=<date> (incremental)       │
                          │     └─► one {_fyralis_record_type:"event", event} record per event   │
                          └────────────────────────────────────────────────────────────────────┬┘
                                                                                                 │
                          ┌──────────────────────── LIVE (push) ──────────────────────────────┐ │
   FILE CHANGE  ──────────►  Figma Webhooks V2 ──HTTP POST──► /webhooks/figma                  │ │
                          │     verify body PASSCODE (constant-time; rotation-safe; NO header)  │ │
                          │     tenant from body team_id → provider_installations               │ │
                          │     PING → rejected (not an observation)                            │ │
                          │     kafka_path_enabled? → 202 cutover : inline figma:event          │ │
                          └────────────────────────────────────────────────────────────────────┘ │
                                                                                                 │
                                                            ┌────────────────────────────────────▼─┐
                                                            │  handle_figma_event  (one channel)     │
                                                            │  external_id =                         │
                                                            │   figma:{team}:event:{id}:{version}    │
                                                            │  trust_tier = authoritative            │
                                                            │  kind = signal | state_change          │
                                                            │  → ObservationDraft                     │
                                                            └────────────────────────────────────────┘
```

**Key invariants**

1. **One handler, one channel, one dedup namespace.** Backfill, poll, and live
   all land on `figma:event` with
   `external_id="figma:{team_id}:event:{event_id}:{version}"`. A backfilled event
   and its live twin dedup to one observation.
2. **Namespaced + versioned external_id.** `team_id` (Figma-global) keeps two
   tenants distinct under the tenant-less `UNIQUE(source_channel, external_id,
   occurred_at)`; `version` lets a re-publish/edit land as a *new* observation.
3. **One credential model.** A single long-lived **Bearer** access token reads
   everything; no OAuth refresh in v1. Backfill uses `figma_installations`; the
   live edge uses `provider_installations`.
4. **One shard family.** `figma_file_events`, one shard per file, offset/limit
   paginated, with a per-file high-water `createdAt` cursor driving incremental
   poll **and** reconciler gap detection.
5. **Webhook auth is a body passcode, not an HMAC header.** The verifier ships
   with `_USE_PASSCODE_IN_BODY = True` (the real, CONFIRMED Figma V2 scheme);
   no replay window; idempotency is the `external_id` dedup.
6. **No revocation chokepoint** (§9) and **no dedicated rate-limit bucket**
   (§3.2) — both deliberate v1 gaps.

---

## 11. Configuration & compliance

> **Compliance caveat.** Unlike the GitHub/Slack docs, this section is **not**
> "verified against Figma's official docs end-to-end." Two facts are CONFIRMED in
> code against `developers.figma.com` (the REST host and the webhook
> passcode-in-body scheme); the read endpoints, scopes, and pagination remain
> **UNVERIFIED** per the `TODO(human)` markers reproduced above.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `FIGMA_API_BASE_URL` | `https://api.figma.com/v1` ([endpoints.py:91](../../../lib/integrations/endpoints.py#L91), [137](../../../lib/integrations/endpoints.py#L137)) | per-env override of the Figma API host |
| `FIGMA_BACKFILL_PAGE_SIZE` | `100` (capped 500) | `list_events` page size ([fetchers/figma.py:63‑67](../../../services/ingest/ingestion/fetchers/figma.py#L63-L67)) |
| `FIGMA_RL_MAX_ATTEMPTS` | `4` | rate-limit (`429`) retry budget ([client.py:200](../../../services/ingest/integrations/figma/client.py#L200)) |
| `FIGMA_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:201](../../../services/ingest/integrations/figma/client.py#L201)) |

There is **no** `FIGMA_APP_*` / OAuth env surface (no OAuth in v1) and **no**
`WEBHOOK_SECRET_FIGMA` env (the webhook passcode lives in the secret store via
`provider_installations.secret_ref`, not an env var).

### 11.2 Verified compliant ✅ / open ⚠️

- **Webhook auth** — passcode-in-body (CONFIRMED against developers.figma.com),
  constant-time compare, rotation-safe, no replay window. ✅
- **REST host** — `https://api.figma.com` (base `/v1`), CONFIRMED. ✅
- **Least secret surface** — token + webhook passcode held in `encrypted_secrets`,
  only opaque refs in the install tables; token/header never logged. ✅
- **Tenant isolation** — `figma_installations` / `figma_files` ENABLE+FORCE RLS
  ([0103_figma.sql:105‑129](../../../db/migrations/0103_figma.sql#L105-L129)). ✅
- **Read endpoints / scopes / pagination** — CLONED from Brex, UNVERIFIED;
  real Figma derives events from `/versions` + `/comments`. ⚠️
- **Rate-limit tiering** — no per-tier bucket; the token's **seat type** (Dev/Full
  vs View/Collab) silently bounds throughput. ⚠️
- **Revocation** — no chokepoint; a rejected token surfaces as a raised error
  until an operator re-finalizes the install. ⚠️

### 11.3 Dev / spammer mode

For local testing against the mock source server, `build_figma_client` detects
spammer mode and **presets** the token to `spam-figma` (skipping any secret-store
lookup), pointing the API base at the endpoint resolver so backfill hits the
local spammer's `/figma` sub-path ([_clients.py:596‑619](../../../services/ingest/ingestion/fetchers/_clients.py#L596-L619),
[endpoints.py:167](../../../lib/integrations/endpoints.py#L167)). The mock Figma
server serves `GET /v1/files`, `GET /v1/files/{key}/meta`, and
`GET /v1/files/{key}/events` (full vs `start=`-driven incremental modes) from
fixtures ([synthetic/mock_servers/figma.py:7‑16](../../../services/ingest/synthetic/mock_servers/figma.py#L7-L16),
[43‑106](../../../services/ingest/synthetic/mock_servers/figma.py#L43-L106)). The
synthetic webhook gate drives the live path through the shared
`HmacWebhookGenerator`, embedding the **passcode in the body** (a wrong passcode
is its tamper-rejection probe).
