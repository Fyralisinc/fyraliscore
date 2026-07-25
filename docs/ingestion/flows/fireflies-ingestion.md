# Fireflies Ingestion — How Fyralis Pulls Fireflies Data

This document explains, in detail, **how Fireflies data enters Fyralis**: which
Fireflies API is called, with which token, and how the single Fireflies signal —
**AI‑notetaker meeting transcripts** — is ingested. Fireflies is an *attesting
agent*: it transcribes what humans said in a meeting, so its transcripts are a
signal, not a system of record.

It deliberately stops at the point where a Fireflies transcript becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **A note on verification.** Fireflies is a recently‑added source built from the
> Brex/HMAC "Bearer‑token" archetype. Several pieces of the *outbound* surface
> (the REST read paths, the offset/limit pagination, the 429 rate‑limit signal)
> are **cloned from Brex and not yet empirically verified against Fireflies' real
> API**, which is GraphQL. The code carries explicit `TODO(human)` markers where
> this matters; this doc reproduces them verbatim rather than papering over them.
> What *is* verified end‑to‑end (one handler, one dedup key, the webhook edge, the
> install/onboarding wiring, the channel mapping, the Provider Lab path) is described
> as fact.

---

## 1. The three ways data arrives

Fireflies data reaches Fyralis through **three paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding | Fyralis *pulls* the workspace's transcript history (`list_transcripts` from offset 0) | [planners/fireflies.py](../../../services/ingest/ingestion/planners/fireflies.py), [fetchers/fireflies.py](../../../services/ingest/ingestion/fetchers/fireflies.py) |
| **Poll (incremental)** | Reconciler reshare / re‑run | Same fetcher re‑run with a `start=` date floor derived from the high‑water transcript cursor | [fetchers/fireflies.py:144‑156](../../../services/ingest/ingestion/fetchers/fireflies.py#L144-L156) |
| **Live (real‑time)** | A meeting transcript completes | Fireflies *pushes* an **HMAC‑signed webhook** (`transcript.completed`) to Fyralis | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/fireflies.py](../../../services/app/webhooks/signatures/fireflies.py), [handlers/fireflies.py](../../../services/ingest/ingestion/handlers/fireflies.py) |

Note that **backfill and poll are the same fetcher** — one `fireflies_transcripts`
shard kind, two sync modes selected by whether a warm‑start cursor is present
([fetchers/fireflies.py:8‑19](../../../services/ingest/ingestion/fetchers/fireflies.py#L8-L19)).

Crucially, **all three paths produce one observation per transcript** through the
**single** `fireflies:transcript` handler
([handlers/fireflies.py:224‑269](../../../services/ingest/ingestion/handlers/fireflies.py#L224-L269)).
The handler branches on the input shape (a live webhook body carries a `type`; a
backfill/poll record is tagged with a private `_fyralis_record_type`) but funnels
both into the **same** record builder so a transcript that is both backfilled
*and* delivered live collapses into **one** observation. All three derive the
**same** dedup key:

```
external_id = "fireflies:{workspace_id}:transcript:{transcript_id}:{version}"
```

built by `idempotency.fireflies_transcript`
([idempotency/__init__.py:246‑253](../../../services/ingest/ingestion/idempotency/__init__.py#L246-L253)).

> **The `external_id` is VERSIONED.** The observations repo dedups on
> `(source_channel, external_id)` ignoring `occurred_at`, so a *re‑processed*
> transcript (a richer summary or corrected text landing later) must change the
> key or it would silently dedup away. The `{version}` segment is the transcript's
> content version — the first present of `version` / `updatedAt` / `updated_at` /
> `processedAt` / `dateTime` / `date`, falling back to the literal `"v1"`
> ([handlers/fireflies.py:70‑81](../../../services/ingest/ingestion/handlers/fireflies.py#L70-L81)).
> An *identical* re‑fetch dedups; a *changed* one re‑observes. This is the central
> design invariant of Fireflies ingestion.

> **The ground‑truth comment in `channel_mapping.py`** describes the external_id
> as `fireflies:{workspace}:transcript:{id}:{version}`
> ([channel_mapping.py:217‑218](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L217-L218))
> — this matches the actual idempotency builder exactly. **Verified.**

---

## 2. Authentication & token model

Fireflies uses **one credential type: a long‑lived API token presented as an HTTP
`Authorization: Bearer` header** — the Brex/Notion/Jira "Bearer archetype." There
is **no OAuth bounce** and **no token refresh**: the operator pastes the API key
(issued in the Fireflies app's *Settings → Developer Settings*) directly into the
connect wizard
([oauth.py:56‑60](../../../services/ingest/integrations/fireflies/oauth.py#L56-L60)).

The token is resolved **once** per client — from the secret store (production) or
preset (Provider Lab) — and reused for the life of the client; the token and the auth
header are **never logged**
([client.py:108‑134](../../../services/ingest/integrations/fireflies/client.py#L108-L134),
[client.py:30](../../../services/ingest/integrations/fireflies/client.py#L30)).

### 2.1 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| API token (Bearer) | secret store, label `fireflies_api_token:{base_url}`; opaque `secret_ref` on the `fireflies_installations` row | [oauth.py:184‑186](../../../services/ingest/integrations/fireflies/oauth.py#L184-L186) |
| Webhook HMAC secret | secret store, label `fireflies_webhook_secret:{base_url}`; `webhook_secret_ref` on the install row **and** `secret_ref` on the `provider_installations` row | [oauth.py:188‑192](../../../services/ingest/integrations/fireflies/oauth.py#L188-L192), [onboarding.py (register_webhook_installation)](../../../services/ingest/integrations/fireflies/onboarding.py) |
| Workspace id | `workspace_id` column on `fireflies_installations`; **also** the `provider_installations.installation_id` for webhook tenant resolution | [0099_fireflies.sql:43‑66](../../../db/migrations/0099_fireflies.sql#L43-L66) |

### 2.2 The connect wizard (how an install gets registered)

There is no `code` exchange. `services/ingest/integrations/fireflies/oauth.py`
implements a two‑step admin "connect" wizard
([oauth.py:9‑30](../../../services/ingest/integrations/fireflies/oauth.py#L9-L30)):

1. **`POST /integrations/fireflies/connect/preflight`** (Bearer‑authed) — takes
   `{api_token, base_url?}`, calls `FirefliesClient.get_workspace()` to verify the
   token and resolve the workspace id, and returns it. On auth failure it returns
   a structured `400` and **stores no secret**
   ([oauth.py:126‑147](../../../services/ingest/integrations/fireflies/oauth.py#L126-L147)).
2. **`POST /integrations/fireflies/connect/finalize`** — re‑verifies the creds
   **before any write**, resolves the workspace id (probed, or a supplied
   `workspace_id` override), then **persists secrets, upserts**
   `fireflies_installations` (keyed `(tenant_id, base_url)`), and **emits an
   `onboarding_triggers` row** (`source='fireflies'`) so the existing M6 backfill
   chain fires
   ([oauth.py:150‑228](../../../services/ingest/integrations/fireflies/oauth.py#L150-L228),
   [onboarding.py:40‑104](../../../services/ingest/integrations/fireflies/onboarding.py#L40-L104)).
3. **Only if** a `webhook_secret` was supplied does finalize also call
   `register_webhook_installation`, which upserts the `provider_installations` row
   (`provider='fireflies'`, `installation_id=workspace_id`) the live edge resolves
   the tenant + HMAC secret from
   ([oauth.py:205‑215](../../../services/ingest/integrations/fireflies/oauth.py#L205-L215),
   [onboarding.py:107‑130](../../../services/ingest/integrations/fireflies/onboarding.py#L107-L130)).

Backfill (`fireflies_installations`) and the live webhook edge
(`provider_installations`) are seeded together but stay **independent** — a tenant
can backfill without a webhook secret, and the live edge simply won't be wired
([oauth.py:29‑31](../../../services/ingest/integrations/fireflies/oauth.py#L29-L31)).

> The connect routes above are implemented and Bearer‑authed, but note the
> codebase comment confirming Fireflies' **real auth has no OAuth bounce** — the
> wizard takes the key directly
> ([oauth.py:57‑59](../../../services/ingest/integrations/fireflies/oauth.py#L57-L59)).

---

## 3. The API surface actually called

All read calls funnel through `FirefliesClient._request`
([client.py:136‑196](../../../services/ingest/integrations/fireflies/client.py#L136-L196)),
which:

- sets `Authorization: Bearer {token}` and `Accept: application/json`,
- honours `Retry-After` on **`429`** within a bounded budget
  (`FIREFLIES_RL_MAX_ATTEMPTS`=4, `FIREFLIES_RL_MAX_SLEEP_SEC`=30),
- maps transport errors and any non‑2xx to a typed `FirefliesApiError`
  ([client.py:251‑282](../../../services/ingest/integrations/fireflies/client.py#L251-L282)).

The endpoints invoked for ingestion:

| Endpoint (as coded) | Wrapper | Purpose | Code |
|---------------------|---------|---------|------|
| `GET /workspace` | `get_workspace()` | resolve the workspace id at install time | [client.py:202‑209](../../../services/ingest/integrations/fireflies/client.py#L202-L209) |
| `GET /transcripts?limit&offset&start` | `list_transcripts()` | one page of transcripts, newest‑first | [client.py:215‑237](../../../services/ingest/integrations/fireflies/client.py#L215-L237) |
| `GET /transcript/{id}` | `get_transcript()` | one full transcript (probe / hydrate) | [client.py:211‑213](../../../services/ingest/integrations/fireflies/client.py#L211-L213) |

> **UNVERIFIED — the REST surface is cloned from Brex.** The client's own
> docstring is explicit: Fireflies' real API is **GraphQL** — a single
> `POST https://api.fireflies.ai/graphql` exposing a `transcripts` query and a
> `transcript(id:)` query, **not** the REST paths above. If/when the GraphQL
> surface is confirmed, `_request` should be swapped for one POST to `/graphql`
> with a query+variables body, and `list_transcripts`/`get_transcript` re‑pointed
> at `data.transcripts` / `data.transcript`
> ([client.py:9‑18](../../../services/ingest/integrations/fireflies/client.py#L9-L18)).
> Reproduced TODO:
>
> > **TODO(human):** confirm Fireflies API host + read endpoints/scopes … the read
> > surface below (`/transcripts`, `/transcript/{id}`) is CLONED from Brex and
> > UNVERIFIED for Fireflies … Implement only the verified read surface.

### 3.1 Pagination — offset/limit

`list_transcripts` returns `(items, next_offset, total)`; `next_offset is None`
is terminal (`offset + len(items) >= total`, or an empty page)
([client.py:228‑237](../../../services/ingest/integrations/fireflies/client.py#L228-L237)).
The fetcher persists `offset` in its shard cursor and resumes next invocation.

> **UNVERIFIED pagination.** Offset/limit is also cloned from Brex. The fetcher
> docstring records a partial confirmation: the GraphQL `transcripts` query is
> `skip`/`limit`‑based with **`limit` max 50** and `fromDate`/`toDate` filters,
> which *maps cleanly* onto the offset/limit + `start=` shape — but the actual
> GraphQL wiring is still outstanding
> ([fetchers/fireflies.py:34‑41](../../../services/ingest/ingestion/fetchers/fireflies.py#L34-L41)).
> Reproduced TODO:
>
> > **TODO(human):** the GraphQL query wiring (vs. the cloned REST `_request`) +
> > the exact digest encoding of the webhook signature still need empirical
> > confirmation. Page size is overridable via `FIREFLIES_BACKFILL_PAGE_SIZE`.

### 3.2 Rate limits — no dedicated bucket

There is **no `("fireflies", …)` entry** in `BUCKET_DEFAULTS`; only
`slack`/`github`/`gmail`/`discord` have client‑side token buckets
([rate_limit/buckets.py:79‑91](../../../services/ingest/ingestion/rate_limit/buckets.py#L79-L91)).
Fireflies' only rate‑limit defense is the client's in‑request `Retry-After`‑aware
429 retry (§3). The 429 signal itself is unverified:

> **TODO(human):** confirm Fireflies rate‑limit signalling. Defaults to 429 +
> `Retry-After` (Brex's scheme) … Fireflies may instead signal via a GraphQL error
> extension (`code: "too_many_requests"`)
> ([client.py:20‑23](../../../services/ingest/integrations/fireflies/client.py#L20-L23)).

---

## 4. Backfill scope — the shard families

The planner is intentionally minimal: Fireflies' install is **workspace‑scoped
with no sharded sub‑resource** — a workspace's transcripts are a single
newest‑first stream — so it emits exactly **ONE** `fireflies_transcripts` shard per
workspace install ([planners/fireflies.py:1‑22](../../../services/ingest/ingestion/planners/fireflies.py#L1-L22)).

```python
shard = Shard(
    shard_kind="fireflies_transcripts",
    shard_identifier={
        "shard_kind": "fireflies_transcripts",
        "workspace_id": workspace_id,
        "installation_id": install_id,
        "transcript_cursor": txn_cursor,   # high-water; None on first sync
    },
    recency_score=1.0,
)
```

([planners/fireflies.py:56‑67](../../../services/ingest/ingestion/planners/fireflies.py#L56-L67)).
The planner reads **DB state only** — `workspace_id` and `transcript_cursor` come
off the install row loaded by the SourceOnboarding loader — so it is stateless and
`ctx.source_client` is `None`
([planners/fireflies.py:37‑54](../../../services/ingest/ingestion/planners/fireflies.py#L37-L54)). If
no workspace id is present, it plans zero shards.

> **TODO(human):** confirm Fireflies resource taxonomy to shard on. This clones
> Brex's per‑install shard model but collapsed to a single workspace stream … If a
> workspace's transcripts must be sharded (e.g. per channel / per host) once the
> surface is confirmed, fan the shard list out here
> ([planners/fireflies.py:17‑21](../../../services/ingest/ingestion/planners/fireflies.py#L17-L21)).

---

## 5. Fetch specifics — one shard, two sync modes, high‑water cursor

`fetch_page_fireflies` takes one `(install, shard_identifier, cursor)` triple and
returns one page of records + the next cursor; ShardFetch loops it, persisting the
cursor between calls
([fetchers/fireflies.py:128‑191](../../../services/ingest/ingestion/fetchers/fireflies.py#L128-L191)).

### 5.1 The cursor

```python
class FirefliesCursor(BaseModel):
    offset: int = 0                       # list pagination offset within a run
    high_water_created: str | None = None # max transcript dateTime seen (ISO)
    incremental_floor: str | None = None  # the start= floor frozen for this run
    transcripts_seen: int = 0             # diagnostic
    seeded: bool = False                  # first-call setup ran
```

([fetchers/fireflies.py:70‑91](../../../services/ingest/ingestion/fetchers/fireflies.py#L70-L91)).
`high_water_created` is the **transcript high‑water mark** — the newest
transcript `dateTime`/`date`/`createdAt` walked
([fetchers/fireflies.py:117‑126](../../../services/ingest/ingestion/fetchers/fireflies.py#L117-L126)).
It is both the warm‑start lower bound for incremental polls **and** the
reconciler's gap reference point.

### 5.2 FULL vs INCREMENTAL

On the first call, the fetcher seeds itself. If the shard was warm‑started with a
`transcript_cursor`, it sets `incremental_floor = high_water = cursor`, switching
to **INCREMENTAL** mode; otherwise it stays **FULL** (offset 0, no floor)
([fetchers/fireflies.py:143‑149](../../../services/ingest/ingestion/fetchers/fireflies.py#L143-L149)).
Each page calls `list_transcripts(limit, offset, start=date(incremental_floor))`,
where `start` is the **date portion only** (`iso[:10]`) because the filter is
date‑granular ([fetchers/fireflies.py:110‑114](../../../services/ingest/ingestion/fetchers/fireflies.py#L110-L114),
[151‑156](../../../services/ingest/ingestion/fetchers/fireflies.py#L151-L156)). The date‑granular overlap is
harmless — re‑fetched transcripts dedup on the versioned `external_id`.

### 5.3 Record shape (fetcher → handler)

The fetcher emits **only** "transcript" records (no snapshot/balance record, unlike
the Brex archetype it was cloned from), tagging each with the private keys the
handler branches on ([fetchers/fireflies.py:168‑174](../../../services/ingest/ingestion/fetchers/fireflies.py#L168-L174)):

```python
records.append({
    "_fyralis_record_type": "transcript",
    "_fyralis_workspace_id": workspace_id,
    "transcript": transcript,
})
```

### 5.4 Rate‑limit soft‑pause

If `list_transcripts` raises a `fireflies_api_rate_limited` error, the fetcher
returns the page collected so far with `end_of_data=False` (so ShardFetch re‑runs
it) rather than failing the shard
([fetchers/fireflies.py:157‑166](../../../services/ingest/ingestion/fetchers/fireflies.py#L157-L166)). Any
other `FirefliesApiError` propagates and fails the shard.

---

## 6. The handler — shaping a transcript into `ObservationDraft`

`handle_fireflies_transcript` ([handlers/fireflies.py:224‑269](../../../services/ingest/ingestion/handlers/fireflies.py#L224-L269))
is a pure function (no DB / network). It branches on input shape:

- **Live webhook**: the body carries a `type`. If it `startswith("transcript")`
  or contains `"transcription"` (so both `transcript.completed` and
  `transcription_complete` match), the handler pulls the transcript out of
  `transcript` / `data` / `meeting` and builds the draft; an unknown `type` is a
  `ValidationError` ([handlers/fireflies.py:233‑256](../../../services/ingest/ingestion/handlers/fireflies.py#L233-L256)).
- **Backfill / poll**: the record is tagged `_fyralis_record_type=="transcript"`
  (or simply contains a `transcript` key)
  ([handlers/fireflies.py:258‑264](../../../services/ingest/ingestion/handlers/fireflies.py#L258-L264)).

Both call `_transcript_draft`, which produces **one** observation:

| Field | Value | Code |
|-------|-------|------|
| `source_channel` | `fireflies:transcript` | [handlers/fireflies.py:43](../../../services/ingest/ingestion/handlers/fireflies.py#L43) |
| `external_id` | `fireflies:{workspace_id}:transcript:{transcript_id}:{version}` | [handlers/fireflies.py:146‑149](../../../services/ingest/ingestion/handlers/fireflies.py#L146-L149) |
| `occurred_at` | first present of `dateTime` / `date` / `createdAt`, else now | [handlers/fireflies.py:157‑162](../../../services/ingest/ingestion/handlers/fireflies.py#L157-L162) |
| `kind` | `signal` (a meeting happened; its content is the signal) | [handlers/fireflies.py:194](../../../services/ingest/ingestion/handlers/fireflies.py#L194) |
| `trust_tier` | `attested_agent` | [handlers/fireflies.py:44](../../../services/ingest/ingestion/handlers/fireflies.py#L44), [195](../../../services/ingest/ingestion/handlers/fireflies.py#L195) |
| `source_actor_ref` | `None` (a meeting has no single human author) | [handlers/fireflies.py:197](../../../services/ingest/ingestion/handlers/fireflies.py#L197) |
| `content_text` | `"{title} · {first 5 participants}"` (truncated to 600) | [handlers/fireflies.py:165‑168](../../../services/ingest/ingestion/handlers/fireflies.py#L165-L168) |
| `entities_hint` | `fireflies_workspace`, `meeting`, one `person` per participant | [handlers/fireflies.py:170‑175](../../../services/ingest/ingestion/handlers/fireflies.py#L170-L175) |

The `content` dict carries the structured transcript core (workspace, transcript
id, title, participants, date, version) plus an additive `_transcript_extras`
merge — summary text, action items, duration, meeting URL, organizer email,
calendar id, Fireflies user id — keeping only non‑None keys
([handlers/fireflies.py:103‑130](../../../services/ingest/ingestion/handlers/fireflies.py#L103-L130),
[177‑188](../../../services/ingest/ingestion/handlers/fireflies.py#L177-L188)). A record missing
`workspace_id` or transcript `id` is rejected with a `ValidationError`
([handlers/fireflies.py:142‑145](../../../services/ingest/ingestion/handlers/fireflies.py#L142-L145)).

### 6.1 Trust tier — `attested_agent`

Fireflies is an AI notetaker: an *attesting agent* transcribing what humans said,
**not** the system of record. So it gets `attested_agent` — the same tier as
`slack:message`, `gmail:`, and `discord:message`
([handlers/fireflies.py:24‑27](../../../services/ingest/ingestion/handlers/fireflies.py#L24-L27)). The
handler **self‑registers** this at import via
`CHANNEL_TRUST_MAP.setdefault("fireflies:transcript", "attested_agent")`
([handlers/fireflies.py:272](../../../services/ingest/ingestion/handlers/fireflies.py#L272)) — note
`fireflies:transcript` is *not* in the static `CHANNEL_TRUST_MAP` literal
([handlers/__init__.py:41](../../../services/ingest/ingestion/handlers/__init__.py#L41)); the
`setdefault` is the source of truth. The channel is bound to the handler by
`@register("fireflies:transcript")`
([handlers/fireflies.py:224](../../../services/ingest/ingestion/handlers/fireflies.py#L224),
[handlers/__init__.py:113](../../../services/ingest/ingestion/handlers/__init__.py#L113)).

---

## 7. Live (real‑time) ingestion via HMAC‑signed webhooks

When a transcript completes, Fireflies **POSTs a webhook** to Fyralis's webhook
edge. Live and backfill both land on the **same** `fireflies:transcript` handler.
The router maps provider `fireflies` → channel `fireflies:transcript`
([webhooks/router.py:461](../../../services/app/webhooks/router.py#L461)).

### 7.1 Signature verification (HMAC‑SHA256, no timestamp)

The inbound body is verified by `FirefliesVerifier`
([signatures/fireflies.py:51‑93](../../../services/app/webhooks/signatures/fireflies.py#L51-L93)),
registered as `VERIFIERS["fireflies"] = fireflies.verifier`
([signatures/__init__.py:44‑61](../../../services/app/webhooks/signatures/__init__.py#L44-L61)).
It reads the signature from the **`x-hub-signature`** header, computes
`HMAC-SHA256(secret, raw_body)`, and constant‑time compares. Each active secret
is tried in turn so a rotation (two valid secrets in flight) verifies.

Like GitHub/Brex, the digest is over the **body alone — no timestamp envelope —
so there is no replay window**; idempotency is the versioned `external_id` at the
ingestion layer ([signatures/fireflies.py:15‑20](../../../services/app/webhooks/signatures/fireflies.py#L15-L20)).
`VerifiedContext.signed_timestamp` is `None`
([signatures/fireflies.py:88‑93](../../../services/app/webhooks/signatures/fireflies.py#L88-L93)).

> **PARTIALLY VERIFIED signature.** The **header name `x-hub-signature` is
> confirmed** against Fireflies' docs. The digest **encoding** (hex vs base64) and
> the **prefix** (`sha256=` vs none) are *not* spelled out in the docs, so the
> verifier keeps the GitHub‑style default (`sha256=` + hex) behind two knobs
> (`_PREFIX`, `_DIGEST_ENCODING`) that remain TODO
> ([signatures/fireflies.py:38‑42](../../../services/app/webhooks/signatures/fireflies.py#L38-L42)).
> Reproduced TODOs:
>
> > **TODO(human):** confirm prefix ("" if none)
> > **TODO(human):** confirm "hex" vs "base64"

### 7.2 Tenant resolution

The tenant is resolved by `_extract_fireflies`, which reads the workspace id from
the webhook body's top‑level `workspaceId` (camel) — falling back to
`workspace_id` — and looks up `provider_installations` for
`(provider='fireflies', installation_id=workspace_id)`
([tenant_resolver.py:434‑447](../../../services/app/webhooks/tenant_resolver.py#L434-L447),
registered at [tenant_resolver.py:536](../../../services/app/webhooks/tenant_resolver.py#L536)). The
`fireflies_installations_workspace_idx` partial index makes this the hot‑path
lookup ([0099_fireflies.sql:71‑73](../../../db/migrations/0099_fireflies.sql#L71-L73)).

> **TODO(human):** confirm fireflies webhook tenant‑id field against Fireflies
> webhook docs ([tenant_resolver.py:442‑443](../../../services/app/webhooks/tenant_resolver.py#L442-L443)).

### 7.3 Kafka cutover vs. inline ingest

Fireflies is wired for the Kafka cutover. When the tenant's
`kafka_path_enabled` flag is TRUE, the router **publishes the verified envelope to
Kafka and returns `202`** (the writer pool produces the observation downstream);
the inline `ingest()` is skipped
([webhooks/router.py:160‑191](../../../services/app/webhooks/router.py#L160-L191),
[1037‑1070](../../../services/app/webhooks/router.py#L1037-L1070)). When the flag is **off** (or the
Kafka publish fails as graceful degradation), the router falls back to **inline
ingest on channel `fireflies:transcript`**
([webhooks/router.py:1071‑1095](../../../services/app/webhooks/router.py#L1071-L1095)). The
backfill/poll/webhook → channel mapping that the inline normalizer relies on is in
`channel_mapping.py` — all three `(fireflies, backfill|poll|webhook)` keys map to
`fireflies:transcript` ([channel_mapping.py:220‑222](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L220-L222)).

### 7.4 No lifecycle / ping events

Unlike GitHub, Fireflies has **no `installation`/`ping` lifecycle branch** in the
router — there is no `install/uninstall/suspend` webhook event modelled. The only
webhook event the handler accepts is a transcript‑completion type; anything else
is a `ValidationError`
([handlers/fireflies.py:254‑256](../../../services/ingest/ingestion/handlers/fireflies.py#L254-L256)).

---

## 8. Reconciliation — gap detection

`reconcile_fireflies` ([reconcilers/fireflies.py:134‑171](../../../services/ingest/ingestion/reconcilers/fireflies.py#L134-L171))
re‑checks each completed workspace shard for transcripts newer than its
high‑water mark, using **one cheap query per workspace**
([reconcilers/fireflies.py:83‑131](../../../services/ingest/ingestion/reconcilers/fireflies.py#L83-L131)):

1. Load the shard's `high_water_created` from its persisted cursor
   ([reconcilers/fireflies.py:68‑76](../../../services/ingest/ingestion/reconcilers/fireflies.py#L68-L76)).
   If there's no reference point (empty workspace), skip.
2. Probe `list_transcripts(limit=1, offset=0, start=high_water[:10])`. If the
   newest returned transcript's `dateTime` is **strictly newer** than the
   high‑water, there's a gap
   ([reconcilers/fireflies.py:97‑116](../../../services/ingest/ingestion/reconcilers/fireflies.py#L97-L116)).
3. On a gap, reshare a `fireflies_transcripts` shard at **`recency_score=1.5`**,
   **warm‑started** at the high‑water (`transcript_cursor=high_water`) so the
   re‑walk runs in incremental mode and only re‑fetches the changed tail
   ([reconcilers/fireflies.py:118‑131](../../../services/ingest/ingestion/reconcilers/fireflies.py#L118-L131)).

A failed probe is logged and treated as "no gap" (best‑effort)
([reconcilers/fireflies.py:101‑106](../../../services/ingest/ingestion/reconcilers/fireflies.py#L101-L106)). The
design is deliberately conservative: it **can over‑reshare but never
under‑reshares**, and the versioned `external_id` dedup makes re‑walks idempotent
([reconcilers/fireflies.py:9‑12](../../../services/ingest/ingestion/reconcilers/fireflies.py#L9-L12)). The
reconciler needs a pool provider registered at service startup via
`set_pool_provider` ([reconcilers/fireflies.py:41‑52](../../../services/ingest/ingestion/reconcilers/fireflies.py#L41-L52)).

---

## 9. Revocation chokepoint — ABSENT

**Fireflies has no revocation chokepoint.** Unlike GitHub (which disables the
install on `401 Bad credentials` / specific `404`s) and the Notion path (which
parks + disables on token revocation), the Fireflies client maps a `401`/`403` to
a typed `fireflies_api_unauthorized` `FirefliesApiError` and **propagates it** —
there is no `_disable_installation` / `disabled_at`‑setting path on auth failure
anywhere in the Fireflies modules
([client.py:192‑196](../../../services/ingest/integrations/fireflies/client.py#L192-L196),
[client.py:256‑261](../../../services/ingest/integrations/fireflies/client.py#L256-L261)). A revoked
token therefore **fails the shard** on its next read rather than gracefully
disabling the install. (The `disabled_at` column exists on
`fireflies_installations` and is honoured by the reconciler's
`WHERE disabled_at IS NULL` filter, but is only *cleared* by re‑finalize — never
*set* by ingestion on revocation.) Recovery is operator‑driven: re‑run the connect
wizard with a fresh token.

> **(inferred)** This is a gap relative to the GitHub/Notion sources, not a
> documented decision — no rationale appears in the code. Whether Fireflies should
> grow a revocation chokepoint is left to the source owner.

---

## 10. End‑to‑end summary

```
                          ┌──────────────── BACKFILL / POLL (pull) ─────────────────┐
                          │  Bearer API token (long-lived; resolved once, never     │
                          │    refreshed, never logged)                             │
   ONE WORKSPACE          │  planner: workspace_id off the install row              │
                          │     └─► ONE fireflies_transcripts shard / workspace      │
                          │  fetcher: GET /transcripts (offset/limit; newest-first) │
                          │     FULL: offset 0   |   INCREMENTAL: start=high_water  │
                          │     └─► tag {_fyralis_record_type:"transcript", ...}     │
                          └─────────────────────────────────────────────────────┬───┘
                                                                                 │
                          ┌──────────────────── LIVE (push) ──────────────────┐  │
   transcript completes ──►  Fireflies webhook ──HTTP POST──► /webhooks/fireflies│
                          │     verify x-hub-signature (HMAC-SHA256, no ts)    │  │
                          │     resolve tenant from body workspaceId           │  │
                          │     kafka_path on → 202 ; off → inline ingest      │  │
                          └────────────────────────────────────────────────────┘  │
                                                                                 │
                                                  ┌──────────────────────────────▼─┐
                                                  │  handle_fireflies_transcript     │
                                                  │  branch on shape (type vs tag)   │
                                                  │  external_id =                   │
                                                  │   fireflies:{ws}:transcript:     │
                                                  │   {id}:{version}                 │
                                                  │  kind=signal, attested_agent     │
                                                  │  → ObservationDraft              │
                                                  └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, poll, and live webhook all land
   on `fireflies:transcript` with the same `external_id`. A backfilled transcript
   and its live twin dedup to one observation.
2. **Versioned `external_id`.** The `{version}` segment (content version) lets a
   *re‑processed* transcript re‑observe while an *identical* re‑fetch dedups —
   because the observations repo dedups on `(channel, external_id)` ignoring
   `occurred_at`.
3. **One credential model.** A single long‑lived **Bearer API token**, resolved
   once and never refreshed; no OAuth bounce. The webhook HMAC secret is a separate
   secret.
4. **One shard per workspace, two sync modes.** The planner emits a single
   `fireflies_transcripts` shard; the fetcher runs FULL (offset 0) or INCREMENTAL
   (`start=high_water`) based on the warm‑start cursor.
5. **No webhook replay window** (signed body only); idempotency is the
   `external_id` dedup. **No revocation chokepoint** — a revoked token fails the
   shard rather than disabling the install (§9).

---

## 11. Configuration & compliance

The Fireflies **API host and webhook header are confirmed** against Fireflies'
docs; the **read surface, pagination, rate‑limit signal, and signature
encoding/prefix are cloned from Brex and remain UNVERIFIED** (see the reproduced
TODOs throughout). This section reflects that split honestly.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `FIREFLIES_API_BASE_URL` | `https://api.fireflies.ai` | API host override; canonical default + env mapping in [endpoints.py:84](../../../lib/integrations/endpoints.py#L84), [135](../../../lib/integrations/endpoints.py#L135) |
| `FIREFLIES_BACKFILL_PAGE_SIZE` | `50` (capped at 500) | transcript list page size ([fetchers/fireflies.py:63‑67](../../../services/ingest/ingestion/fetchers/fireflies.py#L63-L67)) |
| `FIREFLIES_RL_MAX_ATTEMPTS` | `4` | 429 retry budget ([client.py:156](../../../services/ingest/integrations/fireflies/client.py#L156)) |
| `FIREFLIES_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:157](../../../services/ingest/integrations/fireflies/client.py#L157)) |

Per‑install overrides (`base_url`, `workspace_id`, `webhook_secret`) are supplied
through the connect wizard body, not env (§2.2).

### 11.2 Status checklist

- **Bearer auth** — long‑lived token, resolved once, no refresh, never logged. ✅
- **Webhook signing** — HMAC‑SHA256 over the body, header `x-hub-signature`,
  constant‑time compare, multi‑secret rotation. Header ✅; **prefix + digest
  encoding TODO** ⚠️.
- **No replay window** — body‑only signature; idempotency via versioned
  `external_id`. ✅
- **Tenant isolation** — `fireflies_installations` ENABLEs + FORCEs RLS
  (`app.current_tenant`); `source='fireflies'` admitted on all four M6 substrate
  tables ([0099_fireflies.sql:78‑128](../../../db/migrations/0099_fireflies.sql#L78-L128)). ✅
- **API surface** — REST `/transcripts`, `/transcript/{id}`, `/workspace`
  **cloned from Brex; real API is GraphQL** — TODO ⚠️.
- **Pagination** — offset/limit; maps onto GraphQL `skip`/`limit` (max 50) but
  unverified — TODO ⚠️.
- **Rate limits** — no dedicated bucket; only the client's `Retry-After` retry;
  429 signal unverified — TODO ⚠️.
- **Revocation chokepoint** — **absent** (§9).

### 11.3 Dev / Provider Lab mode

For local testing against Provider Lab, `build_fireflies_client`
detects Provider Lab mode and **presets** the token to `spam-fireflies`, skipping the
secret store entirely, and points the API base at Provider Lab's
`/fireflies` sub‑path via the endpoint resolver
([_clients.py:543‑567](../../../services/ingest/ingestion/fetchers/_clients.py#L543-L567)).
The canonical
[Provider Lab Fireflies adapter](../../../services/ingest/synthetic/provider_lab/wave_b.py)
implements the **same REST surface the client clones** — `GET /workspace`,
`GET /transcripts?limit&offset&start` (full vs `start=` incremental), and
`GET /transcript/{id}` — so Provider Lab exercises the cloned REST path, not the
(still‑unwired) GraphQL
path. The synthetic webhook harness sends `workspaceId` explicitly so
tenant resolution matches ([tenant_resolver.py:440](../../../services/app/webhooks/tenant_resolver.py#L440)).
