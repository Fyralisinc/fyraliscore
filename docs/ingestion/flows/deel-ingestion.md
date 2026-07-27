# Deel Ingestion — How Fyralis Pulls Deel Data

This document explains, in detail, **how Deel data enters Fyralis**: which Deel
REST endpoints are called, with which token, and how the two Deel signal types —
**contractor payments** and **contract-state snapshots** — are each ingested.

It deliberately stops at the point where a Deel payment/contract becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Read me first — verification status.** Deel ships as the **IN-FIN2
> Bearer / Mercury archetype**. Several wire-level details (read endpoints,
> pagination shape, webhook signature scheme, the webhook tenant-id field) are
> **modelled on the Mercury archetype and not yet confirmed against Deel's API
> docs** — the code carries explicit `TODO(human)` markers at each spot. This
> doc reproduces those callouts verbatim where they apply; treat anything so
> labelled as **(inferred, unverified)** rather than confirmed Deel behaviour.

---

## 1. The ways data arrives

Unlike Slack (two paths) or GitHub (two paths), Deel reaches Fyralis through
**three ingress kinds that converge on one handler and one dedup namespace**:

| Path | Ingress kind | Trigger | Mechanism | Code |
|------|-------------|---------|-----------|------|
| **Backfill (historical)** | `backfill` | Onboarding | Fyralis *pulls* each contract's full payment history via the Deel **REST API** (`GET /contract/{id}/payments`) | [planners/deel.py](../../../services/ingest/ingestion/planners/deel.py), [fetchers/deel.py](../../../services/ingest/ingestion/fetchers/deel.py) |
| **Poll (incremental)** | `poll` | Reconciliation / warm re-run | Same fetcher, warm-started at the per-contract payment high-water cursor (`start=<date>`) | [fetchers/deel.py:138‑165](../../../services/ingest/ingestion/fetchers/deel.py#L138-L165), [reconcilers/deel.py](../../../services/ingest/ingestion/reconcilers/deel.py) |
| **Live (real-time)** | `webhook` | A payment/contract changes in Deel | Deel *pushes* an **HMAC-signed webhook** delivery to Fyralis | [webhooks/router.py](../../../services/app/webhooks/router.py), [signatures/deel.py](../../../services/app/webhooks/signatures/deel.py), [handlers/deel.py](../../../services/ingest/ingestion/handlers/deel.py) |

> A real Deel **push webhook surface does exist in the code** — the verifier,
> the `provider_installations` row, the tenant-resolver entry, and the
> `("deel","webhook")` channel mapping are all wired. The *signature scheme
> itself* is the unverified part (see §7.1), not the existence of the path.

All three converge on the **single** `deel:payment` handler
([handlers/deel.py](../../../services/ingest/ingestion/handlers/deel.py)) — wired
by `@register("deel:payment")`
([handlers/deel.py:299‑302](../../../services/ingest/ingestion/handlers/deel.py#L299-L302)).
The channel-mapping table makes the convergence explicit
([normalizer/channel_mapping.py:198‑209](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L198-L209)):

```
("deel", "backfill") -> "deel:payment"
("deel", "poll")     -> "deel:payment"
("deel", "webhook")  -> "deel:payment"
```

### 1.1 The single dedup key (exact shape)

Both Deel record types use a **versioned** `external_id` so a status/contract
mutation lands as a *new* observation rather than silently dedup-ing
(the observations repo dedups on `(source_channel, external_id)` **ignoring**
`occurred_at` — see [handlers/deel.py:23‑28](../../../services/ingest/ingestion/handlers/deel.py#L23-L28)):

```
payment            external_id = deel:{contract_id}:payment:{payment_id}:{status}
contract_snapshot  external_id = deel:{contract_id}:contract:{updated}
```

These are minted centrally in `idempotency`
([idempotency/__init__.py:232‑242](../../../services/ingest/ingestion/idempotency/__init__.py#L232-L242)):

```python
def deel_payment(contract_id, payment_id, status):
    return f"deel:{contract_id}:payment:{payment_id}:{status}"

def deel_contract(contract_id, updated):
    return f"deel:{contract_id}:contract:{updated}"
```

Because the handler mints the **same** key from a backfilled record and from a
live-webhook event, a payment that is both backfilled *and* delivered live
collapses into **one** observation **per status**. This is the central design
invariant of Deel ingestion. Status is part of the key by design: a payment that
walks `pending → sent → failed` produces **three** distinct observations, each
its own row ([handlers/deel.py:23‑28](../../../services/ingest/ingestion/handlers/deel.py#L23-L28)).

---

## 2. Authentication & token model

Deel uses a **single credential model: one long-lived API token presented as a
`Bearer` token** — the Mercury/Jira archetype, *not* a per-user OAuth bot token
and *not* an access+refresh pair.

- The token is resolved **once** from the secret store (via the install's
  `secret_ref`) and reused for the life of the client; in Provider Lab mode it is
  preset ([client.py:102‑124](../../../services/ingest/integrations/deel/client.py#L102-L124)).
- The auth header is `Authorization: Bearer {token}`
  ([client.py:126‑128](../../../services/ingest/integrations/deel/client.py#L126-L128)).
- The token and the auth header are **never logged**
  ([client.py:24](../../../services/ingest/integrations/deel/client.py#L24));
  `DeelApiError` keeps the token off its error context by design
  ([oauth.py:99‑117](../../../services/ingest/integrations/deel/oauth.py#L99-L117)).

> **TODO(human)** *(reproduced from [client.py:9‑12](../../../services/ingest/integrations/deel/client.py#L9-L12))*:
> confirm Deel read endpoints + OAuth scopes against the Deel API docs before
> prod. The paths below follow the Mercury archetype's shape — only the verified
> read surface should ship.

> **TODO(human)** *(reproduced from [oauth.py:11‑13](../../../services/ingest/integrations/deel/oauth.py#L11-L13))*:
> confirm Deel does **not** issue refresh tokens. The connect wizard assumes a
> long-lived static token. If Deel ever moves to OAuth access+refresh, this
> becomes a QBO-shaped preflight/finalize and needs a refresh seam — none exists
> today.

### 2.1 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| API token | secret store, label `deel_api_token:{base_url}` | only an opaque `secret_ref` reaches `deel_installations.secret_ref` ([oauth.py:186‑188](../../../services/ingest/integrations/deel/oauth.py#L186-L188)) |
| Webhook signing secret | secret store, label `deel_webhook_secret:{base_url}` | optional; only set when the admin supplies one ([oauth.py:189‑194](../../../services/ingest/integrations/deel/oauth.py#L189-L194)) |
| Organization id (webhook installation_id) | `deel_installations.organization_id` + `provider_installations.installation_id` | identifies the tenant on the live edge ([onboarding.py:127‑149](../../../services/ingest/integrations/deel/onboarding.py#L127-L149)) |

### 2.2 The connect wizard (how an install gets registered)

There is **no public OAuth redirect dance** — Deel uses an **admin connect
wizard** (Bearer-authed, Jira-shaped), implemented in
[integrations/deel/oauth.py](../../../services/ingest/integrations/deel/oauth.py):

1. **`POST /integrations/deel/connect/preflight`** — verifies the API token by
   calling `DeelClient.list_contracts()` and returns the contract list for the
   selector UI; an auth failure returns a structured `400` with **no secret
   stored** ([oauth.py:129‑149](../../../services/ingest/integrations/deel/oauth.py#L129-L149)).
2. **`POST /integrations/deel/connect/finalize`** — re-verifies the creds
   **before any write**, resolves the contract set (all, or a `contract_ids`
   subset), persists the API token (+ optional webhook secret) to the secret
   store, then calls `finalize_install`
   ([oauth.py:152‑230](../../../services/ingest/integrations/deel/oauth.py#L152-L230)).
3. `finalize_install` UPSERTs **`deel_installations`** (keyed on
   `(tenant_id, base_url)`, idempotent), INSERTs one **`deel_contracts`** row per
   contract, and emits an **`onboarding_triggers`** row (`source='deel'`,
   `trigger_kind='install'`) so the M6 backfill chain fires — all in one
   tenant-scoped transaction ([onboarding.py:36‑124](../../../services/ingest/integrations/deel/onboarding.py#L36-L124)).
4. The **live webhook edge** is seeded *only* when both an `organization_id` and
   a `webhook_secret` were supplied: `register_webhook_installation` UPSERTs the
   **`provider_installations`** row (`provider='deel'`,
   `installation_id=organization_id`, `secret_ref=<webhook secret>`,
   `enabled=TRUE`) ([oauth.py:207‑217](../../../services/ingest/integrations/deel/oauth.py#L207-L217),
   [onboarding.py:127‑149](../../../services/ingest/integrations/deel/onboarding.py#L127-L149)).

> **Backfill and live are seeded together but stay independent.** Backfill reads
> `deel_installations`; the webhook edge reads `provider_installations`. An
> install with no `organization_id`/`webhook_secret` backfills+polls happily but
> has **no live coverage** ([onboarding.py:14‑18](../../../services/ingest/integrations/deel/onboarding.py#L14-L18)).

---

## 3. The Deel REST API surface actually called

All read calls funnel through `DeelClient._request`
([client.py:130‑190](../../../services/ingest/integrations/deel/client.py#L130-L190)),
which:

- sets `Authorization: Bearer {token}` and `Accept: application/json`,
- honours `Retry-After` on **`429`** within a bounded budget
  (`DEEL_RL_MAX_ATTEMPTS`=4, `DEEL_RL_MAX_SLEEP_SEC`=30 s)
  ([client.py:150‑173](../../../services/ingest/integrations/deel/client.py#L150-L173)),
- maps any non-2xx to a typed `DeelApiError` (`401/403` →
  `deel_api_unauthorized`, `404` → `deel_api_not_found`, exhausted `429` →
  `deel_api_rate_limited`) ([client.py:252‑283](../../../services/ingest/integrations/deel/client.py#L252-L283)).

The endpoints invoked for ingestion:

| Deel endpoint | Wrapper | Purpose | Code |
|---------------|---------|---------|------|
| `GET /contracts` | `list_contracts()` | enumerate all contracts visible to the token (seed/install + contract enumeration) | [client.py:196‑207](../../../services/ingest/integrations/deel/client.py#L196-L207) |
| `GET /contract/{id}` | `get_contract()` | one contract — the per-shard contract-state snapshot probe | [client.py:209‑211](../../../services/ingest/integrations/deel/client.py#L209-L211) |
| `GET /contract/{id}/payments` | `list_payments()` | paginated payments (optional `start=` date lower bound for incremental polls) | [client.py:213‑238](../../../services/ingest/integrations/deel/client.py#L213-L238) |

> **TODO(human)** *(reproduced from [client.py:9‑12](../../../services/ingest/integrations/deel/client.py#L9-L12))*:
> the three paths above follow the Mercury archetype — verify them (and any
> required token scopes) against the Deel API docs before prod.

### 3.1 Pagination — `limit` + `offset`

List endpoints are assumed to return `{total, contracts|payments: [...]}` and
accept `limit` + `offset`
([client.py:20‑23](../../../services/ingest/integrations/deel/client.py#L20-L23)).
`list_payments` returns `(payments, next_offset, total)`; `next_offset is None`
is the terminal signal, computed from `offset + len(page) >= total or empty page`
([client.py:227‑238](../../../services/ingest/integrations/deel/client.py#L227-L238)).
Page size defaults to **100**, capped at Deel's stated **500**
([client.py:43‑45](../../../services/ingest/integrations/deel/client.py#L43-L45),
[fetchers/deel.py:62‑66](../../../services/ingest/ingestion/fetchers/deel.py#L62-L66), `DEEL_BACKFILL_PAGE_SIZE`).
`list_contracts` tolerates either a `{contracts: [...]}` envelope **or** a bare
list ([client.py:202‑207](../../../services/ingest/integrations/deel/client.py#L202-L207)).

> **TODO(human)** *(reproduced from [fetchers/deel.py:3‑7](../../../services/ingest/ingestion/fetchers/deel.py#L3-L7))*:
> confirm Deel pagination + created-since filter. The archetype assumes
> offset/limit pagination with a date-granular `start=` lower bound; Deel's real
> API may be cursor- or page-token-based and may expose a different "created
> since" field. Kept configurable via `DEEL_BACKFILL_PAGE_SIZE`.

### 3.2 Rate limits — **no dedicated token bucket**

Unlike Slack (per-method buckets) and GitHub (`("github","rest_authenticated")`),
**Deel has no entry in `rate_limit/buckets.py`** — the `BUCKET_DEFAULTS` table
covers only Slack, GitHub, Gmail, and Discord
([rate_limit/buckets.py:82‑90](../../../services/ingest/ingestion/rate_limit/buckets.py#L82-L90)).
Deel's only rate-limit defence is the **client-side `Retry-After`-aware 429
retry loop** in `_request` (§3) plus the bounded `DEEL_RL_*` budget.

> **TODO(human)** *(reproduced from [client.py:14‑18](../../../services/ingest/integrations/deel/client.py#L14-L18))*:
> confirm Deel rate-limit signalling (429 + Retry-After vs `X-RateLimit-Reset`).
> The archetype defaults to 429 + `Retry-After`; tune via `DEEL_RL_MAX_ATTEMPTS`
> / `DEEL_RL_MAX_SLEEP_SEC`.

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one shard per active contract**, all of
`shard_kind = "deel_contract_payments"`
([planners/deel.py:54‑86](../../../services/ingest/ingestion/planners/deel.py#L54-L86)).
There is exactly **one shard family** (contrast GitHub's two fetch classes).

The planner reads **DB state only** — `ctx.source_client` is `None`. The
contracts were enumerated at seed time (`DeelClient.list_contracts`) and persisted
to `deel_contracts`; the SourceOnboarding loader JSON-aggregates them into
`ctx.install["contracts"]` so the planner stays stateless (no DB I/O), exactly
like Jira/Calendar/Gmail ([planners/deel.py:1‑20](../../../services/ingest/ingestion/planners/deel.py#L1-L20),
[38‑51](../../../services/ingest/ingestion/planners/deel.py#L38-L51)).

Each shard carries `contract_id`, `contract_name`, `installation_id`, and the
high-water `payment_cursor` (`None` on first sync), at `recency_score=1.0`
([planners/deel.py:64‑80](../../../services/ingest/ingestion/planners/deel.py#L64-L80)).

> **TODO(human)** *(reproduced from [planners/deel.py:11‑13](../../../services/ingest/ingestion/planners/deel.py#L11-L13))*:
> confirm the Deel resource taxonomy to shard on. If the verified read surface
> is org-wide rather than per-contract, collapse to one shard per install.

---

## 5. Fetch specifics — one shard kind, two sync modes, a contract→N fan-out

`fetch_page_deel` ([fetchers/deel.py:123‑200](../../../services/ingest/ingestion/fetchers/deel.py#L123-L200))
takes one `(install, shard_identifier, cursor)` triple and returns one page of
records + the next cursor, under the N1 backfill contract (ShardFetch loops it,
persisting the cursor between calls).

### 5.1 Two sync modes

- **FULL (initial backfill).** Walk `GET /contract/{id}/payments` from `offset=0`,
  paginated, until `next_offset is None`
  ([fetchers/deel.py:159‑198](../../../services/ingest/ingestion/fetchers/deel.py#L159-L198)).
- **INCREMENTAL (poll).** When the shard is warm-started with a `payment_cursor`
  (the high-water payment `createdAt`), the fetcher freezes it as the run's
  `incremental_floor` and passes `start=<date>` so only recent payments come
  back; the overlap re-fetch dedups via the versioned `external_id`
  ([fetchers/deel.py:138‑144](../../../services/ingest/ingestion/fetchers/deel.py#L138-L144),
  [159‑165](../../../services/ingest/ingestion/fetchers/deel.py#L159-L165)).
  Deel's `start=` is date-granular, so the cursor's ISO timestamp is truncated to
  its date via `_iso_date` ([fetchers/deel.py:109‑113](../../../services/ingest/ingestion/fetchers/deel.py#L109-L113)).

### 5.2 The cursor (payment high-water)

```python
class DeelCursor:
    offset: int = 0                       # list-payments pagination offset within a run
    high_water_created: str | None = None # max payment createdAt seen — warm-start lower bound + reconciler reference
    incremental_floor: str | None = None  # the start= lower bound frozen for this run (None in FULL mode)
    payments_seen: int = 0                # diagnostic
    seeded: bool = False                  # whether first-call setup (snapshot emit) ran
```

([fetchers/deel.py:69‑89](../../../services/ingest/ingestion/fetchers/deel.py#L69-L89)).
`high_water_created` advances over every payment's `createdAt`/`postedAt`
([fetchers/deel.py:116‑120](../../../services/ingest/ingestion/fetchers/deel.py#L116-L120),
[183](../../../services/ingest/ingestion/fetchers/deel.py#L183)) — it is both the
warm-start floor *and* the reconciler's gap reference point (§8).

### 5.3 Fan-out: one contract → N records

On the **first call** of a shard (`not cur.seeded`), the fetcher emits **one**
`contract_snapshot` record (the current contract state from `get_contract`) so
the contract-position signal lands alongside the payment history; a failed
snapshot fetch is swallowed (the payment walk still proceeds)
([fetchers/deel.py:138‑157](../../../services/ingest/ingestion/fetchers/deel.py#L138-L157)).
Thereafter each payment becomes one `payment` record
([fetchers/deel.py:177‑183](../../../services/ingest/ingestion/fetchers/deel.py#L177-L183)).
Each record is tagged with a private `_fyralis_record_type` ∈
`{"contract_snapshot","payment"}` that the handler branches on
([fetchers/deel.py:28‑40](../../../services/ingest/ingestion/fetchers/deel.py#L28-L40)).

A mid-page `deel_api_rate_limited` error is treated as a **soft stop**: the
fetcher returns the records gathered so far with `end_of_data=False` so the run
resumes next invocation ([fetchers/deel.py:166‑175](../../../services/ingest/ingestion/fetchers/deel.py#L166-L175)).

---

## 6. The handler — shaping records into `ObservationDraft`

`handle_deel_payment` ([handlers/deel.py:299‑342](../../../services/ingest/ingestion/handlers/deel.py#L299-L342))
is a **pure function** (no DB / network). It branches on the input shape:

- **Live webhook**: a raw Deel body with a `type` (`payment.*` / `contract.*`)
  ([handlers/deel.py:306‑325](../../../services/ingest/ingestion/handlers/deel.py#L306-L325)).
- **Backfill/poll**: a fetcher-tagged record (`_fyralis_record_type`)
  ([handlers/deel.py:327‑342](../../../services/ingest/ingestion/handlers/deel.py#L327-L342)).

Both shapes route through the **same two draft builders**, so a webhook-delivered
change and its backfill twin dedup:

| Record | Builder | `external_id` | `occurred_at` | `kind` | Trust tier |
|--------|---------|---------------|---------------|--------|------------|
| `payment` (status pending/sent/posted/paid) | `_payment_draft` | `deel:{contract}:payment:{id}:{status}` | `postedAt` ▸ `createdAt` ▸ now | `signal` | **authoritative** |
| `payment` (status **failed/rejected**) | `_payment_draft` | (same) | (same) | **`state_change`** (cash-risk) | **authoritative** |
| `contract_snapshot` | `_contract_snapshot_draft` | `deel:{contract}:contract:{updated}` | `updated`/`as_of` ▸ now | `signal` | **authoritative** |

- **Channel:** `deel:payment`. Confirmed via `@register("deel:payment")`
  ([handlers/deel.py:299](../../../services/ingest/ingestion/handlers/deel.py#L299)) and
  the `CHANNEL_TRUST_MAP.setdefault("deel:payment", "authoritative")` registration
  ([handlers/deel.py:345](../../../services/ingest/ingestion/handlers/deel.py#L345)).
- **Trust posture:** Deel is the system of record for contractor payments →
  every draft is `authoritative` ([handlers/deel.py:29‑31](../../../services/ingest/ingestion/handlers/deel.py#L29-L31),
  [47‑48](../../../services/ingest/ingestion/handlers/deel.py#L47-L48)).
- **`source_actor_ref`** is always `None` (payments/contracts have no human
  message author) ([handlers/deel.py:224](../../../services/ingest/ingestion/handlers/deel.py#L224)).

### 6.1 Highlights

- **failed/rejected → `state_change`.** A payment whose status is in
  `{failed, rejected}` is emitted as `kind=state_change` (the cash-risk signal),
  and the `reasonForFailure` is folded inline into `content_text`
  ([handlers/deel.py:51‑52](../../../services/ingest/ingestion/handlers/deel.py#L51-L52),
  [182‑192](../../../services/ingest/ingestion/handlers/deel.py#L182-L192)).
- **PII redaction.** Bank identifiers (`accountNumber`, `routingNumber`, `iban`)
  in a payment's `details` are masked to the last 4 chars before they reach the
  reasoning layer / LLM context ([handlers/deel.py:98‑119](../../../services/ingest/ingestion/handlers/deel.py#L98-L119),
  [149‑153](../../../services/ingest/ingestion/handlers/deel.py#L149-L153)).
- **Rich fields.** Failure reason, FX exposure, GL code, counterparty identity,
  memos, dashboard link are pulled into `content` (present keys only)
  ([handlers/deel.py:122‑153](../../../services/ingest/ingestion/handlers/deel.py#L122-L153)).
- **`entities_hint`** carries `deel_contract` plus an `organization`
  counterparty ref ([handlers/deel.py:194‑198](../../../services/ingest/ingestion/handlers/deel.py#L194-L198)).
- A payload that is **neither** a webhook event **nor** a tagged record is
  rejected with a `ValidationError`
  ([handlers/deel.py:339‑342](../../../services/ingest/ingestion/handlers/deel.py#L339-L342)).

---

## 7. Live (real-time) ingestion via HMAC-signed webhooks

When a payment/contract changes in Deel, Deel **POSTs a webhook delivery** to
Fyralis's webhook edge (`POST /webhooks/deel`). The router looks up the
per-provider verifier — `VERIFIERS["deel"] = deel.verifier`
([signatures/__init__.py:44‑58](../../../services/app/webhooks/signatures/__init__.py#L44-L58)) —
and `_PROVIDER_CHANNEL["deel"] = "deel:payment"` is the inline-ingest channel
([webhooks/router.py:453‑458](../../../services/app/webhooks/router.py#L453-L458)).

### 7.1 Signature verification (HMAC-SHA256, no replay window)

`DeelVerifier.verify` ([signatures/deel.py:40‑82](../../../services/app/webhooks/signatures/deel.py#L40-L82)):

```
expected = "sha256=" + hex(HMAC-SHA256(secret, raw_body))
compare(expected, Deel-Signature)   # constant-time
```

Each active secret is tried in turn (for rotation), and the digest is over the
**body alone** — there is **no timestamp envelope and no replay window** (like
GitHub/Jira). Idempotency is enforced at the ingestion layer via the versioned
`external_id`, not here ([signatures/deel.py:14‑17](../../../services/app/webhooks/signatures/deel.py#L14-L17)).
The per-tenant signing secret is resolved by
`webhooks/secrets.py::load_installation_secrets`
from the `provider_installations` row (`provider='deel'`).

> **TODO(human)** *(reproduced from [signatures/deel.py:1‑9](../../../services/app/webhooks/signatures/deel.py#L1-L9))*:
> confirm the Deel webhook signature scheme — HMAC algo, digest encoding (hex vs
> base64), header name, and prefix. **UNVERIFIED.** The default mirrors the
> Mercury archetype: HMAC-SHA256 over the raw body, hex, `sha256=` prefix,
> `Deel-Signature` header. The header name and prefix are module constants
> (`_HEADER`, `_PREFIX`) so the verified scheme drops in without touching the
> verify loop.

> **No dedicated replay cache.** Unlike GitHub (which keys a
> `(installation, delivery)` cache), Deel has **no per-delivery replay cache** in
> the router — the only replay path is GitHub's
> ([webhooks/router.py:886‑915](../../../services/app/webhooks/router.py#L886-L915)).
> For Deel, observation-layer `external_id` dedup is the sole idempotency
> backstop.

### 7.2 Tenant resolution

The tenant is resolved from the webhook body's top-level `organizationId` (or the
legacy `accountId`) → the `provider_installations` row for
`(provider='deel', installation_id=organizationId)`
([tenant_resolver.py:417‑431](../../../services/app/webhooks/tenant_resolver.py#L417-L431),
registered in the dispatch map at
[tenant_resolver.py:535](../../../services/app/webhooks/tenant_resolver.py#L535)).
Unknown/disabled installs get `401 unknown_installation`, deferred until *after*
signature verification so a tenant-id prober sees signature failures first
([webhooks/router.py:917‑926](../../../services/app/webhooks/router.py#L917-L926)).

> **TODO(human)** *(reproduced from [tenant_resolver.py:426‑427](../../../services/app/webhooks/tenant_resolver.py#L426-L427))*:
> confirm the Deel webhook tenant-id field (`organizationId` vs `accountId` vs an
> event-envelope path) against Deel webhook docs.

### 7.3 Kafka cutover vs inline (the `deel:payment` cutover)

Deel is registered in **both** `_CUTOVER_ENABLED_PROVIDERS` and the legacy
webhook-router map ([webhooks/router.py:140](../../../services/app/webhooks/router.py#L140),
[179](../../../services/app/webhooks/router.py#L179)). After verification +
tenant resolution:

- If the tenant's `ingestion.kafka_path_enabled` flag is **TRUE** (the kafka-first
  default), the verified envelope is published to Kafka and the edge returns
  **`202 accepted`**; inline `ingest()` is skipped
  ([webhooks/router.py:1036‑1070](../../../services/app/webhooks/router.py#L1036-L1070)).
- If the flag is **off** (the kill-switch) — *or* the Kafka publish fails
  (graceful degradation) — the router falls back to **inline** `ingest()` on
  channel `_PROVIDER_CHANNEL["deel"]` = **`deel:payment`**
  ([webhooks/router.py:1045‑1095](../../../services/app/webhooks/router.py#L1045-L1095),
  [1109‑1120](../../../services/app/webhooks/router.py#L1109-L1120)).

Either way the body lands on the same `deel:payment` handler.

---

## 8. Reconciliation — gap detection

`reconcile_deel` ([reconcilers/deel.py:130‑167](../../../services/ingest/ingestion/reconcilers/deel.py#L130-L167))
re-checks **completed** contract shards for payments newer than the stored
high-water:

1. Load the tenant's single active `deel_installations` row (`disabled_at IS
   NULL`); if none, no gaps ([reconcilers/deel.py:138‑148](../../../services/ingest/ingestion/reconcilers/deel.py#L138-L148)).
2. For each done shard, load `high_water_created` from the shard's persisted
   cursor; skip shards with no reference point (empty contract)
   ([reconcilers/deel.py:68‑92](../../../services/ingest/ingestion/reconcilers/deel.py#L68-L92)).
3. Probe `list_payments(contract_id, limit=1, start=high_water[:10])`; if the
   newest returned `createdAt`/`postedAt` is **strictly greater** than the
   high-water, there's a gap ([reconcilers/deel.py:93‑112](../../../services/ingest/ingestion/reconcilers/deel.py#L93-L112)).
4. On a gap, reshare a `deel_contract_payments` shard at **`recency_score=1.5`**,
   warm-started with `payment_cursor=high_water` (incremental mode) so only the
   changed tail is re-walked ([reconcilers/deel.py:114‑127](../../../services/ingest/ingestion/reconcilers/deel.py#L114-L127)).

The probe is **best-effort**: a probe exception is logged and treated as
"no gap" rather than failing the reconcile ([reconcilers/deel.py:97‑102](../../../services/ingest/ingestion/reconcilers/deel.py#L97-L102)).
By design the probe can **over-reshare but never under-reshare**; the versioned
`external_id` makes re-walks idempotent. The probe also doubles as the
**token keepalive** ([reconcilers/deel.py:8‑12](../../../services/ingest/ingestion/reconcilers/deel.py#L8-L12)).

---

## 9. Revocation chokepoint — **absent**

Deel has **no revocation chokepoint** comparable to GitHub's
`_maybe_disable_on_revocation`. On a rejected token the client raises a typed
`DeelApiError(deel_api_unauthorized)` for `401/403`
([client.py:186‑190](../../../services/ingest/integrations/deel/client.py#L186-L190),
[256‑262](../../../services/ingest/integrations/deel/client.py#L256-L262)) — but
**nothing in the Deel integration disables the install row or zeroizes the
secret** on that signal. A grep for `disable` / `revocat` across
`integrations/deel/` finds only the **install-time** writes
(`disabled_at=NULL` on UPSERT, `enabled=TRUE` on the webhook row) — there is no
disable-on-revocation path.

Practical consequences (inferred):

- A revoked API token surfaces as a `deel_api_unauthorized` error on the next
  backfill/poll/reconcile call and propagates up; the run fails rather than
  parking the install.
- Re-enabling is via the connect wizard (`finalize` re-verifies and resets
  `disabled_at=NULL` / `enabled=TRUE`).

> **TODO(human):** decide whether Deel needs a revocation chokepoint
> (disable-on-`401`/`403` + secret zeroize) analogous to GitHub's. None exists
> today; this section documents the *absence*, not a verified design choice.

---

## 10. End-to-end summary

```
                          ┌──────────────────── BACKFILL (pull) ─────────────────────┐
   ADMIN CONNECT WIZARD   │  POST /integrations/deel/connect/{preflight,finalize}      │
   (Bearer API token)     │    └─► deel_installations + deel_contracts + onboarding   │
                          │  planner: DB state only (no source_client)                │
                          │    └─► one deel_contract_payments shard PER CONTRACT       │
                          │  fetcher (FULL): GET /contract/{id}/payments (offset paged)│
                          │    └─► first call also emits 1 contract_snapshot           │
   POLL (incremental) ────►  fetcher (warm): start=<high-water date> -> changed tail   │
                          └──────────────────────────────────────────────────────────┬┘
                                                                                       │
                          ┌──────────────────── LIVE (push) ─────────────────────────┐│
   payment/contract  ─────►  Deel webhook ──HTTP POST──► /webhooks/deel               ││
   changes in Deel        │    verify Deel-Signature (HMAC-SHA256, hex, no ts) [UNVER]││
                          │    tenant <- organizationId ; kafka cutover OR inline      ││
                          └──────────────────────────────────────────────────────────┘│
                                                                                       │
                                                       ┌───────────────────────────────▼─┐
                                                       │  handle_deel_payment             │
                                                       │  branch: webhook type | tagged   │
                                                       │  external_id (versioned):         │
                                                       │   payment: deel:{c}:payment:{i}:{s}│
                                                       │   contract: deel:{c}:contract:{u} │
                                                       │  failed/rejected -> state_change  │
                                                       │  -> ObservationDraft (authoritative)
                                                       └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, poll, and webhook all land on
   `deel:payment`. The handler mints the **same** versioned `external_id` from a
   tagged record and from a webhook event, so a backfilled record and its live
   twin collapse into one observation.
2. **Status is part of the key.** `deel:{contract}:payment:{id}:{status}` —
   because the observations repo dedups ignoring `occurred_at`, a status
   transition (`pending→sent→failed`) is a *new* observation, not a silent drop.
3. **One credential model.** A single long-lived **Bearer API token** (no
   per-user OAuth, no refresh token), resolved once from the secret store; only an
   opaque `secret_ref` reaches the DB.
4. **One shard family.** `deel_contract_payments`, one per contract; the planner
   is stateless (reads pre-aggregated DB state, `source_client=None`).
5. **`limit`/`offset` pagination + bounded 429 retry; no token bucket.** Deel has
   no `rate_limit/buckets.py` entry — the client's `Retry-After` loop is the only
   throttle.
6. **No webhook replay window, no revocation chokepoint.** HMAC over the body
   alone; idempotency is the `external_id` dedup. Token revocation surfaces as a
   run error, not an install-disable.

---

## 11. Configuration & compliance

> **Compliance status: UNVERIFIED against Deel's official docs.** Deel ships as
> the Mercury archetype; the read endpoints, pagination, rate-limit signalling,
> webhook signature scheme, and webhook tenant-id field all carry `TODO(human)`
> markers in the code and are reproduced in §2/§3/§7 above. The checklist below
> records what the **code implements**, not what Deel **requires**.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `DEEL_RL_MAX_ATTEMPTS` | `4` | 429 retry budget in `DeelClient._request` ([client.py:150](../../../services/ingest/integrations/deel/client.py#L150)) |
| `DEEL_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:151](../../../services/ingest/integrations/deel/client.py#L151)) |
| `DEEL_BACKFILL_PAGE_SIZE` | `100` (capped 500) | payments page size ([fetchers/deel.py:62‑66](../../../services/ingest/ingestion/fetchers/deel.py#L62-L66)) |
| `DEEL_API_BASE_URL` | `https://api.letsdeel.com` | canonical API host; explicit Provider Lab override ([endpoints.py:80](../../../lib/integrations/endpoints.py#L80), [134](../../../lib/integrations/endpoints.py#L134)) |

> **TODO(human)** *(reproduced from [endpoints.py:80](../../../lib/integrations/endpoints.py#L80))*:
> confirm the canonical Deel API host.

### 11.2 What the code implements

- **Auth** — single long-lived `Bearer` API token, resolved once, never logged. ✅ (scheme verified in code; Deel-side correctness UNVERIFIED)
- **Webhook signing** — HMAC-SHA256 `Deel-Signature`, `sha256=` hex prefix,
  constant-time compare, all-secrets rotation loop. ✅ (implementation present; **scheme UNVERIFIED** — §7.1)
- **Pagination** — `limit`/`offset`, `next_offset is None` terminal. ✅ (implementation present; **shape UNVERIFIED** — §3.1)
- **Rate-limit etiquette** — `Retry-After` honoured on 429 within a bounded
  budget; **no dedicated token bucket**. ✅ (signalling UNVERIFIED — §3.2)
- **Least secret surface** — only opaque `secret_ref`s reach the DB; PII bank
  ids masked before the reasoning layer. ✅
- **Revocation chokepoint** — **absent** (§9). ⚠️

### 11.3 Dev / Provider Lab mode

For local testing, `build_deel_client` detects Provider Lab mode and **presets** the
API token to `spam-deel`, skipping the secret-store resolution, and points the
client base at Provider Lab's `/deel` sub-path
([_clients.py:515‑540](../../../services/ingest/ingestion/fetchers/_clients.py#L515-L540),
[endpoints.py:164](../../../lib/integrations/endpoints.py#L164)).
The canonical
[Provider Lab Deel adapter](../../../services/ingest/synthetic/provider_lab/wave_b.py)
serves the three read routes the client calls — `GET /contracts`,
`GET /contract/{id}`, `GET /contract/{id}/payments?limit&offset&start` — matching
on the path **suffix** so the `/deel` prefix is transparent.
