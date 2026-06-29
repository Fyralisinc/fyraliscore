# HiBob Ingestion — How Fyralis Pulls HiBob Data

This document explains, in detail, **how HiBob ("Bob") People/HR data enters
Fyralis**: which HiBob REST APIs are called, with which credential, and how the
HR signal set — **employee directory, lifecycle changes, time‑off requests, and
payroll runs** — is each ingested.

It deliberately stops at the point where a HiBob entity becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Provenance / verification status.** HiBob ingestion was built from a
> **Gusto‑structure / Brex‑auth archetype** (one channel, many record types;
> long‑lived Basic credential, no token refresh). The **webhook signing scheme**
> and the **`external_id` shape** are CONFIRMED against the contract; much of the
> **read surface** (collection paths, the incremental filter param, pagination
> mode, rate‑limit numbers) is **UNVERIFIED placeholder** carried verbatim from
> the archetype and tagged with `TODO(human)` in the code. Those gaps are
> reproduced as callouts below rather than papered over — see §3 and §12.

---

## 1. The three ways data arrives

HiBob data reaches Fyralis through **three paths that converge on one handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* each entity type's history via the HiBob **REST API** (offset‑paginated) | [planners/hibob.py](../../../services/ingest/ingestion/planners/hibob.py), [fetchers/hibob.py](../../../services/ingest/ingestion/fetchers/hibob.py) |
| **Poll (incremental)** | Periodic re‑run / reconciler reshare | the **same fetcher** re‑runs under `ingress_kind="poll"`, warm‑started from the per‑entity `modified` high‑water cursor | [fetchers/hibob.py:8‑20](../../../services/ingest/ingestion/fetchers/hibob.py#L8-L20), [reconcilers/hibob.py](../../../services/ingest/ingestion/reconcilers/hibob.py) |
| **Live (real‑time)** | An HR change in HiBob | HiBob *pushes* an HMAC‑signed **webhook** to Fyralis | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/hibob.py](../../../services/app/webhooks/signatures/hibob.py), [handlers/hibob.py](../../../services/ingest/ingestion/handlers/hibob.py) |

All three converge on the **single** `hibob:object` handler
([handlers/hibob.py:279‑317](../../../services/ingest/ingestion/handlers/hibob.py#L279-L317)).
The channel map proves the convergence — `("hibob","backfill")`,
`("hibob","poll")`, and `("hibob","webhook")` **all** map to `"hibob:object"`
([channel_mapping.py:291‑293](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L291-L293)).

All three derive the **same** dedup key, **versioned** by the row's modified
field (this is the CONFIRMED contract `external_id`):

```
external_id = "hibob:{company_id}:{entity_kind}:{entity_id}:{ver}"
```

where `{ver}` is the entity's `modified`/version field (or `chg:{modified}` /
`chg:{now}` for a thin webhook with no body)
([handlers/hibob.py:88‑96](../../../services/ingest/ingestion/handlers/hibob.py#L88-L96),
[224](../../../services/ingest/ingestion/handlers/hibob.py#L224)). Because the
version is part of the key, a **status change lands as a NEW observation** rather
than silently deduping over the old one — and a backfilled object and its live
webhook twin **at the same version** collapse into one observation. This is the
central design invariant of HiBob ingestion.

> The `external_id` is **namespaced by `company_id`**, not by `tenant_id`,
> because the global `UNIQUE(source_channel, external_id)` has no `tenant_id`
> column — so the same employee id across two HiBob accounts stays distinct
> ([handlers/hibob.py:26‑30](../../../services/ingest/ingestion/handlers/hibob.py#L26-L30)).

---

## 2. Authentication & token model — one long‑lived service‑user credential

HiBob ingestion uses a **single credential model: a HiBob service user**. There
is **no OAuth, no token refresh**. This is the **Brex auth posture** (a
long‑lived secret resolved once and reused for the life of the client), but the
scheme is **HTTP Basic, not Bearer**
([client.py:1‑27](../../../services/ingest/integrations/hibob/client.py#L1-L27)).

### 2.1 The Basic credential — split into a public half and a secret half

```
Authorization: Basic base64("{service_user_id}:{token}")
```

- **`service_user_id`** — the **public** half. Rides on the install row
  (`hibob_installations.service_user_id`).
- **`token`** — the **secret** half. Resolved **lazily, once** from the secret
  store via the install's `secret_ref`, then cached in‑process for the client's
  life; guarded by an `asyncio.Lock` so concurrent first calls don't double‑fetch
  ([client.py:113‑141](../../../services/ingest/integrations/hibob/client.py#L113-L141)).
  In spammer mode the token is **preset** to `spam-hibob`, skipping the secret
  store entirely
  ([_clients.py:742](../../../services/ingest/ingestion/fetchers/_clients.py#L742)).

The service‑user token and the assembled Basic header are **never logged**
([client.py:25‑26](../../../services/ingest/integrations/hibob/client.py#L25-L26)).

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| `service_user_id` (public id half) | `hibob_installations.service_user_id` | rides on the install row |
| service‑user `token` (secret half) | secret store, referenced by `hibob_installations.secret_ref` | resolved once, no refresh; preset `spam-hibob` in spammer mode |
| `company_id` (scope id) | `hibob_installations.company_id` | per‑install; also the `external_id` namespace and the webhook tenant key |
| webhook HMAC secret | secret store, referenced by `provider_installations.secret_ref` (and `hibob_installations.webhook_secret_ref`) | App/per‑install HMAC secret for inbound webhook verification |

### 2.3 The install flow — no OAuth handshake

There is **no `oauth.py`** for HiBob — the package is just
`client.py`, `onboarding.py`, `metrics.py`, `__init__.py`. Provisioning is a
direct DB seed, `finalize_install`
([onboarding.py:42‑121](../../../services/ingest/integrations/hibob/onboarding.py#L42-L121)),
which **in one tenant‑scoped transaction**:

1. **UPSERTs a `hibob_installations` row** keyed on `(tenant_id, company_id)`,
   carrying `service_user_id`, `base_url`, `secret_ref`, `webhook_secret_ref`,
   and clearing `disabled_at` on re‑install
   ([onboarding.py:64‑83](../../../services/ingest/integrations/hibob/onboarding.py#L64-L83)).
2. **INSERTs one `hibob_entities` row per entity type** to shard
   (`employee`, `lifecycle`, `timeoff`, `payroll` by default), each `state='active'`,
   idempotent on `(hibob_installation_id, entity_type)`
   ([onboarding.py:85‑96](../../../services/ingest/integrations/hibob/onboarding.py#L85-L96)).
3. **Emits an `onboarding_triggers` row** (`source='hibob'`, `trigger_kind='install'`)
   so the existing backfill chain (`tenant_onboarding → source_onboarding →
   shard_fetch → reconciler`) fires. Like Gusto/Jira, HiBob is **not** a
   `provider_installations` *backfill* source — the install id rides in
   `installation_row_id` purely for the idempotency dedup index; `source='hibob'`
   is admitted by migration `0105_hibob`
   ([onboarding.py:98‑114](../../../services/ingest/integrations/hibob/onboarding.py#L98-L114)).

`register_webhook_installation` separately UPSERTs the **live‑path** row in
`provider_installations` (`provider='hibob'`, `installation_id=company_id`,
`secret_ref`=webhook HMAC secret, `enabled=TRUE`) so the webhook edge can resolve
the tenant and load the signing secret. **Backfill uses `hibob_installations`;
live uses `provider_installations` — seeded together but independent**
([onboarding.py:124‑154](../../../services/ingest/integrations/hibob/onboarding.py#L124-L154)).

> **TODO(human) (reproduced from code).** `register_webhook_installation` keys
> the install on the `company_id` and `_extract_hibob` resolves the tenant from a
> `companyId` **body field** — but in production HiBob resolves the destination by
> the webhook **endpoint/secret**, not a body field. The `companyId` body field
> is a synthetic‑gate stand‑in; the real per‑endpoint secret model is unverified
> ([onboarding.py:136‑141](../../../services/ingest/integrations/hibob/onboarding.py#L136-L141),
> [tenant_resolver.py:488‑492](../../../services/app/webhooks/tenant_resolver.py#L488-L492)).

---

## 3. The HiBob API surface actually called

All read calls funnel through `HibobClient._request`
([client.py:143‑203](../../../services/ingest/integrations/hibob/client.py#L143-L203)),
which:

- sets `Authorization: Basic …` and `Accept: application/json`,
- honours **`429` + `Retry-After`** within a bounded budget
  (`HIBOB_RL_MAX_ATTEMPTS`=4, `HIBOB_RL_MAX_SLEEP_SEC`=30),
- maps transport errors and any non‑2xx to a typed `HibobApiError`
  (`401/403` → `hibob_api_unauthorized`, `404` → `hibob_api_not_found`, `429`
  past budget → `hibob_api_rate_limited`)
  ([client.py:293‑324](../../../services/ingest/integrations/hibob/client.py#L293-L324)).

The endpoints invoked for ingestion:

| HiBob endpoint | Wrapper | Purpose | Code |
|----------------|---------|---------|------|
| `GET /v1/people` | `list_entities("employee")` | employee directory page | [client.py:213‑247](../../../services/ingest/integrations/hibob/client.py#L213-L247), [266‑267](../../../services/ingest/integrations/hibob/client.py#L266-L267) |
| `GET /v1/people/lifecycle` | `list_entities("lifecycle")` | lifecycle‑change page | ″ , [268](../../../services/ingest/integrations/hibob/client.py#L268) |
| `GET /v1/timeoff/requests` | `list_entities("timeoff")` | time‑off request page | ″ , [269](../../../services/ingest/integrations/hibob/client.py#L269) |
| `GET /v1/payroll/history` | `list_entities("payroll")` | payroll‑history page | ″ , [270](../../../services/ingest/integrations/hibob/client.py#L270) |
| `GET /v1/company/named-lists` | `company_info()` | connectivity / scope probe | [client.py:249‑256](../../../services/ingest/integrations/hibob/client.py#L249-L256) |

A **single generic `list_entities(entity_type, …)`** keeps the fetcher
entity‑agnostic (one shard per type); the entity → `(path, envelope_key)` map is
`_ENTITY_ENDPOINTS`
([client.py:266‑275](../../../services/ingest/integrations/hibob/client.py#L266-L275)).
Unmapped types fall back to `(/v1/{type}, "values")`. The response envelope key
varies per entity (`employees` / `values` / `requests`); a bare list or a generic
`"values"` key is also tolerated
([client.py:239‑243](../../../services/ingest/integrations/hibob/client.py#L239-L243)).

> **TODO(human) (reproduced from code).** The HiBob API **host**, the per‑entity
> **collection paths**, the response **envelope keys**, and the **incremental
> filter param name** are UNVERIFIED. `employee` (`/v1/people` → `{"employees":[…]}`)
> is modelled on the documented People API; **`lifecycle`/`timeoff`/`payroll` are
> speculative placeholders** ([client.py:11‑18](../../../services/ingest/integrations/hibob/client.py#L11-L18),
> [222‑228](../../../services/ingest/integrations/hibob/client.py#L222-L228),
> [263‑265](../../../services/ingest/integrations/hibob/client.py#L263-L265)).
> The `company_info` probe path `/v1/company/named-lists` is also a placeholder
> ([client.py:252‑255](../../../services/ingest/integrations/hibob/client.py#L252-L255)).

### 3.1 Pagination — offset / limit

Every list endpoint pages with `?limit=&offset=` (the Brex/Mercury archetype).
`list_entities` returns `(rows, next_offset)`; `next_offset is None` is terminal,
signalled by a **short page** (`len(rows) < limit`) or an empty page
([client.py:213‑247](../../../services/ingest/integrations/hibob/client.py#L213-L247)).
The fetcher persists `offset` in the shard cursor and resumes next invocation.
Page size defaults to **100**, env‑overridable via `HIBOB_BACKFILL_PAGE_SIZE`
(capped at 1000) ([fetchers/hibob.py:57‑61](../../../services/ingest/ingestion/fetchers/hibob.py#L57-L61)).

> **TODO(human) (reproduced from code).** Whether HiBob actually pages by
> offset/limit (vs a cursor token) and the per‑entity page cap are UNVERIFIED;
> this clones the Brex offset/limit archetype
> ([fetchers/hibob.py:31‑35](../../../services/ingest/ingestion/fetchers/hibob.py#L31-L35),
> [client.py:46‑48](../../../services/ingest/integrations/hibob/client.py#L46-L48)).

### 3.2 Rate limits — no dedicated client‑side bucket

There is **no HiBob entry in the rate‑limit bucket registry** — `buckets.py`
declares no `("hibob", …)` bucket (verified absent). HiBob rate limiting relies
**only** on the client's `429` + `Retry-After`‑aware retry (§3), not a
client‑side token bucket.

> **TODO(human) (reproduced from code).** HiBob's real per‑account concurrency
> limit and 429 signalling are UNVERIFIED; the client defaults to the Brex 429 +
> `Retry-After` scheme tuned via `HIBOB_RL_MAX_ATTEMPTS` / `HIBOB_RL_MAX_SLEEP_SEC`
> ([client.py:20‑23](../../../services/ingest/integrations/hibob/client.py#L20-L23)).

---

## 4. Backfill scope — the shard families

The planner decomposes one install into **one `hibob_entity` shard per active
entity type** ([planners/hibob.py:55‑86](../../../services/ingest/ingestion/planners/hibob.py#L55-L86)).
There is **one shard kind** (`hibob_entity`) with one shard per type:

| Entity type | What it captures | Reasoning signal (see §6) |
|-------------|------------------|---------------------------|
| `employee` | the People directory (highest‑signal entity) | `signal` (profile update) |
| `lifecycle` | hire / termination / role change | `state_change` (org change) on a state word; else `signal` |
| `timeoff` | time‑off requests | `state_change` (coverage/capacity) on approve/decline/cancel; else `signal` |
| `payroll` | payroll runs | `signal` |

The planner is **entity‑type‑agnostic**: it reads the active entity list from the
`hibob_entities` child table (JSON‑aggregated by the SourceOnboarding loader into
`ctx.install["entities"]`), so it stays stateless and `ctx.source_client` is
`None` — entities come from DB state, not a live call
([planners/hibob.py:1‑21](../../../services/ingest/ingestion/planners/hibob.py#L1-L21),
[55‑80](../../../services/ingest/ingestion/planners/hibob.py#L55-L80)). The
default seeded set is `DEFAULT_ENTITIES = ("employee","lifecycle","timeoff","payroll")`
([client.py:279](../../../services/ingest/integrations/hibob/client.py#L279)).

Each shard carries `entity_type`, `company_id`, `installation_id`, and an
`updated_cursor` (the per‑entity `modified` high‑water — `None` on first sync),
at a baseline `recency_score=1.0`
([planners/hibob.py:68‑80](../../../services/ingest/ingestion/planners/hibob.py#L68-L80)).

> **TODO(human) (reproduced from code).** The HiBob resource taxonomy to shard is
> not fully verified — the employee directory is the confirmed high‑signal entity;
> lifecycle/timeoff/payroll are added speculatively as their read surface is
> confirmed ([planners/hibob.py:13‑18](../../../services/ingest/ingestion/planners/hibob.py#L13-L18)).

---

## 5. Fetch specifics — one shard kind, two sync modes

`fetch_page_hibob` ([fetchers/hibob.py:124‑189](../../../services/ingest/ingestion/fetchers/hibob.py#L124-L189))
takes one `(install, shard_identifier, cursor)` triple and returns one page +
the next cursor. A `hibob_entity` shard runs in one of two modes:

- **FULL (initial backfill)** — walk `list_entities(<type>)` from `offset=0`,
  offset‑paginated to a short page.
- **INCREMENTAL (poll)** — when warm‑started with an `updated_cursor` (the
  high‑water `modified` timestamp), the fetcher freezes it as `incremental_floor`
  and passes `modified_since=<floor>` so only changed entities return; the overlap
  re‑fetch dedups via the versioned `external_id`
  ([fetchers/hibob.py:8‑35](../../../services/ingest/ingestion/fetchers/hibob.py#L8-L35),
  [135‑152](../../../services/ingest/ingestion/fetchers/hibob.py#L135-L152)).

### 5.1 Cursor

```python
class HibobCursor:
    offset: int = 0                       # list pagination offset, advances per page
    high_water_updated: str | None = None # max row `modified` seen — warm-start + reconciler ref
    incremental_floor: str | None = None  # `modified_since` lower bound frozen for this run (None in FULL)
    rows_seen: int = 0                    # diagnostic
    seeded: bool = False                  # first-call setup ran
```

([fetchers/hibob.py:64‑85](../../../services/ingest/ingestion/fetchers/hibob.py#L64-L85)).
The high‑water is bumped to the max `modified` across HiBob's varying entity
shapes — the fetcher tries `modified`, `modifiedAt`, `lastModified`, `updatedAt`,
`updated` ([fetchers/hibob.py:104‑117](../../../services/ingest/ingestion/fetchers/hibob.py#L104-L117)).

### 5.2 Records — one per row, tagged for the handler

Each entity row is emitted as one record tagged with the **private**
`_fyralis_record_type` (= the entity type) and `_fyralis_company_id`, with the
raw row under `entity`
([fetchers/hibob.py:164‑171](../../../services/ingest/ingestion/fetchers/hibob.py#L164-L171)):

```python
records.append({
    "_fyralis_record_type": entity_type,    # employee | lifecycle | timeoff | payroll
    "_fyralis_company_id": company_id,
    "entity": row,
})
```

The `hibob:object` handler builds **one observation per record** from these tags.

### 5.3 Rate‑limit soft‑pause

If a page hits `hibob_api_rate_limited` (429 past the retry budget), the fetcher
returns an **empty page with `end_of_data=False`** (preserving the cursor) so the
shard is retried rather than failed
([fetchers/hibob.py:153‑162](../../../services/ingest/ingestion/fetchers/hibob.py#L153-L162)).
Any other `HibobApiError` propagates.

---

## 6. The handler — shaping entities into `ObservationDraft`

`handle_hibob_object` ([handlers/hibob.py:279‑317](../../../services/ingest/ingestion/handlers/hibob.py#L279-L317))
is a pure function (no DB / network) that branches on the input shape and emits
**exactly one** observation per call. It detects which path it's on:

- **Backfill/poll** — a fetcher‑tagged record (`_fyralis_record_type` present) →
  `_entity_draft` over the `entity` body.
- **Live webhook** — a body with `companyId` + a `type`/`eventType`. If a full
  `entity` body is present → the **same** `_entity_draft` (so it dedups with its
  backfill twin); otherwise → `_thin_change_draft`, a thin notification the next
  poll re‑fills.

The webhook event `type`/record_type is normalised onto the canonical entity
kinds — `employee.updated`, `timeOff.approved`, `person`, `people`,
`timeoffrequest` all map back to the `{employee,lifecycle,timeoff,payroll}` set
([handlers/hibob.py:250‑265](../../../services/ingest/ingestion/handlers/hibob.py#L250-L265)).

### 6.1 Handler → `ObservationDraft` (branch on entity type)

| Source shape | Builder | `external_id` | `occurred_at` | `kind` | Trust tier |
|--------------|---------|---------------|---------------|--------|------------|
| `employee` row | `_entity_draft` | `hibob:{co}:employee:{id}:{ver}` | row `modified` (or now) | `signal` | **authoritative** |
| `lifecycle` row | `_entity_draft` | `hibob:{co}:lifecycle:{id}:{ver}` | row `modified` (or now) | `state_change` if status ∈ {hired, terminated, offboarded, left, rehired, …}; else `signal` | **authoritative** |
| `timeoff` row | `_entity_draft` | `hibob:{co}:timeoff:{id}:{ver}` | row `modified` (or now) | `state_change` if status ∈ {approved, declined, rejected, cancelled}; else `signal` | **authoritative** |
| `payroll` row | `_entity_draft` | `hibob:{co}:payroll:{id}:{ver}` | row `modified` (or now) | `signal` | **authoritative** |
| webhook, no body | `_thin_change_draft` | `hibob:{co}:{kind}:{id}:chg:{ver}` | `modified` (or now) | `signal` | **authoritative** |

Highlights:

- **Trust tier is always `authoritative`** — HiBob is the HR system of record
  ([handlers/hibob.py:31](../../../services/ingest/ingestion/handlers/hibob.py#L31),
  [47‑48](../../../services/ingest/ingestion/handlers/hibob.py#L47-L48)); the
  `CHANNEL_TRUST_MAP` entry confirms `"hibob:object" → "authoritative"`
  ([handlers/__init__.py:66](../../../services/ingest/ingestion/handlers/__init__.py#L66)).
- **`kind` classification** is driven by `_classify` — only `lifecycle` and
  `timeoff` promote to `state_change`, and only on a recognised status word; all
  else is `signal` ([handlers/hibob.py:137‑142](../../../services/ingest/ingestion/handlers/hibob.py#L137-L142),
  [56‑62](../../../services/ingest/ingestion/handlers/hibob.py#L56-L62)).
- **`source_actor_ref`** is `hibob:employee:{id}` for an employee row, else `None`
  ([handlers/hibob.py:196](../../../services/ingest/ingestion/handlers/hibob.py#L196)).
- **`entities_hint`** always carries `{"type":"hibob_object","id":"{kind}:{id}"}`,
  plus a `{"type":"person", …}` hint when a display name resolves
  ([handlers/hibob.py:174‑179](../../../services/ingest/ingestion/handlers/hibob.py#L174-L179)).
- **`{ver}` fallback** — if the row has no `modified` field, `ver="0"` so an
  unversioned row dedups against itself
  ([handlers/hibob.py:157‑161](../../../services/ingest/ingestion/handlers/hibob.py#L157-L161)).
- A record/event whose kind doesn't normalise into the entity set is rejected
  with a `ValidationError`
  ([handlers/hibob.py:290‑294](../../../services/ingest/ingestion/handlers/hibob.py#L290-L294),
  [305‑309](../../../services/ingest/ingestion/handlers/hibob.py#L305-L309)).

The `external_id` is built **inline** (not via the shared idempotency module) so
the handler adds no new shared‑file surface; the format is the contract verbatim
([handlers/hibob.py:88‑96](../../../services/ingest/ingestion/handlers/hibob.py#L88-L96)).

---

## 7. Live (real‑time) ingestion via webhooks

When an HR change occurs, HiBob **POSTs an HMAC‑signed webhook** to Fyralis's
webhook edge. Backfill, poll, and live all land on the **same** `hibob:object`
handler — the router maps provider `hibob` → channel `hibob:object`
([router.py:468](../../../services/app/webhooks/router.py#L468)).

### 7.1 Signature verification (HMAC‑SHA512, base64, `Bob-Signature`)

The inbound body is verified against HiBob's `Bob-Signature` header. The
**CONFIRMED** scheme ([signatures/hibob.py:1‑45](../../../services/app/webhooks/signatures/hibob.py#L1-L45)):

```
algorithm : HMAC-SHA512                      (hashlib.sha512)
digest    : base64-encoded   (NOT hex)
header    : Bob-Signature
prefix    : NONE             (bare base64 digest, no "sha512=")
expected  : base64( HMAC-SHA512(secret, raw_body) )
compare   : constant-time
```

The verifier loops over **all active secrets** so a rotation (two valid secrets
in flight) still verifies, and returns `signed_timestamp=None`
([signatures/hibob.py:54‑96](../../../services/app/webhooks/signatures/hibob.py#L54-L96)).
It is registered as `VERIFIERS["hibob"] = hibob.verifier`
([signatures/__init__.py:44](../../../services/app/webhooks/signatures/__init__.py#L44),
[66](../../../services/app/webhooks/signatures/__init__.py#L66)).

> **No replay window.** Like GitHub/Brex, the HiBob digest is over the **body
> alone** — there is no timestamp envelope (contrast Slack's `v0:{ts}:{body}` +
> 300 s window). Idempotency is enforced at the **ingestion layer** via the
> versioned `external_id`, not here
> ([signatures/hibob.py:20‑23](../../../services/app/webhooks/signatures/hibob.py#L20-L23)).

### 7.2 Tenant resolution

`_extract_hibob` resolves the tenant from the **`companyId` body field** → the
`provider_installations` row for `(provider='hibob', installation_id=company_id)`
([tenant_resolver.py:481‑493](../../../services/app/webhooks/tenant_resolver.py#L481-L493),
registered at [539](../../../services/app/webhooks/tenant_resolver.py#L539)). The
signing secret is loaded separately by `services/app/webhooks/secrets.py` from the
same row's `secret_ref`.

> **TODO(human) (reproduced from code).** In production HiBob does **not** carry
> the company id in the webhook body — the tenant is resolved by the per‑install
> endpoint/secret. The `companyId` body field is the **synthetic‑gate stand‑in**;
> the real per‑endpoint resolution is unverified against HiBob's webhook docs
> ([tenant_resolver.py:488‑492](../../../services/app/webhooks/tenant_resolver.py#L488-L492)).

### 7.3 Kafka cutover (inline `hibob:object` when off)

HiBob is in both the routed‑provider map and the cutover‑enabled map
([router.py:148](../../../services/app/webhooks/router.py#L148),
[189](../../../services/app/webhooks/router.py#L189)). When the tenant's
`ingestion.kafka_path_enabled=TRUE`, a verified webhook is published to Kafka and
the edge returns **202** (no synchronous‑response‑shape constraint like Discord).
When the flag is off — or on a Kafka‑publish fallback — the router **inline‑ingests**
via the `_PROVIDER_CHANNEL["hibob"] = "hibob:object"` channel
([router.py:468](../../../services/app/webhooks/router.py#L468),
[1081‑1095](../../../services/app/webhooks/router.py#L1081-L1095)).

---

## 8. Reconciliation — gap detection

`reconcile_hibob` ([reconcilers/hibob.py:125‑163](../../../services/ingest/ingestion/reconcilers/hibob.py#L125-L163))
re‑checks **completed** (`state='done'`) entity shards for new activity. It loads
the single active `hibob_installations` row for the tenant, opens one client, and
runs a **cheap one‑row probe per entity type**
([reconcilers/hibob.py:80‑122](../../../services/ingest/ingestion/reconcilers/hibob.py#L80-L122)):

1. Read the shard's stored `high_water_updated` from `shard_fetch` state
   ([reconcilers/hibob.py:69‑77](../../../services/ingest/ingestion/reconcilers/hibob.py#L69-L77)).
2. `list_entities(entity_type, limit=1, modified_since=high_water)` — if any row
   comes back, there's a gap.

On a gap it reshares a `hibob_entity` shard at **`recency_score=1.5`**, warm‑started
at the high‑water as `updated_cursor` (incremental mode), carrying
`parent_shard_id` and `gap_baseline_updated`
([reconcilers/hibob.py:111‑122](../../../services/ingest/ingestion/reconcilers/hibob.py#L111-L122)).
`external_id` parity (versioned by `modified`) means re‑walked entities dedup
against what backfill already wrote — so the probe **can over‑reshare but never
under‑reshares**, and dedup makes re‑walks idempotent
([reconcilers/hibob.py:1‑14](../../../services/ingest/ingestion/reconcilers/hibob.py#L1-L14)).
A probe failure is best‑effort logged and skipped, never fatal
([reconcilers/hibob.py:101‑106](../../../services/ingest/ingestion/reconcilers/hibob.py#L101-L106)).

---

## 9. Revocation chokepoint / recoverable‑error behavior

**There is no revocation chokepoint for HiBob (verified absent).** Unlike GitHub
(which disables the install on `401 Bad credentials` / app `404`) and unlike the
hardened Notion path, the HiBob client does **not** call any
`_disable_installation`/park routine — `client.py` and `fetchers/hibob.py`
contain no `disable`/`revoke`/`park`/`enabled` logic (grep‑verified empty).

The current behaviour on credential failure is:

- The client maps `401/403` to a `HibobApiError(code="hibob_api_unauthorized")`
  ([client.py:298‑303](../../../services/ingest/integrations/hibob/client.py#L298-L303)).
- This is **not** the rate‑limited code, so the fetcher's only soft‑recovery
  branch (the 429 empty‑page path, §5.3) does **not** catch it — the error
  **propagates and fails the shard**
  ([fetchers/hibob.py:153‑162](../../../services/ingest/ingestion/fetchers/hibob.py#L153-L162)).

So a revoked / rotated service‑user token surfaces as a **failed shard with no
auto‑disable and no auto‑recovery** today. Recovery is operational: re‑seed the
`hibob_installations.secret_ref` and re‑run. *(inferred from the absence of any
disable/park path — contrast the Notion recoverable‑401 + revocation chokepoint.)*

---

## 10. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  service-user token (resolved once from secret store, NO refresh)│
                          │     └─► Authorization: Basic base64(service_user_id:token)        │
   ACTIVE ENTITY TYPES    │  planner: read hibob_entities (DB) — ctx.source_client is None    │
   (employee/lifecycle/   │     └─► one hibob_entity shard per entity_type                    │
    timeoff/payroll)      │  fetcher: GET /v1/{people|people/lifecycle|timeoff/...|payroll}   │
                          │     └─► offset/limit page → records tagged _fyralis_record_type   │
                          └──────────────────────── POLL (incremental) ─────────────────────┐│
   changed entities ──────►  same fetcher, modified_since = high_water cursor               ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                          ┌──────────────────────── LIVE (push) ──────────────────────────┐│
   ANY HR change ─────────►  HiBob webhook ──HTTP POST──► /webhooks/hibob                   ││
                          │     verify Bob-Signature (HMAC-SHA512, base64, no ts)          ││
                          │     tenant ← companyId body field (synthetic stand-in)         ││
                          │     kafka_path_enabled? → 202+Kafka : inline hibob:object       ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_hibob_object             │
                                                            │  branch: tagged record | webhook │
                                                            │  external_id =                   │
                                                            │   hibob:{co}:{kind}:{id}:{ver}   │
                                                            │  trust = authoritative           │
                                                            │  → ObservationDraft               │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, poll, and webhook all land on
   `hibob:object` with `external_id = hibob:{company}:{entity}:{id}:{ver}`. A
   backfilled object and its live twin at the same version dedup to one
   observation; namespacing is by `company_id` (the global UNIQUE has no
   `tenant_id`).
2. **Versioned `external_id`.** `{ver}` = the row's `modified` field, so a status
   change re‑observes as a NEW observation instead of silently deduping.
3. **One credential model.** A single HiBob **service user** — Basic
   `base64(service_user_id:token)`, long‑lived, resolved once, **no refresh, no
   OAuth**. The id is public (install row); the token is secret (secret store).
4. **One shard kind, two sync modes.** `hibob_entity` shards page offset/limit in
   FULL mode, then `modified_since` the high‑water in INCREMENTAL (poll) mode.
5. **No webhook replay window** (HiBob signs the body only); idempotency is the
   versioned `external_id`. **No revocation chokepoint** — an auth failure fails
   the shard without auto‑disable (§9).
6. **Trust = authoritative** for every HiBob observation (HR system of record).

---

## 11. Configuration & compliance

> HiBob's read surface, pagination mode, incremental filter, and rate limits are
> **UNVERIFIED placeholders** (Brex/Gusto archetype) — the items below marked
> 🔶 are the open verification gaps reproduced from the code's own
> `TODO(human)`/UNVERIFIED markers. The **webhook signing scheme** and the
> **`external_id` shape** are CONFIRMED.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `HIBOB_API_BASE_URL` | `https://api.hibob.com` | API host override (per‑install `base_url` still wins) ([endpoints.py:101](../../../lib/integrations/endpoints.py#L101), [139](../../../lib/integrations/endpoints.py#L139)) |
| `HIBOB_BACKFILL_PAGE_SIZE` | `100` (cap 1000) | offset/limit page size ([fetchers/hibob.py:57‑61](../../../services/ingest/ingestion/fetchers/hibob.py#L57-L61)) |
| `HIBOB_RL_MAX_ATTEMPTS` | `4` | 429 retry budget ([client.py:163](../../../services/ingest/integrations/hibob/client.py#L163)) |
| `HIBOB_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:164](../../../services/ingest/integrations/hibob/client.py#L164)) |

### 11.2 Status

- **Webhook signing** — HMAC‑SHA512, base64 digest, `Bob-Signature` header, no
  prefix, constant‑time compare, multi‑secret rotation. ✅ (CONFIRMED)
- **`external_id`** — `hibob:{company}:{entity}:{id}:{ver}`, versioned + company‑namespaced. ✅ (CONFIRMED)
- **Trust tier** — `authoritative` (HR system of record). ✅
- **Auth posture** — long‑lived service‑user Basic credential, no refresh; token never logged. ✅
- **Read surface** — collection paths, envelope keys, `company_info` probe path. 🔶 UNVERIFIED (employee modelled on People API; lifecycle/timeoff/payroll speculative).
- **Incremental filter** — `modifiedSince` param name. 🔶 UNVERIFIED.
- **Pagination** — offset/limit vs cursor token. 🔶 UNVERIFIED.
- **Rate limits** — no dedicated bucket; per‑account concurrency limit + 429 signalling. 🔶 UNVERIFIED.
- **Webhook tenant resolution** — `companyId` body field is a synthetic stand‑in; real per‑endpoint secret model. 🔶 UNVERIFIED.

### 11.3 Dev / spammer mode

For local testing against the mock source servers, `build_hibob_client` detects
spammer mode and **presets** the service‑user token to `spam-hibob` (skipping the
secret store) and points `api_base_url` at the local spammer's `/hibob` sub‑path
via the endpoint resolver
([_clients.py:727‑744](../../../services/ingest/ingestion/fetchers/_clients.py#L727-L744),
[endpoints.py:169](../../../lib/integrations/endpoints.py#L169)). The webhook
tenant resolver reads the `companyId` from the synthetic harness's body
(§7.2). A mock HiBob client + fixture generator live under
[synthetic/mock_clients/hibob.py](../../../services/ingest/synthetic/mock_clients/hibob.py)
and [synthetic/fixtures/hibob_generator.py](../../../services/ingest/synthetic/fixtures/hibob_generator.py).
