# Jira Ingestion — How Fyralis Pulls Jira Data

This document explains, in detail, **how Jira data enters Fyralis**: which Jira
Cloud REST v3 APIs are called, with which credential, and how the Jira signal
set — **issue snapshots, changelog (field/status) transitions, and comments** —
is each ingested.

It deliberately stops at the point where a Jira change becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> A short orientation reference lives at
> [docs/ingestion/sources/jira.md](../sources/jira.md). This page is the deep
> version; it does not duplicate the summary table there.

---

## 1. The three ways data arrives

Unlike Slack/GitHub (two paths), Jira reaches Fyralis through **three ingress
paths that converge on one handler and one dedup namespace**. Jira Cloud
exposes both a **historical query surface** (JQL search) and a **live push
surface** (admin dynamic webhooks):

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / install finalize | Fyralis *pulls* each project's full issue history via `POST /rest/api/3/search/jql` (`expand=changelog`) | [planners/jira.py](../../../services/ingest/ingestion/planners/jira.py), [fetchers/jira.py](../../../services/ingest/ingestion/fetchers/jira.py) |
| **Poll (incremental)** | Reconciler reshare / periodic reconciler | The **same fetcher**, warm‑started from the per‑project `updated` high‑water cursor (`AND updated >= <cursor>`) | [fetchers/jira.py:259‑267](../../../services/ingest/ingestion/fetchers/jira.py#L259-L267), [reconcilers/jira.py](../../../services/ingest/ingestion/reconcilers/jira.py) |
| **Webhook (live)** | New activity in Jira | Jira *pushes* a system/admin webhook (`jira:issue_created/_updated`, `comment_created/_updated`) to Fyralis | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/jira.py](../../../services/app/webhooks/signatures/jira.py), [handlers/jira.py](../../../services/ingest/ingestion/handlers/jira.py) |

The `channel_mapping` block makes the one‑channel invariant explicit — all
three ingress kinds collapse to a single channel
([channel_mapping.py:94‑108](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L94-L108)):

```python
("jira", "backfill"): "jira:issue",
("jira", "poll"):     "jira:issue",
("jira", "webhook"):  "jira:issue",
```

The single handler is `handle_jira_issue` (`@register("jira:issue")`,
[handlers/jira.py:320‑389](../../../services/ingest/ingestion/handlers/jira.py#L320-L389)),
and `CHANNEL_TRUST_MAP["jira:issue"] = "authoritative"`
([handlers/__init__.py:52](../../../services/ingest/ingestion/handlers/__init__.py#L52)).
This mirrors `github:webhook`'s one‑channel/many‑event‑types shape: the handler
branches on a **reshaped record/event type** (issue / changelog‑transition /
comment), not on the channel.

### 1.1 The single dedup namespace

A Jira issue is **mutable** (its `updated` field bumps on every edit) and so are
its comments; a changelog history entry is **immutable**. So the `external_id`
is **versioned** for the mutable entities and stable for transitions
([idempotency/__init__.py:124‑140](../../../services/ingest/ingestion/idempotency/__init__.py#L124-L140),
[handlers/jira.py:20‑26](../../../services/ingest/ingestion/handlers/jira.py#L20-L26)):

```
issue       external_id = "jira:{site}:issue:{issue_id}:{updated}"      # versioned
comment     external_id = "jira:{site}:comment:{comment_id}:{updated}"  # versioned
transition  external_id = "jira:{site}:transition:{issue_id}:{history_id}"  # immutable
```

`{site}` is the bare site host (`acme.atlassian.net`). Backfill/poll records
carry it on the synthetic `_fyralis_site` key (injected by the fetcher); the
live webhook derives the same host from the issue's `self` URL — so a backfilled
issue and its live twin produce the **same** `external_id` and collapse into one
observation ([fetchers/jira.py:238‑244](../../../services/ingest/ingestion/fetchers/jira.py#L238-L244),
[handlers/jira.py:303‑313](../../../services/ingest/ingestion/handlers/jira.py#L303-L313)).
Because the observations repo dedups on `(source_channel, external_id)` **ignoring
`occurred_at`** (the IN‑15 mutable‑source lesson), the `{updated}` suffix is what
lets a re‑edit land as a *new* observation rather than silently dedup against the
original.

> Jira uses the **REST v3 API** for history (`POST /rest/api/3/search/jql`) and
> **admin dynamic webhooks** for real time. There is no GraphQL and no polling
> connector beyond the reconciler‑driven incremental re‑walk (§9).

---

## 2. Authentication & token model — API‑token Basic auth (no OAuth)

Jira ingestion uses **one credential type: a long‑lived API token** issued from
`id.atlassian.com`, sent as HTTP **Basic auth** = `base64("{account_email}:{api_token}")`
against the per‑tenant site base URL `https://<site>.atlassian.net`
([client.py:1‑22](../../../services/ingest/integrations/jira/client.py#L1-L22),
[client.py:144‑147](../../../services/ingest/integrations/jira/client.py#L144-L147)).

This is **not** an Atlassian 3LO/OAuth bounce and **not** a DWD service account.
Like the Notion client, the token is long‑lived, so there is **no per‑request
mint**: it is resolved once from the secret store (or preset in spammer mode) and
reused for the life of the client
([client.py:120‑142](../../../services/ingest/integrations/jira/client.py#L120-L142)).
The API token and the Basic‑auth header are **never logged**; the site host is
blake2b‑hashed before it touches a log line
([client.py:59‑61](../../../services/ingest/integrations/jira/client.py#L59-L61)).

### 2.1 The admin "connect wizard" install flow

Because there is no OAuth callback, the production install surface is an
admin‑driven, Bearer‑authed wizard
([integrations/jira/oauth.py](../../../services/ingest/integrations/jira/oauth.py)
— the file is named `oauth.py` but implements a connect wizard, not an OAuth
bounce):

1. **`POST /integrations/jira/connect/preflight`** — body
   `{base_url, account_email, api_token}`. Calls `JiraClient.myself()` to verify
   the credentials and `list_projects()` to enumerate projects for the selector
   UI. On auth failure it returns a structured `400` and **stores no secret**
   ([oauth.py:148‑179](../../../services/ingest/integrations/jira/oauth.py#L148-L179)).
2. **`POST /integrations/jira/connect/finalize`** — re‑verifies the creds
   *before any write* (so a bad token can't leave half‑state), resolves the
   project set (enumerate if `project_keys` omitted), then stores the API token
   (and optional webhook secret) encrypted‑at‑rest — only opaque refs reach the
   DB ([oauth.py:182‑261](../../../services/ingest/integrations/jira/oauth.py#L182-L261)).

`finalize_install` then UPSERTs, in **one tenant‑scoped transaction**: a
`jira_installations` row (keyed on `(tenant_id, base_url)`), one `jira_projects`
row per project, and an `onboarding_triggers` row (`source='jira'`) that fires
the M6 backfill chain
([onboarding.py:41‑122](../../../services/ingest/integrations/jira/onboarding.py#L41-L122)).
Only when a webhook secret was supplied does `register_webhook_installation`
seed the **separate** `provider_installations` row the webhook edge resolves the
tenant + HMAC secret from
([onboarding.py:125‑148](../../../services/ingest/integrations/jira/onboarding.py#L125-L148)).

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| API token | secret store, label `jira_api_token:{base_url}` → ref on `jira_installations.secret_ref` | long‑lived; resolved lazily on first request ([oauth.py:218‑220](../../../services/ingest/integrations/jira/oauth.py#L218-L220), [client.py:120‑142](../../../services/ingest/integrations/jira/client.py#L120-L142)) |
| Account email | `jira_installations.account_email` (plaintext) | the Basic‑auth username |
| Site base URL | `jira_installations.base_url` | per‑install `https://<site>.atlassian.net`; there is **no** global Jira host (§11) |
| Webhook secret | secret store, label `jira_webhook_secret:{base_url}` → `provider_installations.secret_ref` | **optional**; only set if the operator pasted a webhook secret. Backfill works without it; live webhooks do not |

> **Two install tables, deliberately independent.** Backfill reads
> `jira_installations` + `jira_projects`; the live webhook edge reads the generic
> `provider_installations` row (provider=`jira`, `installation_id` = site host).
> They are seeded together at finalize but stay independent — exactly as the
> onboarding module documents
> ([onboarding.py:11‑18](../../../services/ingest/integrations/jira/onboarding.py#L11-L18)).

---

## 3. The Jira REST API surface that is actually called

All read calls funnel through `JiraClient._request`
([client.py:149‑203](../../../services/ingest/integrations/jira/client.py#L149-L203)),
which:

- sets `Authorization: Basic …` + `Accept/Content-Type: application/json`,
- honours Jira's `429 Retry-After` within a bounded budget
  (`JIRA_RL_MAX_ATTEMPTS`=4, `JIRA_RL_MAX_SLEEP_SEC`=30 s) before surfacing
  `JiraApiError(jira_api_rate_limited)`,
- maps every non‑2xx to a typed `JiraApiError`
  ([client.py:311‑342](../../../services/ingest/integrations/jira/client.py#L311-L342)).

| Jira endpoint | Wrapper | Purpose | Code |
|---------------|---------|---------|------|
| `POST /rest/api/3/search/jql` | `search_issues()` | one page of issues matching a JQL, `expand=changelog` | [client.py:209‑246](../../../services/ingest/integrations/jira/client.py#L209-L246) |
| `GET /rest/api/3/project/search` | `list_projects()` | enumerate projects visible to the token (install/seed time) | [client.py:248‑268](../../../services/ingest/integrations/jira/client.py#L248-L268) |
| `POST /rest/api/3/search/approximate-count` | `has_updates_since()` | reconciler gap probe ("anything updated since the high‑water?") | [client.py:270‑292](../../../services/ingest/integrations/jira/client.py#L270-L292) |
| `GET /rest/api/3/myself` | `myself()` | credential / connectivity probe (connect wizard) | [client.py:294‑297](../../../services/ingest/integrations/jira/client.py#L294-L297) |

> **API migration (verified in code, 2025).** Atlassian **removed** the classic
> `GET /rest/api/3/search` (now returns **HTTP 410 Gone**) in favour of
> `POST /rest/api/3/search/jql`, which is **token‑paginated** (no `startAt`/`total`)
> and no longer returns a count — hence the separate `approximate-count` endpoint
> for the reconciler probe
> ([client.py:218‑229](../../../services/ingest/integrations/jira/client.py#L218-L229),
> [client.py:270‑285](../../../services/ingest/integrations/jira/client.py#L270-L285)).

### 3.1 Pagination — two different schemes

- **`/search/jql`** is **token‑paginated**: pass `nextPageToken` from the prior
  page; the response carries the next one (absent on the last page) plus
  `isLast`. The fetcher persists the opaque token in its cursor; `next_page_token
  is None` (== `isLast`) is terminal
  ([client.py:230‑246](../../../services/ingest/integrations/jira/client.py#L230-L246)).
- **`/project/search`** is classic **offset‑paginated**:
  `{startAt, maxResults, total, values:[…]}`. `list_projects` returns
  `(projects, next_start_at, total)`; the connect wizard loops to completion
  ([client.py:248‑268](../../../services/ingest/integrations/jira/client.py#L248-L268),
  [oauth.py:127‑145](../../../services/ingest/integrations/jira/oauth.py#L127-L145)).

Page sizes: issue search caps at **100** (`JIRA_BACKFILL_PAGE_SIZE`, clamped),
project search at **50** ([client.py:42‑45](../../../services/ingest/integrations/jira/client.py#L42-L45),
[fetchers/jira.py:69‑73](../../../services/ingest/ingestion/fetchers/jira.py#L69-L73)).

### 3.2 Rate limits — **no dedicated client‑side bucket**

Unlike GitHub (`("github","rest_authenticated")`) and Slack (per‑method tiers),
**there is no Jira entry in `BUCKET_DEFAULTS`**
([rate_limit/buckets.py:79‑92](../../../services/ingest/ingestion/rate_limit/buckets.py#L79-L92)
lists only slack/github/gmail/discord). Jira rate limiting relies entirely on
the **server‑driven `429 Retry-After`** retry inside `JiraClient._request`
(§3) — Jira Cloud's published limit is cost‑budget‑based and not a fixed
per‑minute floor a static bucket could model.

---

## 4. Backfill scope — the shard families

The planner decomposes one site install into **one shard per active project**,
all of `shard_kind = "jira_project_issues"`
([planners/jira.py:51‑83](../../../services/ingest/ingestion/planners/jira.py#L51-L83)).

There is exactly **one shard family** — there is no per‑signal split (issues,
transitions, and comments are all fanned out of the *same* issue page; see §6).

`ctx.source_client` is **None**: projects are read from DB state (the
`jira_projects` rows, JSON‑aggregated into `ctx.install["projects"]` by the
SourceOnboarding loader), so the planner stays stateless — exactly like Calendar
([planners/jira.py:1‑16](../../../services/ingest/ingestion/planners/jira.py#L1-L16),
[planners/jira.py:34‑48](../../../services/ingest/ingestion/planners/jira.py#L34-L48)).
Each shard carries `project_key`, `project_id`, `installation_id`, and the
warm‑start `updated_cursor` (None on a first full sync), at `recency_score=1.0`
([planners/jira.py:65‑77](../../../services/ingest/ingestion/planners/jira.py#L65-L77)).

---

## 5. Fetch specifics — one shard kind, two sync modes

`fetch_page_jira` ([fetchers/jira.py:247‑315](../../../services/ingest/ingestion/fetchers/jira.py#L247-L315))
streams one project's issues. ShardFetch calls it in a loop, persisting the
returned cursor between calls. **Two modes share the cursor:**

- **FULL (initial backfill).** JQL `project = "KEY" ORDER BY updated ASC`,
  `expand=changelog`, token‑paginated. `ORDER BY updated ASC` makes the
  high‑water `updated` monotonic, so a crash mid‑walk resumes cleanly.
- **INCREMENTAL ("poll").** When the shard is warm‑started with an
  `updated_cursor`, the JQL adds `AND updated >= "<floor>"`, so only changed
  issues come back ([fetchers/jira.py:173‑179](../../../services/ingest/ingestion/fetchers/jira.py#L173-L179),
  [fetchers/jira.py:259‑267](../../../services/ingest/ingestion/fetchers/jira.py#L259-L267)).
  Jira JQL is **minute precision**; the `>=` overlap re‑fetches the boundary
  minute, and the versioned `external_id` dedups it.

> There is **no separate poll/incremental driver service**. "Poll" ingress is
> the same fetcher under a warm cursor; it is *driven* by the reconciler emitting
> a reshare with `updated_cursor` set (§9), not by a standalone poller.
> *(inferred from code: the only writers of an incremental `updated_cursor` are
> the planner warm‑start and the reconciler reshare; `channel_mapping`'s "poll"
> entry is the `ingress_kind` such a re‑walk normalizes under.)*

### 5.1 The cursor

```python
class JiraCursor:
    next_page_token: str | None = None    # /search/jql continuation token
    high_water_updated: str | None = None # max issue `updated` seen — warm-start + reconciler reference
    incremental_floor: str | None = None  # the `updated >=` floor frozen for this run (None in FULL)
    issues_seen: int = 0                  # diagnostic
    seeded: bool = False                  # whether first-call setup ran
```

([fetchers/jira.py:76‑101](../../../services/ingest/ingestion/fetchers/jira.py#L76-L101)).
On the first call, a warm `updated_cursor` from the shard identifier becomes both
the `incremental_floor` and the initial `high_water_updated`
([fetchers/jira.py:259‑264](../../../services/ingest/ingestion/fetchers/jira.py#L259-L264)).
`end_of_data` is whatever `search_issues` reports as `isLast`.

### 5.2 The timezone trap (load‑bearing)

`_to_jql_datetime` converts an ISO `updated` to Jira's `yyyy/MM/dd HH:mm` literal
**in the value's own offset** — it must *not* convert to UTC. Jira interprets a
bare JQL datetime literal in the *querying user's* timezone and returns
`issue.updated` in that same timezone; converting to UTC shifts the floor by the
offset (a `+0545` account's UTC floor lands ~6 h in the past), so
`updated >= floor` re‑matches every issue and the reconciler re‑shards forever
([fetchers/jira.py:120‑146](../../../services/ingest/ingestion/fetchers/jira.py#L120-L146)).

### 5.3 Rate‑limit handling mid‑fetch

If `search_issues` raises `jira_api_rate_limited` (retry budget spent), the
fetcher leaves the cursor unadvanced and returns an **empty, non‑terminal** page
(`end_of_data=False`) so ShardFetch re‑enters next tick — it does **not** fail the
run ([fetchers/jira.py:276‑289](../../../services/ingest/ingestion/fetchers/jira.py#L276-L289)).

---

## 6. The per‑issue fan‑out — one issue → N records

The `jira:issue` handler produces **one observation per record**. To preserve
historical fidelity (per‑transition *timing* is the velocity/flow signal), the
fetcher fans each issue out into separate tagged records via `_explode_issue`
([fetchers/jira.py:189‑235](../../../services/ingest/ingestion/fetchers/jira.py#L189-L235)),
each carrying a private `_fyralis_record_type` the handler branches on:

| `_fyralis_record_type` | Source within the issue page | Becomes |
|------------------------|------------------------------|---------|
| `"issue"` | the issue's current field snapshot | `kind=signal`, `object_type=issue` |
| `"transition"` | one `changelog.histories[]` entry | `kind=state_change` if a status/resolution change, else `signal` |
| `"comment"` | one `fields.comment.comments[]` entry | `kind=signal`, `object_type=comment` |

> **Documented v1 scope.** `expand=changelog` + `fields.comment` from
> `/search/jql` return the **most‑recent** histories/comments inline, which is
> sufficient for the live + recent‑history reasoning signal. Deep history beyond
> the inline window would need the per‑issue `/changelog` + `/comment` endpoints
> (**not called** in v1); the reconciler's incremental re‑walk catches anything
> the inline window missed
> ([fetchers/jira.py:42‑46](../../../services/ingest/ingestion/fetchers/jira.py#L42-L46)).

---

## 7. The handler — shaping records into `ObservationDraft`

`handle_jira_issue` ([handlers/jira.py:320‑389](../../../services/ingest/ingestion/handlers/jira.py#L320-L389))
is a **pure function** (no DB/network). It detects the path first, then branches
on record/event type. Every draft is `trust_tier=authoritative` (Jira is the
system of record for work tracking, [handlers/jira.py:44‑45](../../../services/ingest/ingestion/handlers/jira.py#L44-L45)).

| Branch | `external_id` | `occurred_at` | `kind` | `source_actor_ref` |
|--------|---------------|---------------|--------|--------------------|
| **issue** (`_issue_draft`) | `jira:{site}:issue:{id}:{updated}` | `fields.updated`/`created` | `signal` | reporter (`email:…` or `jira:account:…`) |
| **transition** (`_transition_draft`) | `jira:{site}:transition:{issue_id}:{history_id}` | `history.created` | `state_change` if any changed field ∈ {`status`,`resolution`}, else `signal` | history `author` |
| **comment** (`_comment_draft`) | `jira:{site}:comment:{id}:{updated}` | `comment.updated`/`created` | `signal` | comment `author`/`updateAuthor` |

Highlights:

- **Actor reference** prefers a cross‑source‑resolvable `email:{lower}` and falls
  back to `jira:account:{accountId}`; entity hints carry typed
  `email_address`/`jira_account` refs with roles (`actor`/`assignee`/`reporter`)
  ([handlers/jira.py:96‑115](../../../services/ingest/ingestion/handlers/jira.py#L96-L115)).
- **ADF flattening.** Description and comment bodies are Atlassian Document
  Format docs in v3; `_adf_to_text` recursively flattens them to plain text
  (older/mock shapes may already be strings)
  ([handlers/jira.py:74‑93](../../../services/ingest/ingestion/handlers/jira.py#L74-L93)).
- **`entities_hint`** always includes `{"type":"jira_issue","id":key}` and, for
  issues, `{"type":"jira_project","id":project_key}`
  ([handlers/jira.py:150‑159](../../../services/ingest/ingestion/handlers/jira.py#L150-L159)).
- A transition's content text synthesizes legible phrases like
  `field: from → to`, leading with the status change when present
  ([handlers/jira.py:217‑225](../../../services/ingest/ingestion/handlers/jira.py#L217-L225)).
- The issue/comment `external_id` is versioned by `updated` (falling back to the
  literal `none` when absent), so each edit re‑observes
  ([idempotency/__init__.py:125‑140](../../../services/ingest/ingestion/idempotency/__init__.py#L125-L140)).

---

## 8. Live (real‑time) ingestion via dynamic webhooks

When activity occurs in Jira, the configured admin **dynamic webhook** POSTs the
raw Jira event body to Fyralis's webhook edge
(`/webhooks/jira/events` — the router registers both `/{provider}` and
`/{provider}/{subpath:path}` on one handler,
[router.py:749‑750](../../../services/app/webhooks/router.py#L749-L750)). Backfill,
poll, and live all land on the **same** `jira:issue` handler.

### 8.1 The webhook path inside the handler

A live body carries `webhookEvent` (e.g. `jira:issue_updated`, `comment_created`),
which the handler maps onto the **same three record builders** as backfill
([handlers/jira.py:329‑365](../../../services/ingest/ingestion/handlers/jira.py#L329-L365)):

- `comment_*` → `_comment_draft`.
- `jira:issue*` → if the inline `changelog.items` carry a `status`/`resolution`
  change, emit a `_transition_draft` (the high‑value `state_change`); otherwise
  emit the issue snapshot via `_issue_draft`. (Comments arrive as separate
  `comment_*` events.)
- An unsupported `webhookEvent` is rejected with a `ValidationError`.

So a webhook‑delivered change and its backfill twin produce the **same**
versioned `external_id` and dedup to one observation.

### 8.2 Signature verification (HMAC‑SHA256, no timestamp)

The webhook router dispatches to `VERIFIERS["jira"] = jira.verifier`
([signatures/__init__.py:44‑51](../../../services/app/webhooks/signatures/__init__.py#L44-L51),
[router.py:756](../../../services/app/webhooks/router.py#L756)). Jira admin
webhooks registered with a **Secret** sign the raw body with HMAC‑SHA256 and
present it as `sha256=<hex>` in the **`X-Hub-Signature`** header — the same
scheme GitHub uses but under the un‑suffixed header name. `JiraVerifier` requires
the `sha256=` prefix and constant‑time compares against each active secret
([signatures/jira.py:38‑80](../../../services/app/webhooks/signatures/jira.py#L38-L80)).

> **No replay window.** Like GitHub, Jira's digest is over the **body alone** —
> there is no timestamp envelope (contrast Slack's `v0:{ts}:{body}` + 300 s
> window), so `signed_timestamp` is `None`. Idempotency is enforced at the
> ingestion layer by the versioned `external_id`, not here
> ([signatures/jira.py:12‑17](../../../services/app/webhooks/signatures/jira.py#L12-L17),
> [signatures/jira.py:75‑80](../../../services/app/webhooks/signatures/jira.py#L75-L80)).

### 8.3 Tenant resolution

A Jira webhook body carries **no `cloudId`**, so the tenant is resolved from the
**site host embedded in the affected entity's `self` URL** (issue, then comment,
then a top‑level `self`) — that host is the `installation_id` registered in
`provider_installations` (provider=`jira`)
([tenant_resolver.py:300‑314](../../../services/app/webhooks/tenant_resolver.py#L300-L314),
[tenant_resolver.py:528](../../../services/app/webhooks/tenant_resolver.py#L528),
[onboarding.py:125‑148](../../../services/ingest/integrations/jira/onboarding.py#L125-L148)).

### 8.4 The kafka_path_enabled cutover

Jira is a **cutover‑enabled** provider — both in `_PROVIDER_TO_SHADOW_SOURCE`
and `_CUTOVER_ENABLED_PROVIDERS`
([router.py:128](../../../services/app/webhooks/router.py#L128),
[router.py:160‑165](../../../services/app/webhooks/router.py#L160-L165)). After
signature verification + tenant resolution, the router branches on the tenant's
`ingestion.kafka_path_enabled` flag ([router.py:1026‑1095](../../../services/app/webhooks/router.py#L1026-L1095)):

- **Flag ON (cutover):** the body is published to the `ingestion.raw` Kafka topic
  (S3 PutIfAbsent → publish → durable flush) and the edge returns **`202
  accepted`**; inline `ingest()` is **skipped** (the writer pool produces the
  observation downstream).
- **Flag OFF (or cutover failure → graceful fallback):** the router falls back to
  **inline** `ingest(channel, payload, …)` with
  `channel = _PROVIDER_CHANNEL["jira"] = "jira:issue"`
  ([router.py:437‑445](../../../services/app/webhooks/router.py#L437-L445),
  [router.py:1095‑1120](../../../services/app/webhooks/router.py#L1095-L1120)),
  preserving the user‑visible 200/201 contract. The inline channel and the cutover
  source are the same destination by two routes.

---

## 9. Reconciliation — gap detection

Two reconcilers share the Jira gap logic via `RECONCILER_DISPATCH["jira"] =
reconcile_jira`:

1. **At‑completion** — `reconcile_jira` runs after a run's shards settle
   ([reconcilers/jira.py:137‑175](../../../services/ingest/ingestion/reconcilers/jira.py#L137-L175)).
2. **Periodic** — `periodic_reconciler` re‑checks already‑reconciled runs on a
   rotation, reusing the same `RECONCILER_DISPATCH` entry + reshare path
   ([periodic_reconciler.py:1‑42](../../../services/ingest/ingestion/workflows/periodic_reconciler.py#L1-L42)).

For each `done` project shard, `_check_one_shard_for_gap` loads the cursor's
`high_water_updated`, then probes the live project cheaply with
`has_updates_since` → `POST /rest/api/3/search/approximate-count` for
`project = KEY AND updated >= <floor>`
([reconcilers/jira.py:86‑134](../../../services/ingest/ingestion/reconcilers/jira.py#L86-L134)).
If the count is `> 0`, it reshares a `jira_project_issues` shard at
**`recency_score=1.5`**, warm‑started with `updated_cursor = high_water` (so the
re‑walk runs in incremental mode and only re‑fetches the changed tail).

### 9.1 The exclusive‑floor convergence fix (load‑bearing)

JQL `updated` is **minute precision**, so `>=` against the high‑water's *own*
minute would re‑match a boundary issue at `HH:MM:ss` (ss>0) forever and the
reconciler would re‑shard on every tick. `_to_jql_minute_after` rounds the floor
**up to the next minute**, which with `>=` excludes the high‑water's own minute —
the only combination that converges
([fetchers/jira.py:149‑170](../../../services/ingest/ingestion/fetchers/jira.py#L149-L170),
[client.py:270‑292](../../../services/ingest/integrations/jira/client.py#L270-L292),
[reconcilers/jira.py:99‑106](../../../services/ingest/ingestion/reconcilers/jira.py#L99-L106)).
Sub‑minute updates after the walk are caught by the live webhook + the periodic
reconciler, not this one‑shot probe. A transient probe error is logged and
treated as "no gap" (best‑effort), never failing the run
([reconcilers/jira.py:107‑116](../../../services/ingest/ingestion/reconcilers/jira.py#L107-L116)).

---

## 10. Revocation / recoverable‑error behaviour

> **No dedicated revocation chokepoint** (contrast GitHub's
> `_maybe_disable_on_revocation`, which disables the install on `401 Bad
> credentials`/`404`). The Jira client maps `401`/`403` to
> `JiraApiError(jira_api_unauthorized)` and `404` to `jira_api_not_found`
> ([client.py:311‑342](../../../services/ingest/integrations/jira/client.py#L311-L342)),
> but **nothing in the fetcher, reconciler, or client toggles
> `jira_installations.disabled_at` on those codes.** The only error the fetcher
> special‑cases is `jira_api_rate_limited`, which it treats as recoverable
> (empty non‑terminal page, retry next tick — §5.3); a `401`/`403`/`404` raised
> by `search_issues` propagates to ShardFetch as an ordinary fetch error.
>
> `disabled_at` is written only by **admin actions**: `finalize_install` clears
> it on re‑connect, and the reconciler simply *skips* installs where
> `disabled_at IS NOT NULL`
> ([onboarding.py:76](../../../services/ingest/integrations/jira/onboarding.py#L76),
> [reconcilers/jira.py:145‑156](../../../services/ingest/ingestion/reconcilers/jira.py#L145-L156)).
> Recovery from a rejected token is therefore re‑running the connect wizard with
> a fresh API token (which re‑clears `disabled_at`), not an automatic chokepoint.

> **TODO(human):** confirm whether a persistent Jira `401`/`403` *should* trip an
> auto‑disable (GitHub/Notion both have a revocation chokepoint; Jira currently
> does not, so a revoked token will surface as repeated fetch errors rather than
> parking the install). The *why* of the asymmetry is not stated in the code.

---

## 11. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  API token (long-lived) ─► Basic base64(email:token)            │
   ALL ACTIVE PROJECTS    │  install: jira_projects → ctx.install["projects"] (loader)      │
                          │     └─► one jira_project_issues shard per project (rec=1.0)      │
   FULL mode              │  fetcher: POST /rest/api/3/search/jql                            │
                          │     JQL `project=KEY ORDER BY updated ASC`, expand=changelog     │
                          │     └─► fan-out each issue → {issue, transition*, comment*}      │
                          └───────────────────────────────────────────────────────────────┬─┘
                          ┌──────────────────────── POLL (incremental) ───────────────────┐│
   RECONCILER reshare ────►  same fetcher, warm `updated_cursor`                          ││
                          │     JQL adds `AND updated >= <minute-after high-water>`        ││
                          └───────────────────────────────────────────────────────────────┘│
                          ┌──────────────────────── WEBHOOK (push) ───────────────────────┐│
   ANY issue/comment ─────►  Jira dynamic webhook ──POST──► /webhooks/jira/events          ││
   activity               │     verify X-Hub-Signature (HMAC-SHA256, no ts)               ││
                          │     tenant = site host from issue.self URL                     ││
                          │     kafka_path_enabled? ON → publish + 202 ; OFF → inline      ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_jira_issue  (jira:issue) │
                                                            │  branch: issue/transition/comment│
                                                            │  external_id =                   │
                                                            │   jira:{site}:issue:{id}:{updated}│
                                                            │   …:transition:{id}:{history_id} │
                                                            │   …:comment:{id}:{updated}        │
                                                            │  → ObservationDraft (authoritative)│
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One channel, one dedup namespace.** Backfill, poll, and live webhook all
   map to `jira:issue` and the single `handle_jira_issue` handler; the handler
   branches on the reshaped record/event type (issue / changelog‑transition /
   comment). A backfilled change and its live twin dedup via the versioned
   `external_id` (`jira:{site}:issue:{id}:{updated}` and friends).
2. **One credential model.** A long‑lived **API token** via HTTP Basic auth;
   no OAuth bounce, no DWD, no per‑request mint. Resolved once from the secret
   store (or preset in spammer mode).
3. **Versioned dedup for mutable entities.** Issues + comments key on `{updated}`
   (a re‑edit re‑observes); transitions key on the immutable `history_id`.
4. **`/search/jql` token pagination + JQL `updated ASC` high‑water cursor.** The
   same fetcher does FULL and INCREMENTAL; convergence depends on the
   minute‑after exclusive floor and on keeping the JQL literal in the value's own
   timezone.
5. **HMAC‑SHA256 `X-Hub-Signature`, no replay window** — idempotency is the
   `external_id` dedup, not a timestamp envelope. The webhook honours the
   tenant's `kafka_path_enabled` cutover (202) with inline `jira:issue` fallback.

---

## 12. Configuration & compliance

Verified against the code paths above and Jira Cloud's documented REST v3 + admin
webhook contracts.

### 12.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `JIRA_RL_MAX_ATTEMPTS` | `4` | 429 `Retry-After` retry budget in `JiraClient._request` |
| `JIRA_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` |
| `JIRA_BACKFILL_PAGE_SIZE` | `100` | issue search page size (clamped to ≤100) |
| `JIRA_API_BASE_URL` | — | **spammer/dev only** — overrides the per‑install base URL; prod uses `jira_installations.base_url` ([endpoints.py:48‑54](../../../lib/integrations/endpoints.py#L48-L54), [endpoints.py:127](../../../lib/integrations/endpoints.py#L127)) |
| `ingestion.kafka_path_enabled` (tenant flag, not env) | tenant‑configured | ON → webhook cutover (202); OFF → inline `jira:issue` ingest |

> There is **no** `JIRA_*` rate‑limit *bucket* env (no static bucket exists; §3.2).

### 12.2 Verified compliant

- **Auth** — HTTP Basic `base64(email:api_token)`; token never logged, host
  hashed in logs. ✅
- **API migration** — uses `POST /rest/api/3/search/jql` (classic `/search` is
  410 Gone) + `/search/approximate-count` for counts. ✅
- **Pagination** — `/search/jql` token pagination (`nextPageToken`/`isLast`);
  `/project/search` offset pagination (`startAt`/`total`). ✅
- **Webhook signing** — HMAC‑SHA256 `X-Hub-Signature` (`sha256=` prefix),
  constant‑time compare, multi‑secret rotation, no replay window. ✅
- **Tenant isolation** — webhook tenant resolved from the `self`‑URL site host →
  `provider_installations` (provider=`jira`); credentials live as opaque secret
  refs only. ✅
- **Convergence** — minute‑after exclusive JQL floor + value‑own‑timezone literal
  prevent reconciler re‑share loops. ✅

### 12.3 Dev / spammer mode

For local testing against the mock source servers, `build_jira_client` detects
spammer mode and **presets the API token** as `spam-jira`, skipping the secret
store entirely; the base URL is overridden via the endpoint resolver to the one
mock host's `/jira` sub‑path (all sites route there)
([_clients.py:278‑307](../../../services/ingest/ingestion/fetchers/_clients.py#L278-L307)).
The mock server serves the token‑paginated `POST /rest/api/3/search/jql`
([synthetic/mock_servers/jira.py:78‑116](../../../services/ingest/synthetic/mock_servers/jira.py#L78-L116)).
The `jira_api` endpoint entry is intentionally **empty** in production — it exists
only so the spammer's `/jira` sub‑path convention resolves uniformly; prod always
uses the per‑install `base_url`
([endpoints.py:48‑54](../../../lib/integrations/endpoints.py#L48-L54)).
