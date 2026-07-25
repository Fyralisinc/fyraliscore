# Grafana Ingestion — How Fyralis Pulls Grafana Data

This document explains, in detail, **how Grafana data enters Fyralis**: which
Grafana APIs are called, with which credential, and how the two distinct Grafana
signal surfaces — **dashboard/state annotations** (historical) and **alert
notifications** (live) — are each ingested.

It deliberately stops at the point where a Grafana signal becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

---

## 1. The ways data arrives

Unlike Slack or GitHub — where backfill and live converge on **one** handler and
**one** dedup namespace — Grafana is a **two‑channel source**. The historical
pull and the live push are **independent streams** that land on **two different
handlers** and **two different `external_id` namespaces**:

| Path | Trigger | Mechanism | Channel | Code |
|------|---------|-----------|---------|------|
| **Backfill (historical)** | Onboarding | Fyralis *pulls* `GET /api/annotations` (epoch‑ms windowed) | `grafana:annotation` | [planners/grafana.py](../../../services/ingest/ingestion/planners/grafana.py), [fetchers/grafana.py](../../../services/ingest/ingestion/fetchers/grafana.py) |
| **Poll (incremental)** | Reconciler reshare | same fetcher, warm‑started at the high‑water | `grafana:annotation` | [reconcilers/grafana.py:128‑165](../../../services/ingest/ingestion/reconcilers/grafana.py#L128-L165) |
| **Live (real‑time)** | An alert fires/resolves | Grafana *pushes* an **Alerting webhook** (Alertmanager‑superset group) | `grafana:alert` | [webhooks/router.py:749](../../../services/app/webhooks/router.py#L749), [webhooks/signatures/grafana.py](../../../services/app/webhooks/signatures/grafana.py), [handlers/grafana.py:229](../../../services/ingest/ingestion/handlers/grafana.py#L229) |

The `(source, ingress_kind)` → channel routing is the normalizer's
`channel_mapping` table
([channel_mapping.py:145‑147](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L145-L147)):

```
("grafana", "backfill") -> "grafana:annotation"
("grafana", "poll")     -> "grafana:annotation"
("grafana", "webhook")  -> "grafana:alert"
```

### 1.1 Why the historical pull surface carries alert history too

The pull surface is `GET /api/annotations`. Grafana **auto‑creates an annotation
for every alert state transition** — tagged with `alertId` / `newState` /
`prevState` — alongside manual notes, deploy markers, and region annotations. So
the single annotations stream already carries the **historical** alert timeline
(no Loki backend required), interleaved with ordinary annotations
([fetchers/grafana.py:26‑34](../../../services/ingest/ingestion/fetchers/grafana.py#L26-L34)).
The annotation handler splits the two by inspecting `alertId`/`newState`
([handlers/grafana.py:128](../../../services/ingest/ingestion/handlers/grafana.py#L128)):
an alert‑state‑change annotation → `kind=state_change`; a plain annotation →
`kind=signal`. Both still ride the **`grafana:annotation`** channel.

The live push surface is the **Grafana Alerting webhook contact point** — a
**different** delivery that arrives as an Alertmanager‑superset **alert group**
(`{status, alerts:[…], groupKey, commonLabels, externalURL, …}`) and lands on the
**`grafana:alert`** channel.

### 1.2 Why there is no cross‑channel `external_id` collision

The two channels key on **structurally different** ids, so a historical alert
annotation and a live alert delivery for the *same* underlying incident never
collapse into one observation — they are deliberately distinct streams
([idempotency/__init__.py:72‑85](../../../services/ingest/ingestion/idempotency/__init__.py#L72-L85)):

```
grafana:annotation   external_id = grafana:{instance}:annotation:{id}:{time}
                       # keyed on the annotation id, versioned by `time` (ms)
grafana:alert        external_id = grafana:{instance}:alert:{group_hash}:{status}:{rep_ts}
                       # keyed on groupKey-hash, versioned by (status, representative ts)
```

The annotation key is the **annotation id**; the alert key is a **groupKey hash +
status**. They share neither key space nor handler, so there is no dedup
interaction between channels. (Within a channel, both keys are *versioned* per the
mutable‑source lesson — a re‑fetched annotation at the same `time` dedups; a
re‑delivered alert group at the same `(status, rep_ts)` dedups; but a genuine
state change lands as a new observation.)

> **Backfill of the live alert‑group stream is a documented v2 (NOT
> implemented).** v1 backfills alerts only as the **annotations** Grafana
> auto‑writes for transitions. The full Loki‑backed alert *state‑history
> timeline* — which would let backfill reconstruct alert groups directly — is an
> explicit v2 enhancement and is not in this codebase
> ([fetchers/grafana.py:25‑34](../../../services/ingest/ingestion/fetchers/grafana.py#L25-L34),
> [client.py:228‑232](../../../services/ingest/integrations/grafana/client.py#L228-L232)).

---

## 2. Authentication & token model

Grafana ingestion uses **one credential: a service‑account token**, presented as
a **Bearer** token (`Authorization: Bearer glsa_…`). There are no per‑user OAuth
tokens — Grafana API keys were deprecated in 2025, and a service‑account token is
**org‑scoped**, so one token reads the whole org's annotations
([client.py:1‑23](../../../services/ingest/integrations/grafana/client.py#L1-L23)).

There is **no `oauth.py` and no `code` exchange** for Grafana — onboarding stores
a pre‑provisioned service‑account token directly (contrast GitHub's App‑JWT mint
or Slack's OAuth v2 dance). The integration directory is
`services/ingest/integrations/grafana/` and contains only `client.py` +
`onboarding.py` + `__init__.py` (no `oauth.py`, no `webhook.py`); the webhook
verifier lives in the app webhook subsystem (§8).

### 2.1 Token resolution

`GrafanaClient` resolves the token **once** and reuses it for the life of the
client — the same long‑lived posture as the Jira/Mercury/Notion clients
([client.py:106‑132](../../../services/ingest/integrations/grafana/client.py#L106-L132)):

- A **preset** `api_token` (Provider Lab mode, §12.3) short‑circuits the lookup.
- Otherwise it lazily reads the secret store at `secret_ref` (under the install's
  `tenant_id`), guarded by an `asyncio.Lock`. Missing
  `secret_store`/`secret_ref`/`tenant_id` → `GrafanaApiError(grafana_api_unauthorized)`.

The token and the `Authorization` header are **never logged**; the base‑URL host
is hashed (`short_host_hash`) before it touches a log line
([client.py:47‑49](../../../services/ingest/integrations/grafana/client.py#L47-L49)).

### 2.2 Where credentials live — two install rows, seeded together

Grafana keeps the backfill identity and the live‑webhook identity in **two
independent tables**, registered in the same onboarding step
([onboarding.py:1‑19](../../../services/ingest/integrations/grafana/onboarding.py#L1-L19)):

| Row | Table | Key | Holds | Used by |
|-----|-------|-----|-------|---------|
| Backfill install | `grafana_installations` | `(tenant_id, base_url)` | `org_id`, `secret_ref` (SA token), `webhook_secret_ref`, `annotations_cursor_ms` | planner / fetcher / reconciler |
| Webhook install | `provider_installations` | `(provider='grafana', installation_id=<instance host>)` | `secret_ref` (HMAC webhook secret) | webhook edge tenant resolution + secret load |

- `finalize_install` UPSERTs the `grafana_installations` row **and** emits an
  `onboarding_triggers` row (`source='grafana'`, `trigger_kind='install'`) in one
  tenant‑scoped transaction, which fires the M6 backfill chain (`oauth_poller →
  tenant_onboarding → source_onboarding → shard_fetch → reconciler`). Idempotent
  on `(tenant_id, base_url)`
  ([onboarding.py:43‑94](../../../services/ingest/integrations/grafana/onboarding.py#L43-L94)).
- `register_webhook_installation` UPSERTs the `provider_installations` row keyed
  on the **instance host** (e.g. `acme.grafana.net`), which **must** match what
  `tenant_resolver._extract_grafana` derives from the webhook payload's
  `externalURL` host (§8.3)
  ([onboarding.py:35‑40](../../../services/ingest/integrations/grafana/onboarding.py#L35-L40), [97‑120](../../../services/ingest/integrations/grafana/onboarding.py#L97-L120)).

> **Instance‑host parity is the invariant that joins the two streams' namespaces.**
> The fetcher derives the `external_id` instance from `base_url`
> ([fetchers/grafana.py:127‑132](../../../services/ingest/ingestion/fetchers/grafana.py#L127-L132));
> the live handler derives it from the payload `externalURL` host
> ([handlers/grafana.py:94‑103](../../../services/ingest/ingestion/handlers/grafana.py#L94-L103)).
> If a host on the backfill side ever drifts from the live `externalURL` host, the
> two channels namespace differently — which is *expected* here (they are distinct
> streams) but matters if a future v2 ever unifies alert backfill with live alerts.

---

## 3. The Grafana API surface that is actually called

All read calls funnel through `GrafanaClient._request`
([client.py:134‑186](../../../services/ingest/integrations/grafana/client.py#L134-L186)),
which:

- sets `Authorization: Bearer {sa_token}` and `Accept: application/json`,
- honours `429 Retry-After` within a bounded retry budget
  (`GRAFANA_RL_MAX_ATTEMPTS`=4, `GRAFANA_RL_MAX_SLEEP_SEC`=30) before surfacing
  `GrafanaApiError(grafana_api_rate_limited)`,
- maps any non‑2xx to a typed `GrafanaApiError`
  ([client.py:246‑278](../../../services/ingest/integrations/grafana/client.py#L246-L278)),
- accepts a JSON **array** as a valid body (the annotations endpoint returns a
  *bare array*, not an object).

The endpoints invoked for ingestion:

| Grafana endpoint | Wrapper | Purpose | Code |
|------------------|---------|---------|------|
| `GET /api/annotations?from=&to=&limit=` | `list_annotations()` | one page of annotations (epoch‑ms window, newest‑first) | [client.py:192‑219](../../../services/ingest/integrations/grafana/client.py#L192-L219) |
| `GET /api/annotations?from=&limit=1` | `has_annotations_since()` | reconciler 1‑row gap probe | [client.py:221‑226](../../../services/ingest/integrations/grafana/client.py#L221-L226) |
| `GET /api/org` | `get_org()` | connectivity + credential probe (seed/onboarding) | [client.py:228‑232](../../../services/ingest/integrations/grafana/client.py#L228-L232) |

The annotations response carries, per element: `id, alertId, dashboardUID,
panelId, userId, userName, newState, prevState, time, timeEnd, text, tags, data`.
`from`/`to` are **epoch milliseconds** (Grafana's unit). `type` ∈
{`None`,`alert`,`annotation`} is *available* on the client wrapper but is passed
as `None` by the fetcher — so backfill pulls **both** user annotations and
auto‑created alert‑state‑change annotations in one stream
([client.py:200‑207](../../../services/ingest/integrations/grafana/client.py#L200-L207)).

### 3.1 Pagination — backward time‑window walk (no cursor token)

Grafana's annotations endpoint has **no opaque cursor / no `Link` header**.
Pagination is a **backward walk over the `time` window**: each page is fetched
newest‑first; the fetcher then sets the *next* page's upper bound to
`min(time seen) − 1ms` and repeats. A page shorter than `limit` is the last page
([fetchers/grafana.py:199‑217](../../../services/ingest/ingestion/fetchers/grafana.py#L199-L217)).
`limit` defaults to 100 (`GRAFANA_ANNOTATIONS_PAGE_SIZE`, clamped 1–100,
[fetchers/grafana.py:66‑70](../../../services/ingest/ingestion/fetchers/grafana.py#L66-L70)).

### 3.2 Rate limits — no dedicated token bucket

Grafana has **no entry in `BUCKET_DEFAULTS`** — there is no client‑side token
bucket for it (contrast Slack's per‑method buckets and GitHub's
`rest_authenticated` bucket)
([rate_limit/buckets.py:79‑91](../../../services/ingest/ingestion/rate_limit/buckets.py#L79-L91)).
Grafana throttling is handled **entirely** by the client's `Retry-After`‑aware
429 retry budget (§3, [client.py:171‑174](../../../services/ingest/integrations/grafana/client.py#L171-L174)).
The fetcher additionally degrades gracefully: a budget‑exhausted
`grafana_api_rate_limited` makes the fetcher return an **empty page with the
cursor unadvanced and `end_of_data=False`**, so ShardFetch simply re‑enters next
tick rather than failing the run
([fetchers/grafana.py:174‑182](../../../services/ingest/ingestion/fetchers/grafana.py#L174-L182)).

---

## 4. Backfill scope — the planner shard family

Annotations and alert state are **org‑wide** in Grafana — there is no
per‑resource sub‑table (contrast Jira's per‑project or Slack's per‑channel
shards). So the planner emits **exactly one** shard per install:
`grafana_org_annotations`
([planners/grafana.py:26‑65](../../../services/ingest/ingestion/planners/grafana.py#L26-L65)).

`ctx.source_client` is `None` — the planner reads only the install row loaded by
SourceOnboarding's `_LOAD_GRAFANA_INSTALL_SQL`
([source_onboarding.py:392‑398](../../../services/ingest/ingestion/workflows/source_onboarding.py#L392-L398)),
so it stays stateless (same as Calendar/Drive). The shard carries
`installation_id`, `base_url`, `org_id`, and a warm‑start
`updated_cursor` (the high‑water annotation `time` in epoch ms, `None` on first
sync), at `recency_score=1.0`
([planners/grafana.py:46‑59](../../../services/ingest/ingestion/planners/grafana.py#L46-L59)).

---

## 5. Fetching annotations — one shard kind, two sync modes

`fetch_page_grafana` ([fetchers/grafana.py:144‑219](../../../services/ingest/ingestion/fetchers/grafana.py#L144-L219))
fetches one page, advances the cursor backward, tags each record, and returns it.
ShardFetch calls it in a loop, persisting the opaque cursor between calls (the N1
invariant — one HTTP fetch per call). Two modes share the one fetcher
([fetchers/grafana.py:4‑23](../../../services/ingest/ingestion/fetchers/grafana.py#L4-L23)):

- **FULL (initial backfill).** No warm cursor → the floor is `now − window`
  (`GRAFANA_BACKFILL_WINDOW_DAYS`, default 90; `0` = all time, no floor). The walk
  advances the upper bound backward until a short page hits the floor
  ([fetchers/grafana.py:152‑161](../../../services/ingest/ingestion/fetchers/grafana.py#L152-L161)).
- **INCREMENTAL (poll / reconciler reshare).** Warm‑started with
  `updated_cursor` (the prior run's high‑water `time`); the floor is set to that
  high‑water so only newer annotations come back. The boundary annotation
  re‑fetches and dedups via the versioned `external_id`
  ([fetchers/grafana.py:153‑157](../../../services/ingest/ingestion/fetchers/grafana.py#L153-L157)).

### 5.1 Cursor

```python
class GrafanaCursor(BaseModel):
    high_water_time_ms: int | None  # max annotation `time` seen — warm-start + reconciler ref
    page_to_ms:         int | None  # next page's UPPER bound (walk advances it backward)
    floor_ms:           int | None  # frozen lower `from` bound for this run
    annotations_seen:   int         # diagnostic
    seeded:             bool         # first-call setup done
```

([fetchers/grafana.py:86‑107](../../../services/ingest/ingestion/fetchers/grafana.py#L86-L107)).
`high_water_time_ms` is the single value the reconciler reads back (§9). The walk
ends with `end_of_data=True` when a page returns fewer than `limit` rows
([fetchers/grafana.py:201‑217](../../../services/ingest/ingestion/fetchers/grafana.py#L201-L217)).

### 5.2 Record tagging

Each annotation is shipped as‑is plus two private keys the handler reads
([fetchers/grafana.py:184‑197](../../../services/ingest/ingestion/fetchers/grafana.py#L184-L197)):

```python
rec["_fyralis_record_type"] = "annotation"
rec["_fyralis_instance"]    = instance   # host from base_url; matches live externalURL host
```

`_fyralis_instance` is the `external_id` namespace, derived from `base_url` to
match what the live webhook handler derives from `externalURL`
([fetchers/grafana.py:127‑132](../../../services/ingest/ingestion/fetchers/grafana.py#L127-L132)).

---

## 6. The annotation handler — `grafana:annotation` → `ObservationDraft`

`handle_grafana_annotation` ([handlers/grafana.py:189‑199](../../../services/ingest/ingestion/handlers/grafana.py#L189-L199),
`@register("grafana:annotation")`) derives the instance and shapes one draft per
annotation via `_annotation_draft`
([handlers/grafana.py:110‑186](../../../services/ingest/ingestion/handlers/grafana.py#L110-L186)).

The pivotal branch: an annotation with a non‑zero `alertId` **or** a `newState` is
an **alert‑state‑change** annotation → `kind=state_change`; otherwise it's a plain
annotation → `kind=signal`
([handlers/grafana.py:128](../../../services/ingest/ingestion/handlers/grafana.py#L128)):

| Annotation kind | `kind` | `external_id` | `occurred_at` | `source_actor_ref` | Trust tier |
|-----------------|--------|---------------|---------------|--------------------|------------|
| alert‑state‑change (`alertId`/`newState`) | `state_change` | `grafana:{instance}:annotation:{id}:{time}` | `time` (ms) → utc, or now | `None` (machine; `userId 0`) | **authoritative** |
| plain annotation (manual/deploy/region) | `signal` | `grafana:{instance}:annotation:{id}:{time}` | `time` (ms) → utc, or now | `grafana:user:{userId}` if `userId>0` | **authoritative** |

- An annotation **missing `id`** is rejected with `ValidationError`
  ([handlers/grafana.py:111‑114](../../../services/ingest/ingestion/handlers/grafana.py#L111-L114)).
- A state‑change annotation synthesizes the sentence
  `"[grafana alert] {prevState} → {newState}: {text}"`; a plain one uses the
  annotation text (or `"(grafana annotation)"`)
  ([handlers/grafana.py:131‑139](../../../services/ingest/ingestion/handlers/grafana.py#L131-L139)).
- **`entities_hint`** carries `grafana_dashboard` (from `dashboardUID`),
  `grafana_user` (actor, when user‑created), and up to 8 `grafana_tag` refs
  ([handlers/grafana.py:149‑157](../../../services/ingest/ingestion/handlers/grafana.py#L149-L157)).

---

## 7. The alert handler — `grafana:alert` → `ObservationDraft`

`handle_grafana_alert` ([handlers/grafana.py:228‑305](../../../services/ingest/ingestion/handlers/grafana.py#L228-L305),
`@register("grafana:alert")`) consumes the **Alertmanager‑superset alert group**
delivered by the live webhook. **v1 emits ONE `state_change` observation per
delivery** — the full per‑alert detail is preserved under `content["alerts"]`.

| Field | Source | Notes |
|-------|--------|-------|
| `external_id` | `grafana:{instance}:alert:{group_hash}:{status}:{rep_ts}` | `group_hash = blake2b(groupKey)`; falls back to a hash of `commonLabels` ([handlers/grafana.py:255‑256](../../../services/ingest/ingestion/handlers/grafana.py#L255-L256)) |
| `occurred_at` | representative ts: newest `startsAt` (firing) / `endsAt` (resolved) | guards Grafana's zero‑value `0001‑…` sentinel ([handlers/grafana.py:215‑225](../../../services/ingest/ingestion/handlers/grafana.py#L215-L225)) |
| `kind` | `state_change` | always |
| `source_actor_ref` | `None` | machine‑generated — exercises the actorless path |
| `trust_tier` | **authoritative** | Grafana is the system of record for its own alert state |

- A payload carrying **neither `alerts` nor `groupKey`** is rejected with
  `ValidationError` ([handlers/grafana.py:247‑251](../../../services/ingest/ingestion/handlers/grafana.py#L247-L251)).
- `content_text` synthesizes `"[{STATUS}×{n}] {alertnames} ({label summary})"`
  ([handlers/grafana.py:258‑265](../../../services/ingest/ingestion/handlers/grafana.py#L258-L265)).
- **`entities_hint`** carries distinct `grafana_alert` names + salient
  `grafana_label_{service|namespace|job|instance|cluster}` refs from
  `commonLabels` ([handlers/grafana.py:270‑276](../../../services/ingest/ingestion/handlers/grafana.py#L270-L276)).

> **Per‑alert fan‑out is a documented v2 (NOT implemented).** Exploding one group
> delivery into one observation per alert needs a normalizer‑level group‑explode
> step that does not exist; the handler contract returns a **single** draft
> ([handlers/grafana.py:13‑16](../../../services/ingest/ingestion/handlers/grafana.py#L13-L16)).

Both handlers register their trust tier in the shared `CHANNEL_TRUST_MAP`
([handlers/grafana.py:308‑309](../../../services/ingest/ingestion/handlers/grafana.py#L308-L309)).

---

## 8. Live (real‑time) ingestion via the Alerting webhook

When an alert fires or resolves, a Grafana **Alerting webhook contact point**
POSTs the alert group to Fyralis's webhook edge at `/webhooks/grafana[/…]`. This
is the **only** path onto the `grafana:alert` channel.

The router registers one handler for both `/webhooks/{provider}` and
`/webhooks/{provider}/{subpath:path}`
([webhooks/router.py:749‑755](../../../services/app/webhooks/router.py#L749-L755)),
and the request flows: verifier lookup → body‑size precheck → best‑effort JSON
parse → tenant resolution → secret load → **verify** → dispatch
([webhooks/router.py:756‑840](../../../services/app/webhooks/router.py#L756-L840)).

### 8.1 Signature verification (HMAC‑SHA256, bare hex)

`GrafanaVerifier.verify` ([signatures/grafana.py:51‑99](../../../services/app/webhooks/signatures/grafana.py#L51-L99))
checks the `X-Grafana-Alerting-Signature` header against
`HMAC-SHA256(secret, signed_bytes)` as a **bare lowercase hex digest** (Grafana
12.0+, May 2025). Unlike GitHub/Jira/Mercury, there is **no `sha256=` prefix**.
Each active secret is tried in turn (constant‑time compare); no match →
`WebhookVerificationError(signature_mismatch)` → `401`.

The verifier is registered as `VERIFIERS["grafana"] = grafana.verifier`
([signatures/__init__.py:54](../../../services/app/webhooks/signatures/__init__.py#L54)),
and the router maps provider `grafana` → channel **`grafana:alert`** via
`_PROVIDER_CHANNEL["grafana"]`
([webhooks/router.py:450‑452](../../../services/app/webhooks/router.py#L450-L452)).

### 8.2 Replay handling

By default Grafana signs the **raw body alone** — there is **no timestamp
envelope and no replay window** (contrast Slack's `v0:{ts}:{body}` + 300 s).
There is also **no per‑provider replay cache** for Grafana (the
`(installation, delivery)` replay cache in the router is GitHub‑specific,
[router.py:886‑897](../../../services/app/webhooks/router.py#L886-L897)).
Idempotency is enforced **purely** at the ingestion layer via the versioned
`external_id` (§1.2): a redelivered alert group at the same `(status, rep_ts)`
dedups
([signatures/grafana.py:17‑20](../../../services/app/webhooks/signatures/grafana.py#L17-L20)).

An **optional** timestamp‑in‑signature mode exists: if
`GRAFANA_WEBHOOK_TIMESTAMP_HEADER` names a header the contact point sends,
Grafana signs `"{unix_ts}:" + body` and the verifier signs the same prefix. Off
by default ([signatures/grafana.py:46‑75](../../../services/app/webhooks/signatures/grafana.py#L46-L75)).

> **Bearer‑header mode (Grafana < 12.0) is a documented follow‑up (NOT
> implemented).** Older self‑hosted instances that cannot HMAC‑sign would instead
> set a static `Authorization: Bearer <secret>` on the contact point; v1 verifies
> **HMAC only** ([signatures/grafana.py:22‑24](../../../services/app/webhooks/signatures/grafana.py#L22-L24)).

### 8.3 Tenant resolution

The tenant is resolved from the payload's top‑level **`externalURL` host** (the
instance root URL), looked up against the `provider_installations` row for
`(provider='grafana', installation_id=<instance host>)`
([tenant_resolver.py:347‑355](../../../services/app/webhooks/tenant_resolver.py#L347-L355),
[531](../../../services/app/webhooks/tenant_resolver.py#L531)). A single
service‑account token is org‑scoped, so **one instance host == one install** in
v1.

### 8.4 Cutover vs inline

Grafana webhooks fit the 202 cutover contract — when the tenant's
`ingestion.kafka_path_enabled` is `TRUE` the router publishes to Kafka and returns
202; otherwise it falls through to the inline `ingest()` on the
`grafana:alert` channel
([webhooks/router.py:171‑172](../../../services/app/webhooks/router.py#L171-L172),
[1095](../../../services/app/webhooks/router.py#L1095)).

---

## 9. Reconciliation — gap detection

`reconcile_grafana` ([reconcilers/grafana.py:128‑165](../../../services/ingest/ingestion/reconcilers/grafana.py#L128-L165))
re‑checks completed `grafana_org_annotations` shards for new annotations. For each
done shard it reads the stored `high_water_time_ms` from the shard's cursor and
probes the live org with a **1‑row** `GET /api/annotations?from=<high_water + 1ms>`
(`has_annotations_since`)
([reconcilers/grafana.py:87‑125](../../../services/ingest/ingestion/reconcilers/grafana.py#L87-L125)):

- The floor is `high_water + 1ms` — **exclusive** — so the high‑water annotation
  doesn't re‑match itself forever (annotation `time` is ms‑precise)
  ([reconcilers/grafana.py:98‑101](../../../services/ingest/ingestion/reconcilers/grafana.py#L98-L101)).
- On a hit it reshares a `grafana_org_annotations` shard at
  **`recency_score=1.5`**, warm‑started (`updated_cursor=high_water`) so the
  re‑walk only re‑fetches the new tail (incremental mode, §5). A probe error is
  logged and skipped (best‑effort)
  ([reconcilers/grafana.py:102‑125](../../../services/ingest/ingestion/reconcilers/grafana.py#L102-L125)).

`external_id` parity (versioned by `time`) makes the re‑walk idempotent: only
genuinely new annotations produce new observations. Pragmatic v1 — one cheap
query that can over‑reshare but never under‑reshares.

> **The reconciler covers the `grafana:annotation` channel only.** The live
> `grafana:alert` channel has no backfill/reconcile path — it is push‑only (alert
> backfill via the Loki state‑history timeline is the v2 from §1.2).

---

## 10. Revocation / recoverable‑error behavior

Grafana has **no dedicated revocation chokepoint** that disables the install (the
codebase has no `_maybe_disable_on_revocation` equivalent for Grafana — contrast
GitHub's `client.py` chokepoint). Instead, the client maps error classes to typed
`GrafanaApiError` codes ([client.py:246‑278](../../../services/ingest/integrations/grafana/client.py#L246-L278)):

| HTTP | `GrafanaApiError.code` | Behavior |
|------|------------------------|----------|
| `401`/`403` | `grafana_api_unauthorized` | token rejected / insufficient role (needs `annotations:read`) — propagates; the run surfaces the failure |
| `404` | `grafana_api_not_found` | endpoint/org not visible to the token — propagates |
| `429` (budget spent) | `grafana_api_rate_limited` | **recoverable** — the fetcher returns an empty unadvanced page so ShardFetch retries next tick ([fetchers/grafana.py:174‑182](../../../services/ingest/ingestion/fetchers/grafana.py#L174-L182)) |
| other non‑2xx | `grafana_api_error` | propagates |

The only *recoverable* branch is the rate‑limited one (the fetcher absorbs it).
The reconciler additionally swallows any probe exception as best‑effort
([reconcilers/grafana.py:102‑107](../../../services/ingest/ingestion/reconcilers/grafana.py#L102-L107)).

> **TODO(human):** there is no automatic install‑disable on a persistent
> `401/403` (revoked service‑account token) — those errors propagate and fail the
> shard, but the `grafana_installations` row is **not** flagged `disabled_at`.
> Whether that is intentional (operator re‑provisions the token) or a gap worth a
> chokepoint is not stated in the code; confirm the intended recovery story.

---

## 11. End‑to‑end summary

```
              ┌──────────────────── BACKFILL / POLL (pull) ─────────────────────┐
              │  SA Bearer token (org-scoped, long-lived)                       │
              │  planner: ONE grafana_org_annotations shard per install         │
 ORG-WIDE     │  fetcher: GET /api/annotations  (epoch-ms window, newest-first) │
 ANNOTATIONS  │     backward walk: to = min(time) - 1ms ; short page = done     │
              │     tag _fyralis_record_type=annotation, _fyralis_instance      │
              │        ├─ alertId/newState  → kind=state_change                 │
              │        └─ plain             → kind=signal                       │
              │  → handle_grafana_annotation  (grafana:annotation)              │
              │     external_id = grafana:{instance}:annotation:{id}:{time}     │
              └─────────────────────────────────────────────────────────────┬──┘
                                                                             │
              ┌──────────────────────── LIVE (push) ─────────────────────────┴┐
 ANY alert    │  Grafana Alerting webhook ──POST──► /webhooks/grafana          │
 fires/resolves│   verify X-Grafana-Alerting-Signature (HMAC-SHA256, bare hex) │
              │   tenant = externalURL host → provider_installations           │
              │   NO replay window / NO replay cache (dedup via external_id)    │
              │  → handle_grafana_alert  (grafana:alert)  ONE draft / delivery  │
              │     external_id = grafana:{instance}:alert:{ghash}:{status}:{ts}│
              └────────────────────────────────────────────────────────────────┘

      INDEPENDENT STREAMS — two handlers, two external_id namespaces, no collision.
```

**Key invariants**

1. **Two channels, two namespaces, no collision.** The pull surface
   (`GET /api/annotations`, incl. auto alert‑state‑change annotations) →
   `grafana:annotation` keyed on the annotation id; the push surface (Alerting
   webhook groups) → `grafana:alert` keyed on `groupKey`‑hash + status. They share
   neither handler nor dedup key.
2. **One credential model.** A single org‑scoped **service‑account Bearer token**
   reads everything; no OAuth, no per‑user tokens, no `code` exchange.
3. **Two install rows, one onboarding step.** `grafana_installations` (backfill)
   and `provider_installations` (live webhook) are seeded together but stay
   independent; the instance host joins them.
4. **One backfill shard per install.** Annotations are org‑wide, so the planner
   emits exactly one `grafana_org_annotations` shard with a backward time‑window
   walk (no cursor token, no `Link` header).
5. **Versioned `external_id` = idempotent re‑fetch.** Annotation versioned by
   `time`; alert group versioned by `(status, rep_ts)`. A redelivery dedups; a
   genuine state change lands as a new observation. This is the *only* replay
   defense for the webhook (no timestamp window, no replay cache).
6. **Two documented v2s, both absent.** Alert backfill via the Loki state‑history
   timeline; per‑alert fan‑out of one webhook group.

---

## 12. Configuration & compliance

Verified against the code paths above (Grafana 12.0 Alerting HMAC, annotations
API, service‑account auth).

### 12.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `GRAFANA_BACKFILL_WINDOW_DAYS` | `90` | Lower‑bound window for the FULL annotations walk; `0` = all time (no floor) ([fetchers/grafana.py:73‑79](../../../services/ingest/ingestion/fetchers/grafana.py#L73-L79)) |
| `GRAFANA_ANNOTATIONS_PAGE_SIZE` | `100` | annotations page `limit` (clamped 1–100) ([fetchers/grafana.py:66‑70](../../../services/ingest/ingestion/fetchers/grafana.py#L66-L70)) |
| `GRAFANA_RL_MAX_ATTEMPTS` | `4` | client 429‑retry budget ([client.py:153](../../../services/ingest/integrations/grafana/client.py#L153)) |
| `GRAFANA_RL_MAX_SLEEP_SEC` | `30` | max sleep per `Retry-After` ([client.py:154](../../../services/ingest/integrations/grafana/client.py#L154)) |
| `GRAFANA_WEBHOOK_TIMESTAMP_HEADER` | `""` (off) | if set, names the timestamp header for `"{ts}:"+body` HMAC mode ([signatures/grafana.py:46‑48](../../../services/app/webhooks/signatures/grafana.py#L46-L48)) |
| `GRAFANA_API_BASE_URL` | — | explicit backfill base-URL override used by Provider Lab ([endpoints.py:130](../../../lib/integrations/endpoints.py#L130)) |

### 12.2 Verified

- **Auth** — org‑scoped service‑account token presented as `Authorization:
  Bearer …`, resolved once and reused, never logged. ✅
- **Webhook signing** — HMAC‑SHA256, **bare hex** `X-Grafana-Alerting-Signature`,
  constant‑time compare, multi‑secret rotation. ✅
- **Pagination** — backward `from`/`to` ms‑window walk; short page = EOF (Grafana
  exposes no cursor/Link). ✅
- **Rate‑limit etiquette** — `Retry-After` honoured within a bounded budget; no
  dedicated token bucket (none needed). ✅
- **Idempotency** — versioned `external_id` per channel; redeliveries dedup. ✅
- **Two‑channel independence** — distinct handlers + distinct `external_id`
  namespaces; no cross‑channel collision. ✅

### 12.3 Dev / Provider Lab mode

`build_grafana_client` detects Provider Lab mode and **presets** the token to
`spam-grafana`, skipping the secret‑store lookup; the API base is overridden via
the endpoint resolver to Provider Lab's explicit `/grafana` URL
([_clients.py:337‑363](../../../services/ingest/ingestion/fetchers/_clients.py#L337-L363),
[endpoints.py:69](../../../lib/integrations/endpoints.py#L69), [160](../../../lib/integrations/endpoints.py#L160)).
The canonical
[Provider Lab Grafana adapter](../../../services/ingest/synthetic/provider_lab/wave_b.py)
serves `GET /api/annotations` (bare array, ms‑windowed, newest‑first) and
`GET /api/org`, so the real `GrafanaClient` + fetcher + reconciler run end‑to‑end
with no Grafana instance.

> **Note.** This page is the **deep** flow reference; the short source card lives
> at [../sources/grafana.md](../sources/grafana.md) and is not duplicated here.
