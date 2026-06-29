# AWS (CloudTrail) Ingestion — How Fyralis Pulls AWS Data

This document explains, in detail, **how AWS data enters Fyralis**: which AWS API
is called, with which IAM credentials, and how an account's
**CloudTrail management events** — both ordinary control-plane actions **and**
CloudWatch alarm-state changes — are ingested.

It deliberately stops at the point where a CloudTrail event becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

AWS belongs to the **Grafana "time-window-backfill / poll-live" archetype**: a
historical query surface (CloudTrail `LookupEvents`) for backfill **and** a live
edge that is a **poll** (an SQS / EventBridge consumer), *not* an inbound HTTP
webhook. There is no HMAC-signed webhook anywhere in the AWS path.

---

## 1. The two ways data arrives

AWS data reaches Fyralis through **two independent paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* history via **`CloudTrail:LookupEvents`** over a time window | [planners/aws.py](../../../services/ingest/ingestion/planners/aws.py), [fetchers/aws.py](../../../services/ingest/ingestion/fetchers/aws.py) |
| **Live (real-time)** | New CloudTrail activity in the account | Fyralis *polls* an SQS / EventBridge queue and dispatches each event in-process (**no webhook**) | [integrations/aws/live_poll.py](../../../services/ingest/integrations/aws/live_poll.py) |

There is **no inbound webhook for AWS.** Unlike GitHub (HTTP push) or Slack
(Events API), AWS's live edge is a **poll**: a long-poll loop drains
CloudTrail-shaped events off a queue the customer owns and calls
`handle_polled_event` directly, the way the Telegram gateway or Gmail's Pub/Sub
edge dispatches in-process
([live_poll.py:1-26](../../../services/ingest/integrations/aws/live_poll.py#L1-L26)).
Because there is no signed HTTP body, **AWS is not registered in the webhook
`VERIFIERS` map** — that registry covers only HMAC-webhook providers, and AWS is
deliberately absent ([signatures/__init__.py:44-68](../../../services/app/webhooks/signatures/__init__.py#L44-L68)).

Crucially, **both paths produce the exact same record shape** — a CloudTrail
event dict tagged with `_fyralis_record_type="event"` plus its
`_fyralis_account_id` / `_fyralis_region` namespace — and both are parsed by the
**single** `aws:event` handler
([handlers/aws.py](../../../services/ingest/ingestion/handlers/aws.py)). The poll
edge applies **byte-for-byte the same tagging** the backfill fetcher applies
([live_poll.py:112-122](../../../services/ingest/integrations/aws/live_poll.py#L112-L122)
vs [fetchers/aws.py:216-221](../../../services/ingest/ingestion/fetchers/aws.py#L216-L221)),
and both channel-map entries resolve to the one channel
([channel_mapping.py:259-269](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L259-L269)):

```
("aws", "backfill") → "aws:event"
("aws", "poll")     → "aws:event"
```

Both derive the **same** dedup key from the same constructor
([handlers/aws.py:43-56](../../../services/ingest/ingestion/handlers/aws.py#L43-L56)):

```
external_id = "aws:{account_id}:{region}:event:{event_id}"   # IMMUTABLE
```

A CloudTrail `eventId` is globally unique and stable, so the key has **no version
suffix** — there is no mutation dimension to track. An event that is both
backfilled *and* delivered live collapses into **one** observation. This is the
central design invariant of AWS ingestion — the one handler, one dedup namespace.

> AWS uses **CloudTrail `LookupEvents` only** for both edges. There is no
> webhook, no signature verification, and no second API surface for live data —
> live is the same event shape arriving through a poll loop instead of a
> historical query.

---

## 2. Authentication & credential model — IAM SigV4

AWS ingestion authenticates with **IAM credentials** and **SigV4-signs every
request**. There is no OAuth token, no per-user token, and no webhook secret. The
signing is performed internally by the `aioboto3` / `botocore` service clients
(`cloudtrail`, `sts`), which also own endpoint resolution and throttle-retry, so
Fyralis never hand-rolls a SigV4 signer
([client.py:1-41](../../../services/ingest/integrations/aws/client.py#L1-L41)).

### 2.1 Two credential kinds

The install's `credential_kind` column selects the posture
([credentials.py:1-27](../../../services/ingest/integrations/aws/credentials.py#L1-L27),
[credentials.py:80-172](../../../services/ingest/integrations/aws/credentials.py#L80-L172)):

| `credential_kind` | What is stored (via `secret_ref`) | How creds are obtained | Expiry |
|-------------------|-----------------------------------|------------------------|--------|
| **`assume_role`** *(default, recommended)* | a cross-account **role ARN** (JSON `{role_arn, external_id?, duration_seconds?}` or a bare ARN string) | **STS `AssumeRole`** from the integration's own base identity (botocore default chain) → short-lived STS creds | refreshed **5 min** before stated expiry ([credentials.py:43-62](../../../services/ingest/integrations/aws/credentials.py#L43-L62)) |
| **`static_keys`** | a long-lived **access-key / secret pair** (`{access_key_id, secret_access_key, session_token?}`) | read directly from the secret store | never expire → resolved once ([credentials.py:57-62](../../../services/ingest/integrations/aws/credentials.py#L57-L62)) |

`resolve_credentials` raises `AwsApiError(code="aws_api_unauthorized")` rather
than ever returning empty credentials, so a production caller can never issue an
unsigned / anonymous call
([credentials.py:96-106](../../../services/ingest/integrations/aws/credentials.py#L96-L106)).

The `AwsClient` resolves creds lazily under an `asyncio.Lock`, caches them, and
re-resolves only when AssumeRole creds are within the refresh skew of expiry;
static keys are resolved once and reused
([client.py:137-161](../../../services/ingest/integrations/aws/client.py#L137-L161)).

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| Account id | `aws_installations.account_id` | the 12-digit target account; part of the external_id namespace |
| Region | `aws_installations.region` | e.g. `us-east-1`; part of the external_id namespace |
| Credential descriptor | `aws_installations.credential_kind` | `assume_role` \| `static_keys` |
| Secret material | `encrypted_secrets`, referenced by `aws_installations.secret_ref` | the role ARN **or** the access-key pair — never inline on the install row ([0101_aws.sql:60-86](../../../db/migrations/0101_aws.sql#L60-L86)) |

> **No webhook secret, no `provider_installations` row.** Because the live edge
> is a poll (not a signed webhook), there is no HMAC secret to store and no
> `provider_installations` row to seed — the poll edge resolves the
> tenant/install directly from `aws_installations` by `(account_id, region)`
> ([onboarding.py:1-18](../../../services/ingest/integrations/aws/onboarding.py#L1-L18)).
> IAM credentials (access key / secret / session token) are **never logged**;
> the account id is hashed before logging
> ([client.py:39-41](../../../services/ingest/integrations/aws/client.py#L39-L41),
> [client.py:79-81](../../../services/ingest/integrations/aws/client.py#L79-L81)).

### 2.3 The install flow (how an installation gets registered)

There is no OAuth handshake. `finalize_install`
([onboarding.py:34-89](../../../services/ingest/integrations/aws/onboarding.py#L34-L89))
**in one tenant-scoped transaction**:

1. **UPSERTs an `aws_installations` row** keyed on `(tenant_id, account_id,
   region)` — idempotent; a re-install clears `disabled_at`
   ([onboarding.py:52-68](../../../services/ingest/integrations/aws/onboarding.py#L52-L68)).
2. **Emits an `onboarding_triggers` row** (`source='aws'`, `trigger_kind='install'`)
   so the existing M6 backfill chain fires
   (`oauth_poller → tenant_onboarding → source_onboarding → shard_fetch →
   reconciler`). The install id rides in `installation_row_id` purely for the
   idempotency dedup index — AWS is **not** a `provider_installations` source
   ([onboarding.py:70-86](../../../services/ingest/integrations/aws/onboarding.py#L70-L86)).

`source='aws'` is admitted by migration `0101` on all four M6 substrate tables
(runs / shards / failures / triggers)
([0101_aws.sql:107-134](../../../db/migrations/0101_aws.sql#L107-L134)).

---

## 3. The AWS API surface that is actually called

All reads funnel through `AwsClient`, which builds an `aioboto3` service client
signed from the resolved credentials and lets a botocore `Config` own
throttle-retry (standard mode, exponential backoff + jitter,
`AWS_RL_MAX_ATTEMPTS`=4)
([client.py:163-198](../../../services/ingest/integrations/aws/client.py#L163-L198)).
Botocore `ClientError`s are mapped into the shared `AwsApiError` taxonomy
(`aws_api_throttled` / `aws_api_unauthorized` / `aws_api_not_found` /
`aws_api_error`) by `_map_botocore_error`
([credentials.py:175-201](../../../services/ingest/integrations/aws/credentials.py#L175-L201)).

| AWS API | Wrapper | Purpose | Code |
|---------|---------|---------|------|
| `CloudTrail:LookupEvents` | `AwsClient.list_events()` | one page of management events in `[from_ms, to_ms]` + a `NextToken` | [client.py:204-253](../../../services/ingest/integrations/aws/client.py#L204-L253) |
| `CloudTrail:LookupEvents` (`MaxResults=1`) | `AwsClient.has_events_since()` | reconciler "anything newer?" probe | [client.py:255-264](../../../services/ingest/integrations/aws/client.py#L255-L264) |
| `STS:GetCallerIdentity` | `AwsClient.describe_account()` | zero-permission connectivity / credential probe (seed script only) | [client.py:266-285](../../../services/ingest/integrations/aws/client.py#L266-L285) |
| `STS:AssumeRole` | `resolve_credentials()` | mint short-lived creds for `assume_role` installs | [credentials.py:126-166](../../../services/ingest/integrations/aws/credentials.py#L126-L166) |

`list_events` converts the fetcher's epoch-**milliseconds** `from_ms` / `to_ms`
into the tz-aware `StartTime` / `EndTime` datetimes CloudTrail's API expects, and
maps the opaque `cursor` to/from CloudTrail's `NextToken`
([client.py:223-253](../../../services/ingest/integrations/aws/client.py#L223-L253)).
It returns `{"events": [...], "next_cursor": str | None}` newest-first.

### 3.1 Pagination — opaque `NextToken`

Every `LookupEvents` call pages the same way: pass the prior page's `NextToken`
back as `cursor`; **end-of-data is a page that returns no token**
([client.py:247-253](../../../services/ingest/integrations/aws/client.py#L247-L253),
[fetchers/aws.py:228-230](../../../services/ingest/ingestion/fetchers/aws.py#L228-L230)).
CloudTrail caps `MaxResults` at **50/page**, so the client clamps `limit` to 50
([client.py:60-61](../../../services/ingest/integrations/aws/client.py#L60-L61),
[client.py:225-227](../../../services/ingest/integrations/aws/client.py#L225-L227))
and the fetcher's `AWS_EVENTS_PAGE_SIZE` is likewise clamped to `[1, 50]`
([fetchers/aws.py:58-62](../../../services/ingest/ingestion/fetchers/aws.py#L58-L62)).
The fetcher returns **one page** plus the next token to ShardFetch, which
persists it in the shard cursor and resumes on the next invocation (the N1
restorability invariant).

### 3.2 Rate limits

There is **no dedicated client-side token bucket for AWS** —
`rate_limit/buckets.py` declares no `("aws", …)` entry (verified: zero `aws`
references in that file). Throttle handling is delegated to **botocore's own
standard retry mode** (backoff + jitter), and if that budget is spent the client
surfaces `AwsApiError(code="aws_api_throttled")`, which the fetcher treats as a
soft empty round — it leaves the cursor unadvanced and ends the round
`end_of_data=False` so ShardFetch re-enters on the next tick
([fetchers/aws.py:202-210](../../../services/ingest/ingestion/fetchers/aws.py#L202-L210)).
This contrasts with GitHub's single per-app bucket and Slack's per-method
buckets.

---

## 4. Backfill scope — one shard per install

CloudTrail management events (and alarm-state changes) are **account/region-wide**
— there is no per-resource sub-table the way Jira has projects or Mercury has
accounts. So the planner emits **exactly one `aws_account_events` shard per
install**, mirroring the Grafana annotations precedent
([planners/aws.py:1-11](../../../services/ingest/ingestion/planners/aws.py#L1-L11)).

`ctx.source_client` is `None`: the planner reads **only the install row** (loaded
by SourceOnboarding's `_LOAD_AWS_INSTALL_SQL`), so it stays stateless like
Grafana / Calendar / Drive
([planners/aws.py:26-65](../../../services/ingest/ingestion/planners/aws.py#L26-L65),
[source_onboarding.py:563-569](../../../services/ingest/ingestion/workflows/source_onboarding.py#L563-L569)).
The shard carries `installation_id`, `account_id`, `region`, and the warm-start
`updated_cursor` (the install's `events_cursor_ms` — the high-water event
`eventTime` in epoch ms, or `None` on first sync)
([planners/aws.py:46-59](../../../services/ingest/ingestion/planners/aws.py#L46-L59))
at baseline `recency_score=1.0`. A missing `account_id` plans **zero** shards
(logged) ([planners/aws.py:36-39](../../../services/ingest/ingestion/planners/aws.py#L36-L39)).

---

## 5. Fetching — one shard kind, two sync modes

`fetch_page_aws` ([fetchers/aws.py:167-246](../../../services/ingest/ingestion/fetchers/aws.py#L167-L246))
streams one `(account, region)`'s CloudTrail events. ShardFetch calls it in a
loop, persisting the returned cursor between calls. The same fetcher serves both
modes ([fetchers/aws.py:1-35](../../../services/ingest/ingestion/fetchers/aws.py#L1-L35)).

### 5.1 The cursor

```python
class AwsCursor:
    high_water_time_ms: int | None    # max event eventTime (ms) seen — warm-start + reconciler ref
    events_cursor: str | None         # opaque CloudTrail NextToken for the NEXT page
    floor_ms: int | None              # frozen lower `from` bound for this run (None = no floor)
    to_ms: int | None                 # frozen upper `to` bound (== run start; window stable across pages)
    events_seen: int = 0              # diagnostic
    seeded: bool = False              # first-call setup done?
```

([fetchers/aws.py:82-107](../../../services/ingest/ingestion/fetchers/aws.py#L82-L107)).
It round-trips through `workflow_states.state_data` as an opaque dict (the M6.2a
contract) ([fetchers/aws.py:109-116](../../../services/ingest/ingestion/fetchers/aws.py#L109-L116)).

### 5.2 First-call seeding — FULL vs INCREMENTAL

On the first call (`seeded=False`) the fetcher freezes the window
([fetchers/aws.py:175-185](../../../services/ingest/ingestion/fetchers/aws.py#L175-L185)):

- `to_ms` = now (frozen so the window stays stable across pages).
- If the shard carries a warm `updated_cursor` (a re-onboarding or a reconciler
  reshare) → **INCREMENTAL**: `floor_ms` and `high_water_time_ms` are set to that
  high-water, so only newer events come back.
- Otherwise → **FULL**: `floor_ms = to_ms − window`, where the window is
  `AWS_BACKFILL_WINDOW_DAYS` (default **90 days**, matching CloudTrail's own
  management-event retention via `LookupEvents`; `0` = no floor)
  ([fetchers/aws.py:65-75](../../../services/ingest/ingestion/fetchers/aws.py#L65-L75)).

Within the window the walk advances the opaque `events_cursor` until a page
returns no token (`end_of_data=True`). The high-water `eventTime` is tracked
across every event on every page so the reconciler has a precise gap reference
([fetchers/aws.py:216-230](../../../services/ingest/ingestion/fetchers/aws.py#L216-L230)).

### 5.3 Record shaping

Each event dict is shallow-copied and tagged with the three private keys the
handler uses for external_id namespacing
([fetchers/aws.py:216-221](../../../services/ingest/ingestion/fetchers/aws.py#L216-L221)):

```python
rec["_fyralis_record_type"] = "event"
rec["_fyralis_account_id"]  = account_id
rec["_fyralis_region"]      = region
```

This is the **identical** tagging the live poll edge applies
([live_poll.py:112-122](../../../services/ingest/integrations/aws/live_poll.py#L112-L122)),
which is what gives the two edges a byte-identical external_id and the cross-path
dedup.

> The fetcher builds its `AwsClient` **inline** via the `_open_aws_client` test
> seam ([fetchers/aws.py:119-141](../../../services/ingest/ingestion/fetchers/aws.py#L119-L141)),
> which the synthetic harness rebinds to a `MockAwsClient`. This is a deliberate
> divergence from the Grafana pattern: the shared
> `fetchers/_clients.py::open_aws_client` opener exists
> ([_clients.py:687-710](../../../services/ingest/ingestion/fetchers/_clients.py#L687-L710),
> [_clients.py:925-930](../../../services/ingest/ingestion/fetchers/_clients.py#L925-L930))
> but is owned by the wiring phase; the inline seam is the production opener for
> now (see the source note).

---

## 6. The handler — shaping events into `ObservationDraft`

AWS is a **single-channel source** (like Jira): both the backfill walk and the
live poll feed the one `aws:event` channel
([handlers/aws.py:1-20](../../../services/ingest/ingestion/handlers/aws.py#L1-L20)).
`handle_aws_event` ([handlers/aws.py:230-239](../../../services/ingest/ingestion/handlers/aws.py#L230-L239))
resolves the `(account_id, region)` namespace from the `_fyralis_*` tags (with
real-CloudTrail `recipientAccountId` / `awsRegion` as fallback,
[handlers/aws.py:108-120](../../../services/ingest/ingestion/handlers/aws.py#L108-L120))
and builds the draft. The channel is registered into `CHANNEL_TRUST_MAP` via
`setdefault` at import time
([handlers/aws.py:242](../../../services/ingest/ingestion/handlers/aws.py#L242),
imported by [handlers/__init__.py:185](../../../services/ingest/ingestion/handlers/__init__.py#L185)).

### 6.1 The signal-vs-state_change branch

The single discriminator is whether the event carries a CloudWatch **alarm
transition** (an `alarmName` or a `newState`), mirroring Grafana's
annotation-vs-state-change split
([handlers/aws.py:123-136](../../../services/ingest/ingestion/handlers/aws.py#L123-L136),
[handlers/aws.py:154-167](../../../services/ingest/ingestion/handlers/aws.py#L154-L167)):

| Channel | `external_id` | `occurred_at` | `kind` | `object_type` | Trust tier |
|---------|---------------|---------------|--------|---------------|------------|
| `aws:event` — **plain management event** | `aws:{account}:{region}:event:{eventId}` | `eventTime` (epoch ms **or** RFC3339; else now) | **`signal`** | `management_event` | **authoritative** |
| `aws:event` — **CloudWatch alarm-state change** (`alarmName`/`newState` present) | *(same)* | *(same)* | **`state_change`** | `alarm_state_change` | **authoritative** |

The channel is `authoritative` because AWS CloudTrail is the system of record for
its own account's control-plane + alarm-state history
([handlers/aws.py:14-19](../../../services/ingest/ingestion/handlers/aws.py#L14-L19),
[handlers/aws.py:35-36](../../../services/ingest/ingestion/handlers/aws.py#L35-L36)).

Highlights:

- **`occurred_at`** parses `eventTime` as epoch ms first (synthetic / normalized
  form), then RFC3339 (the real API shape), falling back to "now"
  ([handlers/aws.py:94-101](../../../services/ingest/ingestion/handlers/aws.py#L94-L101)).
- **`content_text`** is `"[aws alarm] {name}: {prev} → {new}"` for an alarm, or
  `"[aws] {service}:{eventName}"` for a management event, truncated to 600 chars
  ([handlers/aws.py:160-167](../../../services/ingest/ingestion/handlers/aws.py#L160-L167)).
- **`source_actor_ref`** is the IAM principal that performed the action —
  `aws:iam:{arn}` (or `aws:iam:{principalId}`) from `userIdentity`; a
  machine-generated alarm event may be actorless (`None`)
  ([handlers/aws.py:169-182](../../../services/ingest/ingestion/handlers/aws.py#L169-L182)).
- **`entities_hint`** carries typed refs: `aws_account`, `aws_region`, and
  optionally `aws_service` (the `eventSource`), `aws_alarm`, and `aws_principal`
  ([handlers/aws.py:184-198](../../../services/ingest/ingestion/handlers/aws.py#L184-L198)).
- The full CloudTrail JSON is preserved on `content.cloud_trail_event` for audit /
  downstream enrichment ([handlers/aws.py:200-214](../../../services/ingest/ingestion/handlers/aws.py#L200-L214)).
- An event missing `eventId` is rejected with `ValidationError`
  ([handlers/aws.py:144-147](../../../services/ingest/ingestion/handlers/aws.py#L144-L147)).

---

## 7. Live (real-time) ingestion via the poll edge

**AWS has no inbound webhook.** Its live edge is a **poll**: an SQS / EventBridge
consumer long-polls a queue the customer owns, and for every CloudTrail-shaped
event it drains it calls `handle_polled_event`
([live_poll.py:177-231](../../../services/ingest/integrations/aws/live_poll.py#L177-L231)).
This is the Telegram-gateway `handle_update` analog — direct in-process dispatch,
no HTTP request — with one twist: **a single poll loop can drain events for many
installs**, so the tenant/install is resolved **per event** from
`aws_installations` by `(account_id, region)`, with no per-loop tenant binding
([live_poll.py:1-26](../../../services/ingest/integrations/aws/live_poll.py#L1-L26)).

The flow per event ([live_poll.py:177-231](../../../services/ingest/integrations/aws/live_poll.py#L177-L231)):

1. **Drop guards.** Non-dict, or missing `eventId` → drop silently.
2. **Namespace** the event: `(_fyralis_account_id | recipientAccountId,
   _fyralis_region | awsRegion)`; missing either → drop
   ([live_poll.py:74-84](../../../services/ingest/integrations/aws/live_poll.py#L74-L84)).
3. **Resolve the install**: `SELECT … FROM aws_installations WHERE account_id=$1
   AND region=$2 AND disabled_at IS NULL`; **no enabled install owns this
   account/region → drop ("not ours")**
   ([live_poll.py:87-109](../../../services/ingest/integrations/aws/live_poll.py#L87-L109)).
4. **Tag** the event with the same `_fyralis_*` namespace the backfill fetcher
   applies, giving an identical immutable external_id (cross-path dedup)
   ([live_poll.py:112-122](../../../services/ingest/integrations/aws/live_poll.py#L112-L122)).
5. **Cutover branch.** If `ingestion.kafka_path_enabled` for the tenant (and the
   Kafka/S3 deps are wired), shadow-write the canonical record to
   `ingestion.raw.aws` with **`ingress_kind="poll"`** and return — the normalizer
   + observation_writer produce the observation, concurrently with any in-flight
   backfill ([live_poll.py:140-161](../../../services/ingest/integrations/aws/live_poll.py#L140-L161),
   [live_poll.py:194-207](../../../services/ingest/integrations/aws/live_poll.py#L194-L207)).
6. **Inline fallback.** Otherwise call `core.ingest("aws:event", record, …)`,
   then a best-effort M2 shadow audit when `SHADOW_WRITE_ENABLED`
   ([live_poll.py:209-230](../../../services/ingest/integrations/aws/live_poll.py#L209-L230)).

> **No HMAC gate, by design.** The trust boundary is the **IAM-authenticated
> poll** of the customer's own SQS / EventBridge queue — there is no signed
> webhook header to verify (as with Telegram's MTProto connection / Gmail's
> Pub/Sub) ([live_poll.py:22-26](../../../services/ingest/integrations/aws/live_poll.py#L22-L26)).
> This is why AWS is absent from the webhook `VERIFIERS` map (§1).

The poll record body **is** the canonical record (byte-stable sorted-key
`orjson`) — the same shape the backfill path publishes — so the normalizer feeds
`handle_aws_event` identically and content-hash dedup / replay-from-raw hold
([live_poll.py:125-137](../../../services/ingest/integrations/aws/live_poll.py#L125-L137)).

---

## 8. Reconciliation — gap detection

`reconcile_aws` ([reconcilers/aws.py:130-172](../../../services/ingest/ingestion/reconcilers/aws.py#L130-L172))
re-checks each completed `aws_account_events` shard for new activity with **one
cheap probe** per shard ([reconcilers/aws.py:87-127](../../../services/ingest/ingestion/reconcilers/aws.py#L87-L127)):

1. Load the shard's stored `high_water_time_ms` from `workflow_states`; `None`
   (empty account / no cursor) → skip
   ([reconcilers/aws.py:74-84](../../../services/ingest/ingestion/reconcilers/aws.py#L74-L84)).
2. Call `has_events_since(from_ms = high_water + 1)` — a 1-row `LookupEvents`
   with an **exclusive** floor (high-water + 1 ms) so the high-water event does
   not re-match itself forever ([reconcilers/aws.py:98-102](../../../services/ingest/ingestion/reconcilers/aws.py#L98-L102),
   [client.py:255-264](../../../services/ingest/integrations/aws/client.py#L255-L264)).
3. If anything newer exists, **reshare** an `aws_account_events` shard at
   `recency_score=1.5`, warm-started (`updated_cursor = high_water`) so the
   re-walk runs in INCREMENTAL mode and only re-fetches the new tail
   ([reconcilers/aws.py:111-127](../../../services/ingest/ingestion/reconcilers/aws.py#L111-L127)).

A probe failure is best-effort: it is logged and the shard is skipped (no reshare,
no error) ([reconcilers/aws.py:104-109](../../../services/ingest/ingestion/reconcilers/aws.py#L104-L109)).
Because external_id is immutable, re-walked events dedup against what backfill
already wrote — the reconciler can over-reshare but **never under-reshares**, and
dedup makes re-walks idempotent
([reconcilers/aws.py:5-19](../../../services/ingest/ingestion/reconcilers/aws.py#L5-L19)).

The reconciler resolves the install with `set_pool_provider`-injected pool access
([reconcilers/aws.py:44-58](../../../services/ingest/ingestion/reconcilers/aws.py#L44-L58),
[reconcilers/aws.py:137-154](../../../services/ingest/ingestion/reconcilers/aws.py#L137-L154)).

---

## 9. Revocation / recoverable-error behavior

> **TODO(human):** AWS has **no automatic revocation chokepoint** today. Unlike
> GitHub (whose client flips `enabled=FALSE` on a `401 Bad credentials` / app
> `404`), no AWS code path sets `aws_installations.disabled_at` in response to an
> `aws_api_unauthorized` error. `_map_botocore_error` classifies
> `AccessDenied` / `ExpiredToken` / `InvalidClientTokenId` / etc. as
> `aws_api_unauthorized` ([credentials.py:187-190](../../../services/ingest/integrations/aws/credentials.py#L187-L190)),
> but that error simply **propagates** (the fetcher only special-cases
> `aws_api_throttled` — [fetchers/aws.py:202-210](../../../services/ingest/ingestion/fetchers/aws.py#L202-L210))
> and the run fails rather than disabling the install. `disabled_at` is only ever
> *cleared* (by a re-install — [onboarding.py:63](../../../services/ingest/integrations/aws/onboarding.py#L63))
> and only ever *read* (by the planner load SQL, the poll-edge resolve, and the
> reconciler). Confirm whether a revocation chokepoint is intended for AWS, and
> if so where it should live (the client, mirroring GitHub's
> `_maybe_disable_on_revocation`). I could not find a `why` for the current
> absence in the code.

**Recoverable errors that *are* handled:**

- **Throttling** (`aws_api_throttled`) — the fetcher leaves the cursor unadvanced
  and returns an empty, non-terminal round so ShardFetch retries on the next tick
  ([fetchers/aws.py:202-210](../../../services/ingest/ingestion/fetchers/aws.py#L202-L210)).
  Below that, botocore's standard retry mode already absorbs transient throttles
  ([client.py:163-173](../../../services/ingest/integrations/aws/client.py#L163-L173)).
- **AssumeRole expiry** — credentials are proactively refreshed 5 min before
  expiry (§2.1), so a long backfill never signs with a stale STS token.

---

## 10. End-to-end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  resolve IAM creds: assume_role (STS AssumeRole) OR static_keys  │
                          │     └─► SigV4-signed via aioboto3/botocore                        │
   ONE (account,region)   │  planner: read aws_installations row (source_client = None)      │
                          │     └─► exactly ONE aws_account_events shard / install            │
   time-window walk       │  fetcher: CloudTrail:LookupEvents [floor_ms, now]                 │
   (90-day floor)         │     FULL: floor = now-90d ; INCREMENTAL: floor = warm high-water  │
                          │     page via NextToken until no token (end_of_data)               │
                          │     └─► tag _fyralis_record_type/_account_id/_region              │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────────────── LIVE (POLL — no webhook) ─────────────┐│
   ANY CloudTrail activity│  SQS / EventBridge poll loop drains events                     ││
                          │     resolve install by (account_id, region) per event          ││
                          │     tag _fyralis_* (SAME as backfill)                           ││
                          │     kafka_path_enabled? → raw.aws ingress_kind="poll"           ││
                          │                          else → inline core.ingest              ││
                          │     trust boundary = IAM-authed poll (NO HMAC, NOT in VERIFIERS)││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_aws_event                │
                                                            │  one channel: aws:event           │
                                                            │  external_id =                    │
                                                            │   aws:{acct}:{region}:event:{id}  │
                                                            │  alarm? → state_change : signal   │
                                                            │  → ObservationDraft (authoritative)│
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill and the live poll both tag the
   event identically and land on `aws:event` with
   `external_id="aws:{account}:{region}:event:{eventId}"`. A backfilled event and
   its live-poll twin dedup to a single observation. The eventId is immutable, so
   the key has no version suffix.
2. **One install = one shard.** CloudTrail events are account/region-wide, so the
   planner emits exactly one `aws_account_events` shard per install (no
   per-resource child table) — the Grafana time-window-backfill archetype.
3. **IAM SigV4, two credential kinds.** `assume_role` (short-lived STS creds,
   refreshed pre-expiry) is the default; `static_keys` (long-lived pair) is the
   alternative. No OAuth, no per-user token, no webhook secret.
4. **The live edge is a poll, not a webhook.** No HMAC verification; the trust
   boundary is the IAM-authenticated poll of the customer's own queue. AWS is
   deliberately absent from the webhook `VERIFIERS` map.
5. **Window walk with a 90-day floor + opaque `NextToken`**, throttle absorbed by
   botocore retry + a soft non-terminal round; reconciler probes with an
   exclusive high-water floor and reshares incrementally (over-reshares safely,
   never under-reshares).

---

## 11. Configuration & compliance

Verified against AWS's documented CloudTrail `LookupEvents` contract
(50 results/page, 90-day management-event retention, `NextToken` paging) and IAM
SigV4 / STS AssumeRole semantics, as referenced inline in the client + migration.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `AWS_BACKFILL_WINDOW_DAYS` | `90` | lower-bound window (days) for the FULL walk; `0` = no floor. Default matches CloudTrail's `LookupEvents` retention ([fetchers/aws.py:65-75](../../../services/ingest/ingestion/fetchers/aws.py#L65-L75)) |
| `AWS_EVENTS_PAGE_SIZE` | `50` | per-page `MaxResults`, clamped to `[1, 50]` (CloudTrail's cap) ([fetchers/aws.py:58-62](../../../services/ingest/ingestion/fetchers/aws.py#L58-L62)) |
| `AWS_RL_MAX_ATTEMPTS` | `4` | botocore standard-mode retry budget for throttle/transient errors ([client.py:169](../../../services/ingest/integrations/aws/client.py#L169)) |
| `AWS_CLOUDTRAIL_ENDPOINT_TEMPLATE` | `https://cloudtrail.{region}.amazonaws.com` | logging + the moto/localstack override seam ([client.py:67-70](../../../services/ingest/integrations/aws/client.py#L67-L70)) |

Per-install knobs live on the `aws_installations` row, not env: `account_id`,
`region`, `credential_kind`, `secret_ref`, `backfill_window_days`,
`events_cursor_ms` ([0101_aws.sql:60-86](../../../db/migrations/0101_aws.sql#L60-L86)).

### 11.2 Verified compliant

- **Auth** — IAM SigV4 via aioboto3/botocore; `assume_role` (STS, refreshed 5 min
  pre-expiry) or `static_keys`; never returns anonymous creds. ✅
- **API surface** — `CloudTrail:LookupEvents` for both edges + `STS:AssumeRole` /
  `STS:GetCallerIdentity`. ✅
- **Pagination** — opaque `NextToken`, `MaxResults` ≤ 50, end-of-data on no
  token. ✅
- **Window floor** — 90-day default matching CloudTrail retention; window frozen
  at run start across pages. ✅
- **Cross-path dedup** — identical `_fyralis_*` tagging on backfill + poll →
  immutable external_id. ✅
- **Least secret surface** — credential material in `encrypted_secrets` via
  `secret_ref`; no webhook secret; creds never logged, account id hashed. ✅
- **Revocation chokepoint** — **absent** (see §9 TODO). ⚠️

### 11.3 Dev / spammer mode

There is **no spammer base-URL override** in `build_aws_client` (unlike the
HTTP-based sources): the synthetic harness instead **rebinds the fetcher's
`_open_aws_client` seam to a `MockAwsClient`**, so backfill runs against the mock
without resolving any real credentials
([_clients.py:687-710](../../../services/ingest/ingestion/fetchers/_clients.py#L687-L710),
[fetchers/aws.py:119-141](../../../services/ingest/ingestion/fetchers/aws.py#L119-L141)).
For real-AWS integration testing, `endpoint_override` points the aioboto3 clients
at **moto / localstack**
([client.py:124-130](../../../services/ingest/integrations/aws/client.py#L124-L130),
[client.py:196-198](../../../services/ingest/integrations/aws/client.py#L196-L198)).

The live poll path is exercised by `AwsPollGenerator`, which mints a fresh
CloudTrail-shaped event (unique `eventId`, a current-window epoch-ms `eventTime`),
resolves the tenant's real `aws_installations` row for namespacing, and dispatches
it through the **production** `handle_polled_event` → `shadow_write_raw(source="aws",
ingress_kind="poll")` chain — the same normalizer → observation_writer path as
backfill, with **no HTTP status** to assert
([live_generators/aws_poll.py:1-29](../../../services/ingest/synthetic/live_generators/aws_poll.py#L1-L29),
[live_generators/aws_poll.py:155-182](../../../services/ingest/synthetic/live_generators/aws_poll.py#L155-L182)).

> **Note (inferred).** Because the production SigV4 client path is replaced by
> `MockAwsClient` in the synthetic gate, the real signing / AssumeRole code is
> **not exercised in CI** — integration-testing it against moto/localstack is
> called out in the source as the remaining operator step
> ([client.py:6-11](../../../services/ingest/integrations/aws/client.py#L6-L11),
> [credentials.py:14-17](../../../services/ingest/integrations/aws/credentials.py#L14-L17)).
