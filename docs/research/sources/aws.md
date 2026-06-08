# AWS — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (7/8 claims survived 3-vote verification). Date: 2026-06-08.

**Novel — IAM-based (SigV4 / AssumeRole).** No existing archetype; closest functional analog is Grafana (service-account token + org-wide shard + time-window backfill) but mechanically distinct (AWS SDK SigV4, not a Bearer header; live via SQS/EventBridge, not an HMAC webhook). Effort: **M–L**.

---

## TL;DR

AWS exposes a multi-service audit and operational intelligence corpus: CloudTrail records every control-plane API call across the org (verified `eventID` GUID, verified 90-day LookupEvents window, verified 2 req/s/account/Region rate limit); CloudWatch alarm transitions and AWS Health events provide real-time operational state changes; Cost Explorer supplies aggregated cloud-spend snapshots analogous to our Mercury/QuickBooks finance signals. Auth is not a Bearer token or OAuth dance — it is AWS SigV4 request signing via either a scoped IAM access-key pair or, preferably, cross-account `AssumeRole` with an `external-id` yielding rotating STS credentials; neither pattern exists in any current source. The live edge cannot reuse our HMAC webhook machinery — AWS has no per-event signed HTTP callback; the recommended v1 path is SQS polling of an EventBridge rule (IAM-pull, no public endpoint, no signature verifier to build). The main catches are the 90-day LookupEvents hard cap (S3-delivered trail required for deep backfill), the PII/secrets exposure risk in raw `requestParameters`/`responseElements`, and the net-new credential-refresh logic needed for the AssumeRole path.

---

## What companies use it for — and what signal lives there

Engineering organizations running infrastructure on AWS generate a continuous stream of control-plane activity, security events, cost data, and operational state changes — all readable under a single org-admin IAM principal. Four distinct intelligence scenarios emerge:

- **Infra org running EC2/EKS/Lambda/RDS with an Organization CloudTrail.** Whouses: Platform/DevOps and security teams; finance for cost. Signal: full audit timeline of resource creates/deletes, IAM and security-group changes, failed/denied API calls (potential intrusion or misconfiguration), deploy-adjacent activity, and the principals behind each action → operational velocity, security posture, and change blast-radius signals.
- **Security/compliance monitoring (SOC2/ISO) of privileged actions.** Whouses: Security engineers, compliance/audit. Signal: root-account usage, console logins (with/without MFA), policy changes, KMS key use, and access-denied spikes — high-value `state_change` observations for anomaly and posture intelligence; cross-account access surfaced via `sharedEventID` + `recipientAccountId`.
- **FinOps / cloud cost governance.** Whouses: Finance, FinOps, engineering managers. Signal: daily/monthly spend by service/account/tag from Cost Explorer (or CUR), cost-spike detection, and correlation of cost jumps with the CloudTrail resource-create events that caused them — a finance signal analogous to Mercury/QuickBooks burn-rate.
- **Production incident lifecycle.** Whouses: SRE/on-call, eng leadership. Signal: CloudWatch alarm fire/resolve transitions and AWS Health provider-side events as `state_change` observations → incident timeline, MTTR proxies, and reliability trends, correlatable with the deploy/config CloudTrail events that preceded an alarm.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| CloudTrail event (management) | Every control-plane API call across the org: who (`userIdentity`) did what (`eventName`/`eventSource`) to which resource, when (`eventTime`), from where (`sourceIPAddress`), success/error. Returned by LookupEvents with fields `EventId`, `EventName`, `EventSource`, `EventTime`, `ReadOnly`, `Resources[ResourceName/ResourceType]`, `Username`, `AccessKeyId`, plus the full `CloudTrailEvent` JSON blob. | `eventID` (GUID, dedup PK, verified non-optional since 1.01), `sharedEventID` (cross-account fan-out group), `userIdentity` (IAM principal, non-optional since 1.0), `recipientAccountId`, `eventVersion` (currently 1.11; major== / minor>= compatibility), `eventName`, `eventSource`, `eventTime`, `readOnly`, `requestParameters`, `responseElements`, `errorCode` | Highest-value signal: a complete audit timeline of every admin action, IAM/security change, resource create/delete, and failed/denied call. Maps directly to `state_change`. Reveals operational velocity, security posture, who-touched-what attribution, blast-radius of changes, and anomalous principals. |
| CloudTrail data event | Object-level / data-plane operations (e.g. S3 GetObject/PutObject, Lambda Invoke, DynamoDB item ops). High-volume; opt-in per-trail. Same record shape as management events but `eventCategory='Data'`. | `eventID`, `eventSource`, `eventName`, `resources[]`, `readOnly`, `requestParameters` | Fine-grained data-access signal. Very high volume = firehose risk. **Treat as opt-in / out-of-scope v1** unless a specific intelligence need justifies it (mirrors how Grafana raw time-series was scoped out). |
| CloudWatch alarm state change | Alarm transitions OK ↔ ALARM ↔ INSUFFICIENT\_DATA, delivered via SNS or EventBridge. Closest analog to a Grafana alert. | `alarmName`, `newState`/`oldState`, `stateReason`, `stateChangeTime`, `namespace`/`metric` | Operational health and incident signal → `state_change` observation. Captures when systems break and recover; correlates with deploy/cost events. Cleanest live-edge candidate. |
| AWS Health event | AWS-side service issues, scheduled maintenance, and account-affecting notifications (Health API / EventBridge Health events). | `eventArn`, `eventTypeCode`, `statusCode`, `affectedEntities`, `startTime`/`endTime`, `eventScopeCode` | Externally-caused operational risk signal (provider outages affecting the company's own services). Useful for incident correlation; low volume. Requires Business/Enterprise Support tier — see Open Questions. |
| Cost Explorer cost & usage | Aggregated daily/monthly cost and usage by service/account/tag via `GetCostAndUsage` API (or CUR files in S3). | `TimePeriod`, service dimension, linked account, `UnblendedCost`, `usageType`, tags | Financial/burn-rate signal analogous to Mercury/QuickBooks finance entities. Cloud spend trend, per-team/per-service cost, sudden cost spikes. Aggregate (not event) shaped → periodic-snapshot observation, not a per-event stream. See Open Questions. |
| IAM / Organizations inventory (derived) | Account list, IAM users/roles, and org structure obtainable from Organizations + IAM list APIs to resolve tenant/account attribution. | `accountId`, `accountName`, OU, role ARNs | Not a signal stream itself but the attribution backbone — needed to map `recipientAccountId` / `userIdentity` to a tenant and human owner. Equivalent to Jira's project enumeration or Slack's workspace map. |

---

## API & authentication

**API style:** AWS SDK over SigV4-signed REST/JSON (`botocore`/`boto3`). Each AWS service (CloudTrail, CloudWatch, Health, Cost Explorer, Organizations/IAM) exposes its own SigV4 API — not a single unified endpoint. `botocore` is already vendored in the repo, so no new dependency is required.

**Key endpoints/APIs:**

| Endpoint / API | Status | Notes |
|---|---|---|
| `cloudtrail:LookupEvents` | VERIFIED | Recent 90-day query; MaxResults 1–50 (default 50), NextToken, 2 req/s/account/Region; results most-recent-first |
| S3 `GetObject` over a CloudTrail trail bucket | VERIFIED (mechanism) | Deep backfill path — gzipped JSON log files, no per-call rate limit; `AWSLogs/<acct>/CloudTrail/<region>/YYYY/MM/DD/` prefix |
| `cloudwatch:DescribeAlarmHistory` / EventBridge alarm-state-change events | Listed in profile | Live alarm transitions |
| `health:DescribeEvents` / EventBridge AWS Health events | Listed in profile | Business/Enterprise Support required |
| `ce:GetCostAndUsage` | Listed in profile | ~24h data latency; must be explicitly enabled |
| `organizations:ListAccounts` + `iam:ListRoles` | Listed in profile | Attribution inventory |

**Docs:** [CloudTrail LookupEvents API Reference](https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupEvents.html)

**Auth mechanism:** AWS SigV4 request signing — not a Bearer token or OAuth. Two options:

- **Scoped IAM access-key pair** (access-key-id + secret-access-key): static-token analog closest to Jira/Mercury `secret_ref`; stored encrypted in `encrypted_secrets` via `secret_ref` on the install row. Simpler but carries rotation burden.
- **Cross-account AssumeRole + external-id** (preferred): the cloud admin creates a read-only IAM role in their account; we call `STS:AssumeRole` with an `external-id` to obtain rotating temporary credentials. More secure, but is **net-new credential-refresh logic** — no existing source uses STS temp creds.

**Org vs. per-user:** ORG / account-level, not per-user. One IAM principal sees the whole account/Organization. No per-user OAuth dance; CloudTrail already records every user's actions under the org-admin-readable trail. Resembles Google DWD org-token and QBO realm models more than per-user Slack OAuth.

**Scopes / permissions required** (read-only managed policies):
`cloudtrail:LookupEvents`, `cloudtrail:GetTrailStatus`, `s3:GetObject` + `s3:ListBucket` on the trail bucket, `cloudwatch:DescribeAlarm*`, `health:DescribeEvents` (Business/Enterprise Support required), `ce:GetCostAndUsage`, `organizations:ListAccounts`, `iam:List*`. Provisioned via customer-created IAM role (CloudFormation/Terraform template, least-privilege).

**Admin requirements:** AWS account/Organization administrator must create the cross-account IAM role (with external-id) or issue scoped access keys, and must enable an (Organization) CloudTrail trail delivering to S3. AWS Health API requires Business or Enterprise Support tier. Cost Explorer must be explicitly enabled.

---

## Backfill (historical pull)

**Supported:** Yes — two tiers.

- **Recent tier:** `LookupEvents` covers the last 90 days only (verified hard cap). 2 req/s/account/Region (verified). MaxResults 1–50, NextToken, results most-recent-first.
- **Deep tier (production path):** S3-delivered CloudTrail trail retains gzipped log files for the full bucket lifecycle (years). No per-call rate limit. Walk date-prefix `AWSLogs/<acct>/CloudTrail/<region>/YYYY/MM/DD/` and stream events. `ListObjectsV2` with `ContinuationToken`.

**Pagination:**
- LookupEvents: NextToken **must** be re-sent with identical original parameters; backward time-walk advances the upper bound backward (same pattern as `fetch_page_grafana`'s `page_to_ms = min_time - 1`).
- S3 backfill: `ListObjectsV2 ContinuationToken` + date-prefix iteration; cursor = last-processed S3 key.

**History depth:** 90 days via LookupEvents (verified hard cap). Effectively unbounded (bucket-lifecycle-limited, typically multi-year) via the S3 trail. A new account with no pre-existing trail has zero history before trail creation.

**Rate limits:** LookupEvents: 2 req/s/account/Region (verified). Multi-Region/multi-account org backfill is the bottleneck. The S3 trail path sidesteps this entirely.

**Maps to our pipeline:** The shard model is org-events shaped — one shard per (account or Organization) trail, analogous to Grafana's single `grafana_org_annotations` shard, NOT per-resource fan-out like Jira's per-project shards. The cursor round-trips the opaque `workflow_states.state_data` dict exactly like `GrafanaCursor`: for the recent tier, `page_to_ms`-style backward walk + `NextToken`; for the S3 tier, last-processed S3 key + `ContinuationToken`. On throttling, the fetcher returns `FetchResult(records=[...], next_cursor=<unadvanced>, end_of_data=False)` — mirroring the `GrafanaApiError` rate-limit branch — so the `ShardFetch` N1 invariant holds and the cursor stays unadvanced.

---

## Live ingestion (real-time)

**Mechanism:** Push via AWS-native eventing — NOT a Grafana-style signed HTTP webhook. AWS has no per-event HMAC callback.

**Events deliverable live:**
- CloudTrail management API-call events (via EventBridge or S3 new-object notification)
- CloudWatch alarm state-change events (OK/ALARM/INSUFFICIENT\_DATA)
- AWS Health account/service events
- S3 object-created notification for new CloudTrail log files (deep-trail live trigger)

**Signature scheme:** If we expose an HTTPS endpoint for SNS subscriptions, verification is **SNS MESSAGE SIGNATURE** (X.509 RSA/SHA; `SigningCertURL` must be validated against an `amazonaws.com` host) plus `SubscriptionConfirmation` handling — **not** the HMAC-SHA256 shared-secret scheme our `grafana`, `jira`, `mercury`, and `quickbooks` verifiers use. This is a **new verifier archetype**. If we use SQS polling instead, no signature endpoint is needed (IAM-authenticated pull) — simpler and avoids a public confirmation endpoint.

**Maps to our pipeline:** The v1 recommended path is **path (d) / poll-only**, analogous to the gateway/direct-dispatch pattern: an EventBridge rule → SQS queue, polled by an ingest worker using IAM credentials (no HTTP status expected, `_EXPECTED_LIVE_STATUS["aws"] = set()`). This avoids building the SNS X.509 verifier and a public `SubscriptionConfirmation` endpoint. Live dedup uses `eventID`; backfill and live twins dedup naturally because both carry the same CloudTrail `eventID` GUID — `external_id = aws:{accountId}:{region}:event:{eventID}` is immutable (no time-versioning needed, simpler than mutable Grafana annotations or Jira issues). A v2 SNS-push path would require a net-new `services/app/webhooks/signatures/aws_sns.py` X.509 verifier registering in `VERIFIERS`.

---

## Can we gather this? — feasibility

**Verdict: Yes, definitively**, for our own or a consenting customer's AWS account.

**Access model:** Org/account-level admin access. A cross-account IAM role (`AssumeRole` + `external-id`, rotating STS temp creds) is the preferred and most secure model; scoped long-lived access keys are the simpler static-token analog. One principal sees the whole account/Org — no per-user fan-out. We (or our customer's cloud admin) provision the role or keys and enable the Organization CloudTrail.

**Legal / ToS:** AWS API access under the AWS Customer Agreement / Service Terms is fully permitted for an account owner pulling their own data. No scraping or ToS-gray-area concerns (unlike consumer platforms). We become a processor of the customer's CloudTrail logs under standard cloud-data-processor obligations.

**Compliance:** Primary concern. CloudTrail `requestParameters` / `responseElements` **can** contain PII, secrets, tokens, and sensitive resource names (AWS redacts some but not all). Data events can expose object-level access to sensitive S3 data. Raw CloudTrail JSON must be treated as sensitive: encrypt at rest in MinIO/S3, consider field redaction before the observation layer, honor the customer's data-residency (Region-scoped). Cost data is financial but aggregate. No end-to-end encryption barrier (unlike Telegram/Signal).

**Legal risk:** Low-to-moderate. Standard cloud-data-processor obligations + PII-in-logs handling. No anti-scraping or account-ban risk.

**Blockers (none hard; practical frictions):**
1. 90-day LookupEvents cap — deep history requires a pre-existing S3 trail.
2. 2 req/s LookupEvents throttle — multi-Region/multi-account backfill is slow without the S3 path.
3. AWS Health API requires Business/Enterprise Support tier.
4. Cost Explorer has ~24h data latency and must be explicitly enabled.
5. SNS-signature live verifier (v2 path) is net-new — no HMAC reuse.
6. AssumeRole credential-refresh logic does not exist in any current source.

**Confidence:** high.

---

## How it maps onto our pipeline

```
SOURCE: aws

Auth shape →            novel — IAM SigV4 (not Bearer/OAuth).
                        Option A: scoped IAM access-key pair (static; secret_ref on aws_installations,
                          analogous to jira/mercury static token).
                        Option B (preferred): cross-account AssumeRole + external-id -> STS temp creds
                          (rotating; role_arn + external_id on aws_installations; net-new credential-
                          refresh logic — no existing source uses STS).
                        base_url equivalent: account_id + role_arn + region(s).
                        NOT per-user OAuth, NOT a Bearer header.

Install table →         aws_installations (cols: id, tenant_id, account_id, role_arn [nullable],
                          secret_ref [nullable; mutually exclusive with role_arn], external_id,
                          regions[] text[], webhook_secret_ref [null for v1 SQS-poll path])
                        child resource table?: NONE — org-wide (one shard per account/Org trail,
                          like grafana_installations with no child shard table)

Backfill cursor →       dimension: time-window-walk (like Grafana) for recent tier;
                          S3-key high-water for deep tier
                        high_water field: eventTime (recent) / last S3 key (deep)
                        incremental floor: now − 90d for recent tier; oldest trail S3 key for deep
                        rate-limit-safe empty page: yes (return next_cursor=unadvanced, end_of_data=False)
                        shard_kind: "aws_cloudtrail_events"   one-shard (org-events, like Grafana)

Live mechanism →        v1: SQS poll (EventBridge rule → SQS; IAM-pull, no public endpoint,
                          no HTTP status) — path (d) gateway/direct-dispatch analog.
                        v2 (deferred): SNS → HTTPS → NEW X.509 SNS message-signature verifier
                          (cannot reuse HMAC machinery) + SubscriptionConfirmation handling.
                        signature: NONE for v1 SQS-poll; X.509 RSA/SHA for v2 SNS-push
                          (SigningCertURL must resolve to amazonaws.com host)
                        tenant identifier in payload: account_id (CloudTrail recipientAccountId);
                          extractor _extract_aws

New files →             services/ingest/ingestion/fetchers/aws.py
                          (FETCHER_DISPATCH['aws'], LookupEvents + S3-walk, AwsCursor)
                        services/ingest/ingestion/planners/aws.py
                          (PLANNER_DISPATCH['aws'], one org-events shard per account/Org trail)
                        services/ingest/ingestion/handlers/aws.py
                          (@register channels: aws:cloudtrail / aws:cloudwatch_alarm /
                           aws:health / aws:cost; kind branching)
                        services/ingest/ingestion/idempotency/__init__.py
                          (aws_cloudtrail_event, aws_cloudwatch_alarm, aws_health_event,
                           aws_cost_snapshot constructors)
                        services/ingest/ingestion/workflows/shard_fetch.py
                          (_LOAD_AWS_INSTALL_SQL branch in _load_install)
                        integrations/aws/client.py   (boto3/STS client, SigV4 auth, AssumeRole refresh)
                        integrations/aws/onboarding.py  (IAM role onboarding flow)
                        fetchers/_clients.py          (build_aws_client / open_aws_client)
                        [v2 only] services/app/webhooks/signatures/aws_sns.py
                          (NEW X.509 verifier; NOT HMAC — cannot extend existing verifiers)
                        [v2 only] services/app/webhooks/tenant_resolver.py
                          (_extract_aws + ResolverProvider Literal + PROVIDER_EXTRACTORS)
                        [v2 only] services/app/webhooks/router.py
                          (_CUTOVER_ENABLED_PROVIDERS / _PROVIDER_CHANNEL maps)
                        db/migrations/0095_aws.sql

Migration →             0095_aws.sql: aws_installations (id, tenant_id, account_id, role_arn,
                          secret_ref, external_id, regions[], ENABLE+FORCE RLS on
                          app.current_tenant, mirrors grafana_installations shape) +
                          source_check widening: DROP+re-ADD the inline source_check on ALL FOUR
                          substrate tables (source_onboarding_runs, onboarding_shards,
                          ingestion_failures, onboarding_triggers) listing all 13 sources
                          (12 existing + 'aws') as a strict superset. Landmine: re-running any
                          prior widening migration in integration tests must clean up 'aws' first.

Observation kind(s) →   state_change: CloudTrail management events (admin actions, IAM changes,
                            denied calls, resource create/delete), CloudWatch alarm transitions,
                            AWS Health events.
                          signal: read-only / informational CloudTrail events, Cost Explorer snapshots.
                          channels: "aws:cloudtrail", "aws:cloudwatch_alarm", "aws:health", "aws:cost"
                          trust_tier: "authoritative" (org-admin-readable audit log, like grafana/jira)
                          external_id: IMMUTABLE (eventID is a CloudTrail GUID, non-optional since 1.01,
                            does not change on re-fetch — no time-versioning needed)
                          format: aws:{accountId}:{region}:event:{eventID}
                          namespacing: account_id + region ensure global uniqueness across tenants
                            (satisfying the global UNIQUE(source_channel, external_id, occurred_at)
                            constraint with no tenant_id column — cross-tenant collision avoided)

Rate-limit risk →       MODERATE-HIGH on LookupEvents path: 2 req/s/account/Region (verified);
                          multi-Region org backfill multiplies by (accounts × Regions).
                          LOW on S3 trail path (S3 GET limits are far higher).
                          LOW for EventBridge/SQS live polling (generous service limits).
                          Fetcher must back off and return end_of_data=False on throttle.

Legal/ToS risk →        LOW for access (own/consenting account, AWS-sanctioned API use under
                          Customer Agreement). MODERATE for data handling: CloudTrail
                          requestParameters/responseElements can contain PII and secrets →
                          encrypt raw envelopes at rest, consider redaction before observation
                          layer, respect Region data-residency. We become a processor of customer
                          audit logs.

Effort →                M (single-account, LookupEvents-only recent window, SQS-poll live v1):
                          heavier than Grafana because SigV4/STS auth is more than a Bearer
                          header, but botocore is already vendored.
                        L (full Org + S3-deep-backfill + multi-Region + v2 SNS-push verifier):
                          net-new credential-refresh (AssumeRole), two-tier backfill cursor,
                          multi-account fan-out, PII redaction/compliance handling, and a new
                          X.509 SNS signature verifier.
```

**Prose walk-through:**

**Auth archetype.** This source is adjacent to no existing archetype — it is the first IAM-based source. The closest structural analog is Grafana (service-account token + `base_url` + org-wide single shard), but Grafana's auth is a Bearer header stored as a single `secret_ref`. AWS SigV4 requires either a static access-key pair (stored identically to a Jira/Mercury `secret_ref` — the simpler path) or an `AssumeRole` flow producing rotating STS credentials, which requires periodic credential refresh logic in `build_aws_client`. The `boto3` session/credential-provider chain can be wired to handle refresh transparently, but it must be explicitly coded in `integrations/aws/client.py` and plumbed through `open_aws_client` in `_clients.py`. Neither pattern is present in any current source.

**Install table.** `aws_installations` mirrors `grafana_installations` in shape (one row per account/Org, no child resource table, org-events shard from the planner). The `role_arn` + `external_id` columns accommodate the AssumeRole path; `secret_ref` covers the static-key fallback. `regions[]` is needed for multi-Region fan-out. RLS pattern is identical to `grafana_installations`.

**Backfill cursor.** The planner emits one shard per account/Org trail (`shard_kind = "aws_cloudtrail_events"`), not per-resource. The fetcher (`fetchers/aws.py`) walks LookupEvents backward in time (`page_to_ms` style) — same pattern as `fetch_page_grafana` — and advances the `AwsCursor` through `workflow_states.state_data`. The S3 deep-backfill path uses a parallel cursor dimension (last-processed S3 key), switchable via a cursor field. On a 2 req/s throttle response, return `end_of_data=False` with the unadvanced cursor so `ShardFetch`'s N1 invariant holds.

**Live mechanism.** v1 recommends path (d): SQS polling of an EventBridge rule covering CloudTrail, CloudWatch alarm-state-change, and Health events. The worker polls the SQS queue using IAM credentials (no public HTTPS endpoint, no signature verifier, no `SubscriptionConfirmation` handshake). `_EXPECTED_LIVE_STATUS["aws"] = set()` (direct-dispatch, no HTTP status). Because there is no HMAC, `_HMAC_SOURCES` does not include `aws`. v2 SNS-push would register a new `aws_sns.py` verifier (X.509, not HMAC) in `VERIFIERS` and add the tenant extractor and router maps, but this deferred.

**Observation kinds and channels.** The `handlers/aws.py` module registers four channels: `aws:cloudtrail` (management events → `state_change` for writes/errors, `signal` for read-only); `aws:cloudwatch_alarm` (transitions → `state_change`); `aws:health` (AWS-side events → `state_change`); `aws:cost` (Cost Explorer snapshots → `signal`, periodic). `trust_tier = "authoritative"` for all (org-admin audit log). `external_id` is immutable — `aws:{accountId}:{region}:event:{eventID}` — so no time-versioning suffix is needed (unlike Grafana annotations or Jira issues, which require a mutation dimension).

**External\_id namespacing.** The `external_id` includes `accountId` + `region` to satisfy the global `UNIQUE(source_channel, external_id, occurred_at)` constraint that has no `tenant_id` column. Cross-tenant collision is prevented because different tenants own different AWS accounts. The fetcher tags records with `_fyralis_account_id` (analogous to Grafana's `_fyralis_instance`) so the handler can construct the namespaced key consistently on both the backfill and live paths.

**Migration.** Latest on-disk migration is `0094_telegram.sql`. `0095_aws.sql` must `DROP`+re-`ADD` all four source-`CHECK` constraints on `source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, and `onboarding_triggers` with a strict superset of all 13 sources — following the documented re-run/superset landmine. `aws_installations` enables and forces RLS keyed on `app.current_tenant`.

**Rate-limit risk.** Moderate-high for multi-Region LookupEvents backfill; negligible for the S3 trail and SQS live paths. The fetcher must implement a backoff branch identical to the `GrafanaApiError` rate-limit branch.

**Compliance / legal risk.** The access itself is low-risk (AWS-sanctioned API use). The data handling is the risk: raw CloudTrail JSON can contain PII and secrets in `requestParameters`/`responseElements`. Raw envelopes should be encrypted at rest in MinIO/S3, and a redaction pass (or field-omission policy) should be decided before the handler layer.

---

## Open questions

- **Live edge decision:** SQS poll (IAM pull, no public endpoint, v1 recommended) vs SNS → HTTPS push (net-new X.509 SNS message-signature verifier + `SubscriptionConfirmation` handling; our HMAC webhook machinery does NOT apply). Which path does the contract owner want for v1?
- **Auth archetype:** Long-lived scoped IAM access keys (static-token analog, simpler, rotation burden on the customer) vs cross-account `AssumeRole` + `external-id` with rotating STS temp creds (more secure, but is net-new credential-refresh logic — no existing source uses STS temp creds). Which is the required v1 target?
- **Backfill depth:** Accept the 90-day LookupEvents cap for v1, or require customers to have a pre-existing S3 CloudTrail trail for multi-year history? A new account has zero history before trail creation — this should be communicated clearly during onboarding.
- **Scope of data events:** Management events only (recommended v1, analogous to Grafana annotations-only scope) or include high-volume data events (S3/Lambda object-level)? The latter is a firehose requiring explicit justification and a volume gate.
- **Multi-Region / multi-account fan-out:** Is one shard per (account × Region) acceptable given the 2 req/s/account/Region LookupEvents throttle, or must the S3 trail path be built up-front to avoid throttling a wide Organization?
- **`eventVersion` compatibility policy:** The parser must equal-compare the major version and `>=` the minor (current 1.11). Do we pin/alert on an unexpected major bump, or best-effort parse? Note: the `eventCategory`-based filtering claim was **refuted** in verification — do not rely on `eventCategory` for LookupEvents filtering.
- **PII/secrets in `requestParameters`/`responseElements`:** Redact at ingest, store encrypted raw-only, or pass through? This decision affects compliance posture and raw-envelope handling in `content["_raw"]`.
- **Cost Explorer shape:** Cost data is aggregate/snapshot-shaped (not per-event) with ~24h latency. Does it fit the per-event observations model or should it be a periodic-snapshot `signal` kind emitted on a schedule? CloudWatch metric time-series and CloudTrail data events are likely out-of-scope firehoses (parallel to Grafana raw time-series).
- **AWS Health Support tier dependency:** The Health API requires Business or Enterprise Support. Is Health in scope given many customers lack that tier, or should it be gated behind a capability flag on the install row?

---

## Sources

- <https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html> (primary) — CloudTrail event record field reference; `eventID`, `userIdentity`, `eventVersion` 1.11 claims
- <https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupEvents.html> (primary) — LookupEvents API; 90-day cap, 2 req/s/account/Region, MaxResults 1–50, NextToken, most-recent-first
- <https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_GetCostAndUsage.html> (primary) — Cost Explorer `GetCostAndUsage` API; `TimePeriod`, dimensions, linked account
- <https://docs.aws.amazon.com/health/latest/APIReference/API_DescribeEvents.html> (primary) — AWS Health `DescribeEvents`; Business/Enterprise Support requirement
- <https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_FilterLogEvents.html> (primary) — CloudWatch Logs API reference
- <https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_GetMetricData.html> (primary) — CloudWatch Metrics API reference
- <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_common-scenarios_third-party.html> (primary) — cross-account IAM role + `external-id` pattern; AssumeRole third-party scenario
