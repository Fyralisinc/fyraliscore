# Fyralis Source Connector Contract-Only Final Summary

Date: 2026-08-04
Scope: all 26 ingestion source families
Repository state: implementation complete for review; no commit, push, PR, or merge performed in this run

## Outcome

Fyralis now has one source connection and execution architecture. Every source
is defined by a stable-v1 manifest, registered through the immutable connector
catalog, installed in the common control plane, bound with durable least
authority, and executed through typed Source Connector capabilities.

The connector-local legacy architecture was removed. Production source code no
longer contains the old integration clients, planner/fetcher/reconciler maps,
source handler dispatch, source-specific webhook verifier registry, dedicated
installation CLI split, or source-specific poll/watch/gateway launchers.
Routing accepts only contract connector execution; artifact quarantine fails
closed rather than invoking another implementation.

## Source families

- Collaboration/session: Slack, Discord, Telegram, Signal, WhatsApp.
- Developer/operations: GitHub, Jira, Grafana, AWS.
- Google Workspace: Gmail, Google Calendar, Google Drive.
- Knowledge/design: Notion, Miro, Figma.
- Finance/equity: Mercury, QuickBooks, Brex, Ramp, Carta.
- People/recruiting: Gusto, Deel, Fireflies, HiBob, Ashby, LinkedIn.

All 26 have stable `sources.fyralis.io/v1` manifests, explicit first-party
factories, structural/behavioral evidence, identity and normalization facets,
and only their declared ingestion/lifecycle/authentication capabilities.

## Delivered architecture

### Contract-only foundation

- Canonical source index and manifest-derived catalog.
- Immutable registry with compatibility and evidence validation.
- Contract-only executor with typed failures and no fallback callable.
- Partial least-authority binding: provider facets appear only when their
  `configuredBy` credentials exist; pure semantic facets may bind independently.
- Host-owned secret access, governed HTTP/gateway, CAS installation data,
  callback allocation, leases, logging/metrics, S3-first raw emission and Kafka
  publication.

### Installation and authentication

- Common OAuth install/callback routes for connector-owned OAuth facets.
- Common configuration route for API keys, service tokens, manually supplied
  OAuth tokens, gateway credentials and AWS credentials.
- Manifest validation of secret slots, outbound hosts, scopes and namespaced
  installation data.
- Durable installation, authority, credential reference, callback, lifecycle
  and provenance records.
- OAuth state consumption, scope/slot validation, credential rotation,
  bootstrap retirement and incomplete-install `Maintenance` behavior.
- Installation-scoped webhook callback URLs; bare source webhook routes reject.

### Execution archetypes

- REST/OAuth provider calls use explicit source factories and governed HTTP.
- Google Workspace implements Gmail history/watch, Calendar sync/watch and Drive
  changes/watch; Gmail Pub/Sub uses Google OIDC and the other watches use
  installation-scoped nonce callbacks.
- Discord, Telegram and Signal use one gateway/session supervisor with durable
  resume state after publication acknowledgement.
- AWS CloudTrail performs connector-owned SigV4 using scoped access key, secret
  key and optional session token handles.
- A generic poll worker owns all incremental-poll capable sources.
- A continuous lifecycle controller owns health, pause, maintenance,
  degradation/recovery, cleanup, credential retirement, authority revocation
  and removal.

### Runtime cutover and deletion

- Gateway, webhook, onboarding/workflow, poll, subscription and gateway process
  owners resolve the common registry and installation authority.
- Compose, process manifest, Prometheus targets and PgBouncer checks use the
  generic connector workers.
- Legacy/shadow/source-specific routing policy is rejected in Python and
  constrained in PostgreSQL.
- Old source integration, planner, fetcher, reconciler, handler, signature,
  synthetic source harness and dedicated script trees were deleted.
- Direct non-source product webhooks (Linear and Stripe billing) remain
  intentionally separate from the 26 ingestion sources.

## Database migration

`0230_source_connector_contract_only.sql` completes the pre-production cutover:

- links onboarding to common connector installations and removes old provider/
  Gmail installation references;
- imports remaining Google and AWS configuration into declared namespaces;
- makes routing and rollout evidence connector-only and rejects legacy/shadow
  state;
- removes parity/legacy metrics and dual-runtime retirement evidence;
- updates common lifecycle operator audit actions;
- places incompatible imported specialized-auth rows in `Maintenance`.

No availability bridge was retained because the deployment is not in
production, per the approved scope.

## Verification evidence

Completed in this run:

- repository-wide pytest collection: **4,924 tests collected, zero errors**;
- focused source contract/runtime/connectors/platform/install/webhook/process
  suite: **133 passed, 9 skipped**;
- the nine skips are database-backed tests with no `DATABASE_URL`, including
  migration replay and lifecycle CLI integration;
- source connector release gate: **passed for all 26 native stable-v1
  candidates**;
- common lifecycle coverage gate: **passed for all 26 sources**;
- Python compilation for `services` and `scripts`: passed during the cutover
  validation pass.

The local environment does not contain Ruff and no database URL was supplied.
No claim is made that the live migration replay or full CI ran in this final
pass. The user explicitly deprioritized CI; these are transparent review notes,
not implementation blockers for the requested repository work.

The repository-wide technical-debt ratchet still reports 13 baseline mismatches
in unchanged non-connector files. The modified shard fetch loop was refactored
back under its function budget and is no longer a violation; unrelated modules
were not changed merely to make this source-connector work hide that existing
branch condition.

## Merge boundary

The source contract work is represented as a single contract-only architecture
and is ready for code review. Before an actual merge, review the large deletion
set and migration `0230`, run database-backed migration tests in an environment
with PostgreSQL/pgvector, and run the repository's normal lint/CI if desired.

Before production deployment, separately certify real provider credentials,
OAuth applications, watch/Pub/Sub configuration, AWS permissions, gateway
sessions, signed artifact records, S3/Kafka infrastructure and operational
alerts. Those deployment receipts are intentionally not fabricated here.

## Workspace note

`docs/research/entity-resolution-architecture-review.md` was pre-existing
untracked user work and was not modified or included in this implementation.
