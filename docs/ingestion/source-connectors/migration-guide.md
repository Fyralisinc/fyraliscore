# Source connector contract-only migration record

## Status

The repository migration is complete. All 26 source families use stable-v1
manifests, first-party connector factories, common installations, durable
authority, registry-resolved capabilities, and generic runtime owners. The old
source planner/fetcher/reconciler/handler/integration trees and source-specific
webhook/gateway/poller launchers have been removed.

Because Fyralis is not in production, the final cutover deliberately chose a
single runtime over availability-preserving dual execution. There is no
legacy/shadow route and no source-specific fallback.

## Migration 0190

`0190_source_connector_contract_only.sql` is the final schema cutover. It:

- adds installation-scoped callback linkage to onboarding;
- backfills common connector installation IDs from old onboarding references;
- removes the old generic/dedicated onboarding installation columns and indexes;
- moves remaining Google/AWS configuration into namespaced common installation
  data;
- places incompatible imported gateway/AWS rows in `Maintenance` for explicit
  credential/configuration repair;
- rewrites active routing policies to `{global: connector}` and adds database
  constraints rejecting source overrides and legacy/shadow modes;
- removes parity/legacy rollout data and the dual-runtime retirement-evidence
  table;
- updates operator audit admissibility for the common lifecycle CLI.

The migration intentionally does not claim that imported credentials are valid.
Only current secret references configure provider capabilities; incomplete rows
remain unavailable.

## Preserved invariants

| Invariant | Contract-only owner |
| --- | --- |
| Canonical source identity | `source-index.json` and manifest validation |
| Tenant and installation identity | common installation + authority binding |
| Raw durability | host raw-emission port writes S3 before Kafka |
| Checkpoint/resume ordering | generic execution router commits state after emission |
| Idempotency and envelopes | existing versioned raw/normalized data-plane contracts |
| Provider retry meaning | typed connector error classification |
| Webhook trust | connector verification after installation-scoped callback lookup |
| Poll/watch/gateway state | namespaced CAS installation data |
| Pause/maintenance/removal | continuous common lifecycle controller |
| Operator audit | common lifecycle CLI and `operator_action_log` |

## Source family cutover

- REST/API sources moved to explicit first-party factories backed by governed
  HTTP, provider-owned auth semantics, identity, normalization and webhook/poll
  capabilities.
- Slack, Notion and QuickBooks retain connector-owned OAuth authorization and
  lifecycle facets; other token/API-key sources use the common configuration
  facet.
- Gmail, Google Calendar and Google Drive moved to Google-specific watch,
  Pub/Sub/callback and cursor capabilities.
- Discord, Telegram and Signal moved to the gateway/session capability and one
  generic supervisor.
- AWS moved to a connector-owned CloudTrail SigV4 implementation with
  manifest-declared regional egress and namespaced region configuration.

## Runtime owners removed

The migration removed direct source client builders, central endpoint dispatch,
mutable planner/fetcher/reconciler registries, source handler and channel maps,
source-specific webhook verifiers/resolvers, source sandbox workers, dedicated
installation CLIs, and source-specific poll/watch/gateway launchers.

Non-source product webhooks such as Linear and Stripe billing remain in the
application webhook subsystem. They are not Fyralis ingestion source families
and therefore are intentionally outside this migration.

## Rollback after cutover

Rollback means reverting an artifact revision, repairing configuration, pausing
an installation, or reverting the deployment/database change before data is
accepted. It never invokes a second source implementation. Already acknowledged
raw objects and Kafka envelopes remain authoritative and resume through the
host-owned checkpoint/idempotency model.

## Deployment prerequisites

Repository completion does not manufacture live-provider evidence. Before a
production launch, apply migration `0190` to a staging clone, reauthorize rows in
`Maintenance`, configure signed artifact admission, exercise provider sandboxes,
and validate Kafka/S3/checkpoint behavior under the target infrastructure. These
are deployment certification tasks, not missing legacy migration work.
