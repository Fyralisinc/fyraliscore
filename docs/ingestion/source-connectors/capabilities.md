# Source connector capabilities

Capabilities are small, independently versioned facets. A manifest declaration
is static support; binding determines configured support for one installation;
health determines current operational availability.

| Capability | Interface responsibility | Host remains responsible for |
| --- | --- | --- |
| `installation.configure/v1` | Validate connector configuration | Persisting authorized configuration |
| `installation.oauth2/v1` | Build authorization redirect; exchange callback code | State/tenant validation, secret persistence, authority record |
| `installation.oauth2_lifecycle/v1` | Refresh and revoke OAuth grant | Rotation fencing, current/pending credential state |
| `installation.secret_rotation/v1` | Verify a candidate credential | Secret storage and atomic promotion |
| `resource.discovery/v1` | Enumerate provider resources | Selection policy and durable configuration |
| `ingestion.historical_pull/v1` | Plan shards and fetch pages | S3/Kafka publication, checkpoints, retries, DLQ |
| `ingestion.incremental_poll/v1` | Fetch from an installation cursor | Poll scheduling and checkpoint commit |
| `ingestion.webhook/v1` | Verify and decode a bounded raw request | Endpoint tenancy, response policy, durable publication |
| `ingestion.push_subscription/v1` | Ensure, renew, revoke provider subscription | Callback allocation and renewal scheduling |
| `ingestion.gateway_stream/v1` | Open, receive, close a resumable stream | Worker process, lease, durable raw publication |
| `ingestion.reconciliation/v1` | Identify repair work | Repair scheduling and execution policy |
| `semantic.identity/v1` | Produce stable external identity | Global idempotency and entity-resolution policy |
| `semantic.normalization/v1` | Produce observation drafts | Normalized envelope publication and domain writes |
| `health.probe/v1` | Report installation health | Lifecycle transition and alert policy |
| `lifecycle.cleanup/v1` | Revoke/clean connector-owned resources | Authority revocation, credential retirement, audit retention |

## Resolution

Code requests a canonical `CapabilityKey`, such as `HISTORICAL_PULL_V1`. The
registry verifies that the manifest declares the same reference and that the
factory result satisfies its runtime-checkable protocol. A bound connector may
return no optional facet; `require()` raises a typed unavailable error.

Capability identifiers and versions form the wire contract. Python class names
and provider SDK versions do not.

## Capability design rules

- Add a new version for a breaking DTO or semantic change; do not silently
  change v1.
- Keep provider-specific payloads in `SourceRecord.payload` or connector-owned
  state, not shared contract DTO fields.
- Prefer a new orthogonal facet to a growing source-specific root interface.
- Make operation IDs, cursors, and cleanup behavior retry-safe.
- Declare required secret slots, outbound hosts, scopes, and trust ceiling in
  the manifest.
- Do not interpret a declared facet as permission. Binding requires matching
  durable granted authority.

## OAuth

OAuth begin and completion are connector-owned facets. The host validates the
callback transaction and tenant, binds with bootstrap authority, validates the
returned granted scopes, writes secret candidates, persists installation
provenance, and promotes credential references. Refresh returns replacement
secret candidates; revocation reports remote completion. Tokens never belong in
`OAuthResult.metadata`.

## Ingestion

Pull and poll return pages with records and cursor proposals. Webhook returns
verified events. Gateway returns bounded batches and resume state. In every
case, the host owns the durability boundary. A connector cannot claim success
for a checkpoint merely because the provider request succeeded.

## Semantic facets

Identity and normalization are intentionally separate from transport. Identity
must be stable across backfill and live ingress. Normalization produces immutable
observation drafts and must preserve source provenance and trust constraints.

