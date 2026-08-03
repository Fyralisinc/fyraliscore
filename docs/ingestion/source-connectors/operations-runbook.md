# Source connector operations runbook

## Readiness checks

Before enabling connector routing:

1. Apply migrations through
   `db/migrations/0188_byoc_control_panel_access_grants_rls.sql` on a fresh or
   correctly versioned database.
2. Confirm the process reports a 26-connector registry and the expected
   registry fingerprint.
3. Confirm installation and authority rows exist for the target tenant.
4. Confirm required credential slots are current and authority is not revoked.
5. Confirm the installation lifecycle is `Ready` or intentionally `Degraded`.
6. Confirm the connector artifact is enabled, signed by a trusted key, built by
   an allowed builder, matches the digest measured from the running module and
   exact manifest, and is not quarantined.
7. Confirm an active routing revision exists and
   `source_connector_rollout_events` is receiving bounded execution, duration,
   parity, lifecycle, and DLQ evidence for that revision.

## Runtime configuration

| Variable | Meaning |
| --- | --- |
| `SOURCE_CONNECTOR_ROUTING_JSON` | Process bootstrap routing policy; durable active revisions supersede it when newer |
| `SOURCE_CONNECTOR_REQUIRE_SIGNED_ARTIFACTS` | Require an attestation for every connector candidate; production forces this on |
| `SOURCE_CONNECTOR_TRUSTED_SIGNERS_JSON` | JSON map of signer key ID to base64 raw Ed25519 public key |
| `SOURCE_CONNECTOR_ALLOWED_BUILDERS` | Comma-separated artifact builder allowlist |
| `CONNECTOR_CALLBACK_BASE_URL` | Base URL used for host-allocated callbacks |
| `CONNECTOR_LIFECYCLE_INTERVAL_SECONDS` | Continuous lifecycle idle interval |

Never place private signing keys or provider tokens in routing configuration.

## Health interpretation

The connector health snapshot includes registry status and fingerprint, active
routing revision, lifecycle phase counts, persisted artifact statuses, and the
process-local runtime quarantine map. Overall status is degraded when an
installation is failed/degraded or an artifact is quarantined.

Startup diagnostics separately report registry size/fingerprint and artifact
admission counts. A quarantined connector always resolves to legacy mode even
if fleet rollout later applies a connector override.

Stateless owners that cannot consume durable authority and artifact admission
must remain legacy in signed-production mode. Do not disable signed-artifact
requirements to promote them; provision an audited distribution path first.

## Common incidents

### Artifact quarantined

Inspect the reason: missing attestation, disabled/quarantined status, identity or
version mismatch, missing/mismatched running-artifact measurement, manifest
digest mismatch, missing/mismatched conformance, builder rejection, unknown
signer, or invalid signature. Keep legacy routing.
Build and sign a new artifact; do not edit digests to match an existing binary.
Enable it through the artifact release process, then restart or refresh
admission.

### Lifecycle failed or degraded

Read sanitized conditions and connector telemetry. Validate authority
generation, current credential slots, provider access, outbound-host grants,
and remote health. Correct the cause and allow reconciliation to retry. Do not
manually mark an unhealthy installation ready.

### Parity mismatch

Stop promotion. Compare canonical identity, cursor, publication, normalization,
and state projections. Route the affected connector or cohort to legacy. Repair
native behavior before collecting a new clean evidence window.

### Elevated failures, latency, or DLQ delta

The rollout controller automatically creates a legacy revision when configured
thresholds are breached. Verify the audit record, preserve raw objects and
checkpoints, and investigate provider throttling, retry classification, host
capacity, and connector regressions.

### Credential rotation failure

Keep the existing current credential. Candidate credentials remain pending or
become rejected until verified. Check requested/granted scopes and secret-store
access. Never copy raw token values into tickets or logs.

## Emergency rollback

Activate a newer routing revision whose global mode is `legacy`, or use the
runtime configuration rollback where durable rollout control is unavailable.
Verify every process has propagated the revision through rollout audit records.
Artifact quarantine is an additional fail-closed control, not a replacement for
an audited fleet rollback.

After rollback, verify S3/Kafka publication and checkpoint continuity. Do not
rewind checkpoints unless the source-specific recovery procedure requires it.

## Uninstall

Set desired state to `Removed`. The lifecycle controller binds the cleanup
facet, retries it idempotently, retires credentials, revokes durable authority,
and records `Removed`. Do not delete the installation row to force completion.

## Audit queries

Operators should inspect these control-plane relations through approved admin
tools: `source_connector_installations`, `source_connector_authority_grants`,
`source_connector_credentials`, `source_connector_artifacts`,
`source_connector_routing_revisions`, `source_connector_rollout_audit`, and
`source_connector_rollout_events`. Tenant-scoped tables enforce fail-closed
RLS; use the normal tenant context rather than bypassing it for routine
diagnostics.
