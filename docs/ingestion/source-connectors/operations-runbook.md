# Source connector operations runbook

## Inventory

Run:

```bash
.venv/bin/python scripts/check_source_connector_release_gate.py
.venv/bin/python scripts/check_source_lifecycle_contract.py
```

The expected inventory is 26 stable-v1 native connectors and one common
lifecycle command surface.

## Installation status and control

```bash
.venv/bin/python scripts/manage_source_installations.py status \
  --tenant TENANT_UUID --operator-actor ACTOR_UUID --source slack

.venv/bin/python scripts/manage_source_installations.py pause \
  --tenant TENANT_UUID --operator-actor ACTOR_UUID \
  --installation-id INSTALLATION_UUID --reason "provider incident"
```

`resume`, `maintenance`, and `uninstall` use the same shape. Mutations are
generation fenced, auditable, and reconciled by the common lifecycle worker.

## Failure triage

1. Confirm the installation is `Ready` or `Degraded` and its observed generation
   has caught up with the desired generation.
2. Confirm an active authority grant matches tenant, connector and generation.
3. Confirm every capability's `configuredBy` slots have current credential
   references.
4. Confirm the artifact version is admitted and not quarantined.
5. Check the generic owner for the capability: poll worker, subscription
   scheduler, gateway worker, webhook edge or onboarding workflow.
6. Classify the typed failure: authentication, rate limit, transient provider,
   payload, state incompatibility, deadline/cancellation, binding, or admission.

Move an installation to `Maintenance` while repairing credentials or provider
configuration. Do not create a source-specific bypass.

## Webhooks and watches

Source webhooks must target
`/webhooks/{source}/callback/{endpoint_id}`. A bare source webhook route is
rejected. Confirm the callback is active, belongs to the installation and has
the expected purpose. Google Calendar/Drive callbacks also validate channel ID
and nonce. Gmail Pub/Sub validates the configured Google OIDC audience and
service-account email before resolving a common Gmail installation.

## Poll and gateway state

Poll cursors live in `source_connector_installation_data` under `poll.cursor`.
Gateway resume state uses `gateway.resume`; subscription state uses
`subscription.state`. Generation changes use compare-and-set. Never edit these
values while a worker owns the installation; pause first, repair with an audited
operation, then resume.

## Artifact incident

Quarantine the faulty artifact or activate a previously admitted revision. The
source becomes unavailable until a valid connector artifact is active. There is
no second source runtime. Preserve raw/checkpoint state and verify compatibility
before resuming.

## Removal

Uninstall sets desired state `Removed`. The lifecycle controller binds the
cleanup capability, revokes remote resources when supported, retires credential
references, revokes authority, and advances to observed `Removed`. Repeated
cleanup is idempotent.

## Required alerts

Monitor connector execution failures/duration, DLQ rate, artifact quarantine,
lifecycle failures, stalled generation reconciliation, callback authentication,
poll lag, gateway reconnects/lease heartbeats, S3/Kafka publication errors and
database pool health. Labels must remain bounded by connector, capability,
version and outcome.
