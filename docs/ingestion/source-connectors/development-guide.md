# Source connector development guide

This is the repository implementation path for adding a first-party source.
The normative boundary is the
[Source Connector Contract](../../architecture/source-connector-contract.md).

## Connector shape

A connector consists of:

- one canonical source entry and stable `fyralis/<source>` ID;
- one side-effect-free stable-v1 manifest;
- one explicit zero-argument factory returning a `SourceConnector` root;
- only the capability facets the source supports;
- installation-scoped binding through `BindingContext`;
- structural and behavioral release evidence.

Provider code owns authentication mechanics, API/resource semantics, cursor
meaning, webhook verification, identity and normalization. Fyralis owns tenant
selection, secrets at rest, governed egress, scheduling, leases, S3/Kafka
durability, acknowledgement, checkpoint commits, telemetry and lifecycle.

## Add a source: Stripe example

1. Add `stripe` to
   `services/ingest/source_contract/source-index.json`. Do not create another
   allowlist or dispatch registry.
2. Add `services/ingest/connectors/manifests/stripe.json` with ID
   `fyralis/stripe`, connector version, implementation factory, artifact
   modules, exact capabilities, ingress kinds, secret slots, allowed hosts,
   scopes, trust ceiling and runtime profile.
3. Add an explicit factory, for example
   `services.ingest.connectors.stripe:build_stripe_connector`. It may use shared
   contract-native authoring utilities, but the factory remains source named and
   deterministic.
4. Implement installation facets. Stripe would normally expose OAuth2 and OAuth
   lifecycle, configuration/secret rotation where manual credentials are
   supported, health and cleanup.
5. Implement ingestion facets that are actually supported: historical pull,
   incremental poll, webhook, reconciliation, identity and normalization.
   Unsupported behavior is absent, never a no-op stub.
6. Declare `oauth_access_token`, `oauth_refresh_token`, and
   `webhook_signing_secret` only if the implementation uses them. OAuth
   completion returns `SecretCandidate` values; connector code never persists
   or logs the token text.
7. Route provider calls through `GovernedHttpPort`; declare `api.stripe.com` and
   any OAuth/revocation host in the manifest. Do not instantiate an unrestricted
   HTTP client.
8. Verify Stripe webhook signatures over the bounded raw body inside the
   webhook capability. The platform will allocate an installation-scoped
   callback URL and durably emit verified records.
9. Define stable native identity inputs and normalize to contract observation
   drafts. Do not write domain tables from the connector.
10. Add facet unit tests plus structural/behavioral conformance. Cover OAuth
    state and scope handling, webhook tampering/replay, pagination, cursor
    monotonicity, identity stability, normalization, failure classification,
    cleanup and lifecycle binding.
11. Generate the structural fingerprint, review the behavioral result, and add
    the approved evidence to `release-evidence.json`.
12. Add the connector to the appropriate generic process topology only when its
    capabilities introduce a new execution archetype. A normal REST/OAuth,
    webhook, poll, Google-style subscription, or gateway connector requires no
    new source dispatch.

## Manifest example

```json
{
  "apiVersion": "sources.fyralis.io/v1",
  "kind": "SourceConnector",
  "metadata": {
    "id": "fyralis/stripe",
    "source": "stripe",
    "displayName": "Stripe",
    "version": "1.0.0",
    "owner": "ingestion"
  },
  "spec": {
    "contract": ">=1.0,<2.0",
    "implementation": "services.ingest.connectors.stripe:build_stripe_connector",
    "artifactModules": ["services.ingest.connectors.stripe"],
    "maturity": "stable",
    "capabilities": [
      {"id": "installation.oauth2", "version": 1, "required": true},
      {"id": "installation.oauth2_lifecycle", "version": 1, "required": true},
      {"id": "ingestion.webhook", "version": 1, "required": true,
       "configuredBy": ["webhook_signing_secret"]},
      {"id": "semantic.identity", "version": 1, "required": true},
      {"id": "semantic.normalization", "version": 1, "required": true},
      {"id": "health.probe", "version": 1, "required": true,
       "configuredBy": ["oauth_access_token", "webhook_signing_secret"]},
      {"id": "lifecycle.cleanup", "version": 1, "required": true}
    ],
    "ingressKinds": ["webhook"],
    "permissions": {
      "secretSlots": [
        "oauth_access_token",
        "oauth_refresh_token",
        "webhook_signing_secret"
      ],
      "outboundHosts": ["api.stripe.com", "connect.stripe.com"],
      "requestedScopes": []
    },
    "trust": {"maximumTier": "authoritative"},
    "runtime": {"isolation": "in_process_trusted", "resourceClass": "io_standard"}
  }
}
```

The exact capabilities, scopes and hosts must follow the provider contract; the
example is a shape, not an authorization recommendation.

## Install behavior

Use the common endpoints only:

```text
GET  /integrations/stripe/install
GET  /integrations/stripe/callback
POST /integrations/stripe/configure
POST /webhooks/stripe/callback/{endpoint_id}
```

The configuration payload accepts `external_installation_id`, `credentials`,
`configuration`, and manifest-declared `installation_data`. The platform
persists the common installation, authority, credential references, callback,
and onboarding trigger transactionally.

## Required verification

```bash
.venv/bin/python -m pytest -q \
  services/ingest/source_contract/tests \
  services/ingest/connector_conformance/tests \
  services/ingest/connector_runtime/tests \
  services/ingest/connector_platform/tests \
  services/ingest/connectors/tests

.venv/bin/python scripts/check_source_connector_release_gate.py
.venv/bin/python scripts/check_source_lifecycle_contract.py
```

## Review checklist

- Source index, manifest and factory identity agree.
- The manifest requests least authority and only bare allowed DNS hosts.
- Declared capabilities have implementations and correct `configuredBy` slots.
- No credential value appears in state, logs, metrics or errors.
- Provider I/O uses governed ports and typed failures.
- Raw durability and checkpoint ordering stay host-owned.
- Installation generation and authority fences are preserved.
- Conformance evidence is independently reproducible.
- Adding the connector did not add a source-keyed runtime dispatch map.
