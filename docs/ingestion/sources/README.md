# Source-specific notes

The files in this directory preserve provider research, payload semantics and
operational history. References inside them to source-local integrations,
planner/fetcher/reconciler registries, dedicated installation tables, shadow
publishing, circuit-breaker routing, or source-specific workers describe the
pre-contract implementation and are not current runtime instructions.

Current implementation authority is:

- source inventory: `services/ingest/source_contract/source-index.json`;
- capabilities and permissions: `services/ingest/connectors/manifests/*.json`;
- provider code: `services/ingest/connectors/`;
- installation/runtime wiring: `services/ingest/connector_platform/`;
- operational instructions: `docs/ingestion/source-connectors/`.

When updating one of these provider notes, retain useful provider protocol and
payload details but replace pre-contract file paths and lifecycle steps with the
manifest/factory/common-installation path.
