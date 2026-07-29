# Contract-Driven Source Binding — R0 Legacy Selector Inventory

## Scope

This is the frozen path-level inventory required by the Terra handoff step
"Reproduce and inventory." It was captured from the isolated implementation
worktree rooted at
`9a55c1d44e4fe84c550eb61a1df56a8d91e39d97` plus the R0 deterministic-surface
repair. It does not include, modify, or classify the user's unrelated dirty
worktree paths.

## Audit result

The strict source-architecture ratchet reports zero active legacy findings:

```text
PYTHONPATH=. .venv/bin/python scripts/check_source_architecture_ratchet.py --no-baseline
Source architecture strict/no-baseline passed: 0 baselined, 0 resolved.
```

Therefore there is no currently-detected mutable source dispatcher, import-side
registration hook, duplicate canonical-source literal, source-selected client
switch, arbitrary-installation selector, raw provider-HTTP bypass, fabricated
live binding, or retired mock/spammer surface to delete at R0. P9 must rerun
this strict check from its final deletion commit; a passing R0 scan is not a
substitute for that release gate.

## Source-binding surfaces retained by design

| Path / surface | Classification | Contract authority | R9 action |
| --- | --- | --- | --- |
| `services/ingest/source_contract/catalog.py` / `SOURCE_DEFINITIONS` and `PROVIDER_DEFINITIONS` | Canonical immutable catalog | `SourceDefinition` / `ProviderDefinition` | Retain. This is the only source metadata authority. |
| `services/ingest/source_contract/catalog.py` / derived OAuth and live-ingress indexes | Immutable projections, not independent registries | Catalog-derived ingress metadata | Retain only as derived projections; do not add handwritten entries. |
| `services/ingest/source_contract/runtime.py` | Lazy callable resolver | Catalog callable-binding fields | Retain. It resolves provider algorithms without import-side registration. |
| `services/ingest/ingestion/planners/`, `fetchers/`, and `reconcilers/` modules | Provider-specific algorithms | `planner_binding`, `fetcher_binding`, and `reconciler_binding` | Retain algorithms. Remove only a selector if a future scan proves one exists. |
| `services/ingest/ingestion/handlers/` modules | Provider-specific normalizers | `normalizer_bindings` and channel-level catalog bindings | Retain algorithms. No decorator/import registry is present. |
| `services/ingest/ingestion/raw_tier/s3.py` and `ingestion/dlq/publish.py` source validation sets | Type/catalog-derived validation | `SourceLiteral` / generated `INGESTION_SOURCES` | Retain only while generated from canonical source identity; never hand-maintain a source list here. |
| `services/ingest/ingestion/workflows/source_onboarding.py`, `tenant_onboarding.py`, and `reconcilers/__init__.py` | Catalog-derived workflow filtering and runtime preparation | `SOURCE_DEFINITIONS`, exact installation IDs | Retain. Verify during R2/R3 that all exact-binding paths remain contract-owned. |
| `services/ingest/synthetic/provider_lab/` | Canonical local simulator | Generated certification surfaces and Provider Lab protocol | Retain and extend in R1–R7. It replaces the prohibited standalone mock/spammer model. |
| `services/ingest/source_certification/` | Certification catalog, generated surfaces, executable bindings, evidence schema | Source-certification catalog and generated artifacts | Extend in R1–R8. Its execution driver must become contract-driven, not become a second source registry. |

## Deletion inventory status

| Legacy category mandated by the handoff | R0 status | Final proof required |
| --- | --- | --- |
| Mutable planner/fetcher/reconciler maps | No active selector found | Strict ratchet on final SHA. |
| Handler decorator/import registration | No active selector found | Strict ratchet on final SHA. |
| Parallel route/verifier/secret/Kafka maps | No active selector found; catalog-derived projections remain | Contract-only integration and route parity tests. |
| Tenant-only/latest installation selection | No active selector found | Exact-installation integration tests plus final ratchet. |
| Raw provider HTTP bypass | No active selector found | Static transport enforcement and Provider Lab request-ledger tests. |
| Standalone mocks/spammer/compatibility harness | No active selector found | Provider Lab conformance and final strict ratchet. |
| Manual source fixture/deployment lists | Must be re-audited after R1–R8 generation work | Generator checks and final strict ratchet. |

## Update rule

Each implementation milestone must update this inventory if it creates,
migrates, or deletes a source-selection surface. R9 must replace the R0 status
with exact removed paths and final proof; no compatibility facade may be
declared removed merely because it is no longer exercised by a test.
