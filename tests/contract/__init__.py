"""Real-provider contract tests (Phase 2 — Real-World Integrations).

DISTINCT from `services/ingest/synthetic/` (whose mock clients mirror our own
code and therefore cannot reveal real-provider drift). A *contract fixture* is a
physical `.json` file capturing a REAL provider payload — a webhook delivery
(headers + body), an API response, or an OAuth token response — recorded from
official provider documentation or a sanitized production capture. Contract
tests assert that our verifiers / tenant-resolvers / handlers / fetchers parse
these EXACT shapes.

See README.md for the fixture file schema and capture/sanitize rules.
"""
