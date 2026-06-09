# Real-provider contract tests

This layer closes the **synthetic-fidelity gap**: `services/ingest/synthetic/`
mocks mirror our own code, so they cannot reveal real-provider payload drift
(camelCase vs snake_case, REST vs GraphQL, pagination shape, signature scheme).
Contract tests assert our verifiers / tenant-resolvers / handlers / fetchers
parse **real** provider payloads, recorded as physical JSON fixtures.

## Layout

```
tests/contract/
  framework.py     # strict fixture loader/validator
  registry.py      # the coverage checklist (every contract Phase 2 must verify)
  test_contract_framework.py   # meta-tests + the live AWAITING-FIXTURE checklist
  fixtures/<provider>/<kind>/<name>.json
```

`kind` ∈ `webhook` | `api_response` | `oauth_token`.

## See what's still needed

```bash
pytest -m contract -rs
```

Each `AWAITING FIXTURE` skip line names the exact fixture path, the Phase-1
finding it unblocks, what our code assumes today, and what the fixture must
confirm.

## Fixture file schema

Webhook delivery:

```json
{
  "_meta": {
    "provider": "gusto",
    "kind": "webhook",
    "description": "company.updated webhook delivery",
    "source": "doc:https://docs.gusto.com/...  |  capture:redacted prod delivery",
    "captured_at": "2026-06-09",
    "sanitized": true
  },
  "request": {
    "url": "https://hooks.example.com/webhooks/gusto/inst_123",
    "headers": { "X-Gusto-Signature": "<scrubbed>", "Content-Type": "application/json" },
    "body": { "...": "the EXACT real JSON body" }
  }
}
```

API / OAuth response:

```json
{
  "_meta": { "provider": "aws", "kind": "api_response", "description": "...",
             "source": "...", "captured_at": "2026-06-09", "sanitized": true },
  "response": { "status": 200, "body": { "...": "the EXACT real response" } }
}
```

## Rules

1. **`sanitized: true` is mandatory and attested.** Scrub tokens, signing
   secrets, real names/emails/account numbers *before* committing. Keep the
   **structure, key names, and casing** exactly as the provider sends them —
   that is the whole point. Replace secret *values*, never rename keys.
2. **Keep header names and casing verbatim** for webhook fixtures — signature
   and routing bugs hide in header casing.
3. One fixture = one real shape. Add the matching contract test alongside the
   integration fix that consumes it.
4. Prefer an official-doc capture when a sanitized production capture isn't
   available; cite it in `_meta.source`.
