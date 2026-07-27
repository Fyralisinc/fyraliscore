# Miro ingestion: contract-owned polling

Miro is a poll-only source in Fyralis. Initial backfill and scheduled
reconciliation both read Miro's REST API; there is no production webhook route,
signature verifier, webhook tenant resolver, or webhook installation row.
Miro's experimental webhooks were
[discontinued on 2025-12-05](https://developers.miro.com/changelog/removed-experimental-webhooks-support).

This document follows the production path from connection through creation of a
`miro:item` `ObservationDraft`.

## Contract

The canonical source and provider definitions declare:

- ingress kinds: `backfill` and `poll`;
- live transport: `api_poll`;
- normalization channel: `miro:item`;
- allowed observation kind: `signal`;
- installation table: `miro_installations`;
- exact provider scope: `org_id`;
- child resources: `miro_boards`;
- secret references used by runtime: `secret_ref` only;
- no webhook ingress path and no `provider_installations` binding.

The authoritative declaration is
[`source_contract/catalog.py`](../../../services/ingest/source_contract/catalog.py).
Startup validation resolves every callable owned by that contract before a
runtime accepts work.

## Connection and exact installation binding

[`integrations/miro/oauth.py`](../../../services/ingest/integrations/miro/oauth.py)
provides the admin connection routes:

1. `POST /integrations/miro/connect/preflight` accepts an API token and optional
   base URL. It calls `MiroClient.list_boards()` and returns the visible boards.
2. `POST /integrations/miro/connect/finalize` re-verifies the token before any
   write, applies the optional board allowlist, stores the token in the encrypted
   secret store, and calls `finalize_install()`.

The finalize route accepts `api_token`, `base_url`, `board_ids`, and optional
`org_id`. It explicitly rejects `webhook_secret`; Miro has no supported push
binding to attach that secret to. Its response contains the installation ID and
board count, not `webhook_registered`.

[`integrations/miro/onboarding.py`](../../../services/ingest/integrations/miro/onboarding.py)
then performs one tenant-scoped transaction:

- upsert `miro_installations` by the exact normalized `org_id` when known;
- use `(tenant_id, base_url)` only for unresolved legacy installs with no org;
- upsert each selected board in `miro_boards`;
- enqueue one idempotent `onboarding_triggers` row for source `miro`.

Two Miro organizations for the same tenant can therefore share the canonical
API URL without collapsing into one installation. Reconnecting the same org
updates that same row even if its API endpoint changes.

The schema still has a nullable `miro_installations.webhook_secret_ref` column
for migration compatibility. Production onboarding does not accept, write, or
read it. It may be removed in a later schema cleanup after old deployments have
been audited.

## Data flow

```text
Miro REST API
    │
    ├─ initial onboarding trigger ──> planner ──> board shards
    │
    └─ reconciliation cadence ──────> planner ──> board shards
                                                │
                                                v
                                      fetch_page_miro()
                                                │
                                  fetcher-tagged item records
                                                │
                                                v
                                      handle_miro_item()
                                                │
                                      miro:item / signal
                                                │
                                                v
                                      core ingest + persistence
```

[`planners/miro.py`](../../../services/ingest/ingestion/planners/miro.py)
creates one `miro_board_items` shard per selected board. The exact installation
ID and org scope travel with each shard.

[`fetchers/miro.py`](../../../services/ingest/ingestion/fetchers/miro.py)
loads the exact installation and pages `GET /boards/{board_id}/items`. Every
record emitted to normalization contains:

```python
{
    "_fyralis_record_type": "item",
    "_fyralis_org_id": "<exact org scope>",
    "_fyralis_board_id": "<board id>",
    "item": { ... },
}
```

[`handlers/miro.py`](../../../services/ingest/ingestion/handlers/miro.py)
fails closed unless that fetcher tag is present. Raw webhook-shaped or untagged
item payloads are rejected. A valid item always becomes a `signal`; the retired
webhook-only deleted/state-change branch no longer exists.

## Refresh and cursor semantics

Initial backfill walks all configured boards. Reconciliation schedules the same
fetcher with the persisted per-board cursor/high-water state. Polling may
re-read an overlap window so that boundary updates are not missed.

The item external ID is versioned:

```text
miro:{org_id}:item:{item_id}:{version}
```

The org scope prevents collisions between installations. Re-reading the same
item version deduplicates; a changed version produces a new observation.

The fetcher must not advance past required missing data. Provider throttling and
retry timing run through the source's declared request policy and
`ProviderTransport`; a deferred request exits the current tick instead of
spinning.

## Local verification

The focused proof surfaces are:

- handler tests:
  [`handlers/tests/test_miro.py`](../../../services/ingest/ingestion/handlers/tests/test_miro.py);
- connect write-order and poll-only persistence tests:
  [`integrations/tests/test_oauth_admin_paste_write_order.py`](../../../services/ingest/integrations/tests/test_oauth_admin_paste_write_order.py);
- exact multi-install binding:
  [`integrations/tests/test_exact_scope_onboarding.py`](../../../services/ingest/integrations/tests/test_exact_scope_onboarding.py);
- exact poll lookup:
  [`integrations/miro/tests/test_poll.py`](../../../services/ingest/integrations/miro/tests/test_poll.py);
- contract assertions:
  [`source_contract/tests/test_catalog.py`](../../../services/ingest/source_contract/tests/test_catalog.py);
- local end-to-end pull path:
  [`scripts/sandbox_miro.py`](../../../scripts/sandbox_miro.py).

The Provider Lab and certification harness seed only `miro_installations`,
`miro_boards`, and the onboarding trigger. They do not create a synthetic Miro
`provider_installations` row.
