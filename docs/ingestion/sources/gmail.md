# Gmail

> Email as a signal, via Google **Pub/Sub push** for live + the **History API**
> for backfill. The only push-based source; uses Google Workspace
> Domain-Wide-Delegation (DWD), not per-user OAuth bot tokens.

| Field | Value |
|---|---|
| Source | `gmail` |
| Primary channel | `gmail:` |
| Trust tier | `attested_agent` |
| Live ingress | Google **Pub/Sub push** → full pipeline (`ingress_kind="pubsub"`) |
| Backfill / poll | History API (`ingress_kind="poll"` / `"backfill"`) |
| Auth | Domain-Wide Delegation — service account mints per-user tokens |
| Signature | Google OIDC token on the push request |

## Auth (DWD substrate)

Gmail established the Google Workspace DWD auth substrate that Calendar and Drive
reuse: [services/integrations/gmail/dwd.py](../../../services/integrations/gmail/dwd.py)
(`get_minter()`), the shared `GoogleHttpClient`, `DirectoryClient`, and
`resolve_inclusion`. A single service account is granted the Gmail scope and
mints per-user access tokens. OAuth/admin-consent flow in
[gmail/oauth.py](../../../services/integrations/gmail/oauth.py).

## Ingress (live) — Pub/Sub push

Google Pub/Sub delivers a **notification** (not the message body) to the
dedicated endpoint
[services/webhooks/gmail_pubsub.py](../../../services/webhooks/gmail_pubsub.py)
— **not** the generic `/webhooks/{provider}` router. The push request carries a
Google OIDC token ([signatures/google_oidc.py](../../../services/webhooks/signatures/google_oidc.py)).
The handler / history poller then **fetches the real Gmail message resource** and
publishes it to `ingestion.raw` via the canonical `app.state.kafka_producer` /
`s3_raw_client` (with `flush()`), channel `gmail:`.

> Note the `ingress_kind` nuance: the live-via-Kafka cutover publishes the fetched
> message under `ingress_kind="poll"` (a *real* Gmail message, not the Pub/Sub
> notification), which maps to the same `gmail:` handler as backfill so
> `external_id` parity holds. The raw push notification ingress is `"pubsub"`.

Supporting machinery under
[services/integrations/gmail/](../../../services/integrations/gmail/):
`watch.py` / `watch_scheduler.py` (renew Gmail watches), `pubsub.py`,
`push_handler.py`, `history_poller.py`, `fetcher.py`, `threading.py`,
`directory.py`, `optout.py`, `audit.py`, `status_api.py`, `uninstall.py`.

## Backfill / poll

[planners/gmail.py](../../../services/ingestion/planners/gmail.py) +
[fetchers/gmail.py](../../../services/ingestion/fetchers/gmail.py) pull via the
History API → `RawEnvelope` → channel `gmail:`. The reconciler re-runs the
fetcher to close gaps.

## Handler & dedup

[handlers/gmail.py](../../../services/ingestion/handlers/gmail.py) → `gmail:`,
`trust_tier=attested_agent`. Dedup via the observations unique index;
message-id-based `external_id` is stable across pubsub/poll/backfill.

See [architecture.md](../architecture.md).
