"""services/ingest/integrations/notion/ — Notion integration package (IN-14).

Notion is the fifth ingestion source. Unlike slack/github/discord this
package ships NO webhook ingress (Notion has no reliable content push):
the live path is a periodic re-run of the backfill fetcher under
ingress_kind="poll" (see services/ingest/ingestion/fetchers/notion.py).

Modules:
  - oauth.py   : GET /integrations/notion/install + /callback (state-token
                 + nonce-consume + UPSERT provider_installations + audit).
  - client.py  : async Notion REST client (search / database query / block
                 children / comments) with 429 Retry-After handling.
  - metrics.py : notion_install_* + notion_fetch_* counters.
"""
