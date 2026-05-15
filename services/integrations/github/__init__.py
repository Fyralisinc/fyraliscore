"""services/integrations/github/ — IN-13 GitHub App integration.

Mirrors the IN-08 Slack and IN-09 Discord integration packages with one
GitHub-specific shape: a **single App-level webhook secret** (set in
the App's developer settings, loaded via `WEBHOOK_SECRET_GITHUB` env or
the secret store), NOT per-installation secrets. Per-tenant isolation
is achieved structurally via `installation.id` payload routing and the
existing `services/webhooks/tenant_resolver.py::_extract_github`.

Public surface (consumed by `services/integrations/router.py` and
`services/webhooks/router.py`):

- oauth.install_handler / oauth.callback_handler
- client.GithubClient
- jwt.mint_app_jwt
- lifecycle.dispatch_installation_event / dispatch_installation_repositories_event
- uninstall._disable_installation_github (private; called by lifecycle + client)
- replay_cache.ReplayCache + make_replay_cache
- metrics.* (counter helpers, low-cardinality labels only)

Constitution alignment: §I (Observations only, no new Foundation),
§III (`provider_installations.selected_repositories` inherits RLS from
migration 0039), §VII (uuid7 IDs, audit-log every state transition),
§X (no premature abstraction — copies the IN-09 module shape).
"""
