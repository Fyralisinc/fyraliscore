"""Grafana ingestion integration (IN-GRAFANA).

Grafana is a token-auth REST source on the Jira/Mercury dual-edge shape:

  - client.py      — outbound Grafana HTTP API client (service-account Bearer).
  - onboarding.py  — finalize_install (grafana_installations + onboarding
                     trigger) + register_webhook_installation (provider_installations).

Backfill walks the org's annotations (GET /api/annotations); the live edge is a
Grafana Alerting webhook contact point (HMAC-signed) handled by
services/app/webhooks/signatures/grafana.py + the `grafana:alert` handler.
"""
