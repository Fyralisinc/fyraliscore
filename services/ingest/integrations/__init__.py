"""services.ingest.integrations — third-party integration endpoints.

This package owns user-facing OAuth install/uninstall flows for each
provider Fyralis integrates with (Slack first; GitHub, Linear, Stripe,
Discord later under the same shape). The webhook ingress (under
services.app.webhooks) remains the inbound event surface; this package
adds the *outbound* admin and management surface.

Mounted at `/integrations/*` by services.app.gateway.main.build_app via
services.ingest.integrations.router.build_integrations_router().
"""
