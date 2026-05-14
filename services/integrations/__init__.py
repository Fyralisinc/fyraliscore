"""services.integrations — third-party integration endpoints.

This package owns user-facing OAuth install/uninstall flows for each
provider Fyralis integrates with (Slack first; GitHub, Linear, Stripe,
Discord later under the same shape). The webhook ingress (under
services.webhooks) remains the inbound event surface; this package
adds the *outbound* admin and management surface.

Mounted at `/integrations/*` by services.gateway.main.build_app via
services.integrations.router.build_integrations_router().
"""
