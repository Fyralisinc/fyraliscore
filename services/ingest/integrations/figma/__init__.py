"""Figma integration (design-file source).

Figma is a collaborative design tool exposing a REST/JSON API (v1) for files,
named versions, and comment threads, plus Webhooks V2 for change-data-capture.
It is authenticated with a long-lived org/team access token presented as an HTTP
**Bearer** token (or OAuth2 per-resource — out of v1 scope here). The ingestion
source key is ``figma`` and the single channel is ``figma:event``.

This package clones the Brex Bearer-token archetype (which itself clones
Mercury). Several external-API details are UNVERIFIED (pagination scheme, exact
read endpoints, the webhook auth scheme); see the ``TODO(human): confirm …``
markers in ``client.py``, ``../../ingestion/fetchers/figma.py``, and
``../../../app/webhooks/signatures/figma.py``.

WEBHOOK DIVERGENCE: real Figma webhooks (V2) authenticate the callback via a
shared **PASSCODE carried in the request BODY**, NOT an HMAC signature header.
For the synthetic gate (which drives every HMAC provider through the shared
``HmacWebhookGenerator``), ``signatures/figma.py`` is implemented as an
HMAC-SHA256 HEADER verifier — see the prominent TODO there to reconcile against
the real passcode-in-body scheme before production.
"""
