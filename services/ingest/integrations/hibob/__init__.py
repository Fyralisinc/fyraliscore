"""HiBob integration (People / HR source).

HiBob ("Bob") is a People/HR platform. Its public API is authenticated with a
**service user**: an id + token pair presented as HTTP **Basic** auth
(``base64(service_user_id:token)``) — NOT OAuth, so there is no token refresh and
no ``oauth.py`` (this clones Brex's long-lived-token posture, but Basic instead
of Bearer). Every call is scoped to a HiBob account; the scope-id we shard /
resolve on is the ``company_id``. It exposes People/HR entities (employees,
lifecycle changes, time-off, payroll) plus HMAC-signed webhooks for the live
edge. The ingestion source key is ``hibob`` and the single channel is
``hibob:object``.

This package clones the Gusto vertical STRUCTURE (entity-model:
``hibob_installations`` + ``hibob_entities``; the planner emits ONE shard per
entity type) but swaps the auth onto Brex's secret-resolved-token client. The
live edge is an HMAC webhook (HMAC-SHA512, base64 digest, ``Bob-Signature``
header — see ``services/app/webhooks/signatures/hibob.py``).

Several external-API details are UNVERIFIED (exact read endpoints/paths, the
concurrent rate-limit numbers, and the real production webhook tenant-resolution
which is by endpoint/secret rather than a body field); see the
``TODO(human): confirm …`` markers in ``client.py``,
``../../ingestion/fetchers/hibob.py``, and
``../../../app/webhooks/signatures/hibob.py``.
"""
