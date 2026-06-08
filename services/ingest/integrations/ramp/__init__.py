"""Ramp integration (finance source).

Cloned from the QuickBooks OAuth archetype. Ramp is a spend/card-management API
authenticated with OAuth 2.0 (Bearer access token + a rotating refresh token).
Every call is scoped to a company ``business_id``. The ingestion source key is
``ramp`` and the single channel is ``ramp:transaction``.

Several external-API specifics are UNVERIFIED (host, read endpoints + OAuth
scopes, pagination scheme, the incremental "updated since" filter, the webhook
signature scheme, and OAuth token refresh). Each is kept configurable behind the
archetype default with a visible ``TODO(human): confirm ...`` marker in the
relevant module (client.py / oauth.py / fetcher / signatures / endpoints).
"""
