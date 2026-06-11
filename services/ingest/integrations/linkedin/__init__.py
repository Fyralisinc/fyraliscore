"""LinkedIn integration (organization/people source).

LinkedIn's Community Management API is authenticated with OAuth 2.0 (Bearer
access token, ~60 days; partner-issued refresh token). Every call is scoped to
an ``organization_urn`` (the scope-id, analogous to Carta's ``firm_id`` /
Gusto's ``company_uuid``). The read surface is the Rest.li finders under
``https://api.linkedin.com/rest``: the org posts finder
(``/posts?q=author``), ``/organizationalEntityShareStatistics`` and
``/organizationalEntityFollowerStatistics`` (both ``q=organizationalEntity``),
and the ``/organizations/{id}`` probe. Every call carries the two REQUIRED
headers ``LinkedIn-Version: YYYYMM`` + ``X-Restli-Protocol-Version: 2.0.0``;
all wire timestamps are epoch-millis integers. Scopes:
``r_organization_social`` (posts) + ``rw_organization_admin`` (statistics /
org lookup).

LinkedIn is POLL-ONLY: there is NO webhook. The live edge re-lists changed
objects on an interval and dispatches each change directly through the ingestion
pipeline (`services/ingest/integrations/linkedin/poll.py`). The ingestion source
key is ``linkedin`` and the single channel is ``linkedin:object``.

TODO(human): ACCESS IS PARTNER-GATED — Community Management API tiers are
    approval-only, and programmatic refresh tokens are only issued to approved
    programs. Confirm the tier/entitlement (and wire the token refresh seam in
    `client.py`) before real traffic.
"""
