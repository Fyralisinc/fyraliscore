"""LinkedIn integration (people/recruiting source).

LinkedIn is a people/recruiting API authenticated with OAuth 2.0 (Bearer access
token, short-lived; rotating refresh token). Every call is scoped to an
``organization_urn`` (the scope-id, analogous to Carta's ``firm_id`` / Gusto's
``company_uuid``). It exposes organization-facing recruiting/marketing entities
(shares/posts, social actions, follower statistics).

LinkedIn is POLL-ONLY: there is NO webhook. The live edge re-lists changed
objects on an interval and dispatches each change directly through the ingestion
pipeline (`services/ingest/integrations/linkedin/poll.py`). The ingestion source
key is ``linkedin`` and the single channel is ``linkedin:object``.

This package clones the Carta OAuth2 archetype WHOLESALE (itself a Gusto/
QuickBooks clone); the read surface, pagination, and OAuth refresh are flagged
`TODO(human): ...` where the real LinkedIn behavior is unverified.

TODO(human): LinkedIn recruitment/organization data access is PARTNER-GATED —
    invite-only Marketing Developer Platform / Talent Solutions entitlement. The
    exact OAuth scopes (e.g. r_organization_social, rw_organization_admin,
    r_organization_followers) and the entity data shapes are NOT verified here;
    confirm against the approved partner agreement before real traffic.
"""
