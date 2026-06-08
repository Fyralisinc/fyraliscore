"""Carta integration (cap-table source).

Carta is a cap-table / equity-management API authenticated with OAuth 2.0
(Bearer access token, short-lived; rotating refresh token). Every call is scoped
to a firm ``firm_id`` (the scope-id, analogous to Gusto's ``company_uuid`` /
QuickBooks' ``realmId``). It exposes cap-table entities (shareholders, share
classes, SAFEs, option grants, …).

Carta is POLL-ONLY: there is NO webhook. The live edge re-lists changed
cap-table objects on an interval and dispatches each change directly through the
ingestion pipeline (`services/ingest/integrations/carta/poll.py`). The ingestion
source key is ``carta`` and the single channel is ``carta:object``.

This package clones the Gusto OAuth2 archetype (itself a QuickBooks clone); the
read surface, pagination, and OAuth refresh are flagged `TODO(human): confirm ...`
where the real Carta behavior is unverified.
"""
