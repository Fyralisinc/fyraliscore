"""Ashby integration (recruiting ATS source).

Ashby is an applicant-tracking-system (ATS) API authenticated with an **API
key** presented as HTTP Basic: the key is the username and the password is
empty (``base64("KEY:")``). Every call is scoped to an organization ``org_id``.
It exposes recruiting entities (candidates, applications, jobs, interviews,
offers) and organization-level recruiting intelligence objects (users, openings,
job postings, feedback, approvals, sources, surveys, interview plans/schedules,
etc.) over an RPC-style surface — ``POST /<Category>.list`` /
``POST /<Category>.info`` — with cursor pagination (response ``nextCursor`` /
request param ``cursor`` / ``moreDataAvailable`` bool) and an incremental
``syncToken`` for delta polls where the endpoint supports it. Live changes
arrive as HMAC-signed webhooks (HMAC-SHA256, hex digest,
``Ashby-Signature: sha256=<hex>``, verified over the RAW unparsed body — see
``services/app/webhooks/signatures/ashby.py``).

The ingestion source key is ``ashby`` and the single channel is ``ashby:object``.

This package clones the Gusto entity-model vertical STRUCTURE (one shard per
entity_type) but swaps in Brex/Jira-style API-key Basic auth. The auth scheme,
RPC list/info verbs, cursor pagination, and syncToken incremental are CONFIRMED
from Ashby's first-party docs. Where a production specific is genuinely
unverified (concurrent rate-limit numbers; the real webhook tenant-resolution,
which in prod is by endpoint/secret rather than a body field) a precise
``TODO(human): …`` marker is left.
"""
