"""Jira Cloud integration (IN-17).

Jira is the 7th ingestion source. It authenticates with HTTP Basic
(account_email:api_token) against a per-tenant Jira Cloud site
(`https://<site>.atlassian.net/rest/api/3/...`). See
`specs/IN-17-jira-integration/plan.md` for the design decisions.
"""
