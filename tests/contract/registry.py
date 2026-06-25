"""Contract-coverage registry — the single checklist of every real-provider
contract Phase 2 must verify, each mapped to its Phase-1 finding and the EXACT
uncertainty a fixture must resolve.

Each entry's fixture lives at `fixtures/<provider>/<kind>/<fixture>.json`. Until
that file is provided, `test_contract_coverage` skips with `must_confirm`, so
`pytest -m contract -rs` prints a live list of outstanding fixtures. When a
fixture lands, its dedicated contract test (added alongside the integration fix)
asserts our code parses that exact shape.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContractNeed:
    provider: str
    kind: str  # webhook | api_response | oauth_token
    fixture: str  # fixture file stem under fixtures/<provider>/<kind>/
    finding: str  # Phase-1 report finding reference
    we_currently_read: str  # what our code assumes today
    must_confirm: str  # what the real fixture must let us verify


# Ordered by leverage: webhook tenant-resolution (breaks live ingestion wholesale)
# first, then API-response shapes, then idempotency/lifecycle, then OAuth refresh.
REGISTRY: list[ContractNeed] = [
    # --- A. Webhook tenant-resolution: casing & mechanism --------------------
    ContractNeed(
        "gusto", "webhook", "employee_event", "#17",
        "RESOLVED: real field is `resource_uuid` (always the company); signature "
        "is lowercase-hex HMAC-SHA256 in X-Gusto-Signature",
        "covered by tests/contract/test_gusto_contract.py against the doc-sourced "
        "employee_event fixture (verifier + resolver + handler)",
    ),
    ContractNeed(
        "ramp", "webhook", "transaction_event", "#35",
        "RESOLVED: real Ramp is a flat event with ROOT `business_id`; no "
        "eventNotifications wrapper. (HMAC encoding hex-vs-base64 still unconfirmed upstream.)",
        "covered by tests/contract/test_ramp_contract.py (resolver + verifier + handler)",
    ),
    ContractNeed(
        "quickbooks", "webhook", "entity_change_multi", "#7",
        "RESOLVED (R1): the ingress now fans a multi-realm/multi-entity "
        "eventNotifications delivery out into one ingest per (realmId, entity), "
        "each re-resolved to its realm's tenant; intuit-signature (app-level "
        "base64 HMAC) verifies the whole batch once",
        "covered by tests/contract/test_quickbooks_contract.py (fan-out split + "
        "per-realm resolution + app-level verifier + per-unit handler) against "
        "the doc-sourced 2-realm/3-entity fixture",
    ),
    ContractNeed(
        "hibob", "webhook", "event", "#20",
        "FALSE POSITIVE (resolved): real Bob V2 DOES carry top-level numeric "
        "`companyId` on every delivery; code reads it correctly",
        "covered by tests/contract/test_hibob_contract.py (resolver + Bob-Signature "
        "SHA512/base64 verifier) against the doc-sourced fixture",
    ),
    ContractNeed(
        "ashby", "webhook", "candidate_event", "#28",
        "RESOLVED (R3): real Ashby carries NO org id in the body — the tenant is "
        "resolved from the PER-INSTALL ENDPOINT URL (/webhooks/ashby/{installId}) "
        "threaded into TenantResolver.resolve(subpath=...); body `organizationId` "
        "is now only a legacy fallback. Ashby-Signature sha256=+hex verifier already correct",
        "covered by tests/contract/test_ashby_contract.py (no-org body + path-segment "
        "resolution end-to-end + Ashby-Signature verifier) against the doc-sourced fixture",
    ),
    ContractNeed(
        "figma", "webhook", "file_update", "#C",
        "RESOLVED (R2): real Figma V2 carries a Figma-assigned `webhook_id` (the "
        "install scope) and NO team_id, and no event id — resolver now reads "
        "webhook_id, install is keyed by it, handler discriminates by "
        "(file_key, timestamp); verification is passcode-in-body (no HMAC header)",
        "covered by tests/contract/test_figma_contract.py (webhook_id resolution "
        "+ passcode-in-body verifier + (file_key,timestamp) external_id) against "
        "the doc-sourced fixture",
    ),
    # --- B. API / SDK response shapes ---------------------------------------
    ContractNeed(
        "aws", "api_response", "cloudtrail_lookup_events", "#2",
        "RESOLVED (Phase 3, additive): fetcher/handler now read PascalCase "
        "Events[].* + EventTime as a datetime/ISO and json.loads the "
        "CloudTrailEvent string; camelCase/int-ms read first as the synthetic fallback",
        "covered by tests/contract/test_aws_contract.py against the doc-sourced "
        "PascalCase LookupEvents fixture",
    ),
    ContractNeed(
        "fireflies", "api_response", "transcripts_query", "#5",
        "RESOLVED (Phase 3): client now speaks GraphQL (POST /graphql, "
        "`transcripts` query → data.transcripts) via _graphql/list_transcripts_graphql; "
        "REST path kept for the synthetic mock",
        "covered by tests/contract/test_fireflies_contract.py (GraphQL request + "
        "data.transcripts parse + errors[] handling)",
    ),
    ContractNeed(
        "brex", "api_response", "transactions_page", "#3",
        "RESOLVED (Phase 3, additive): client follows real `next_cursor` cursor "
        "pagination (items[], no total); offset/total path kept as the synthetic fallback",
        "covered by tests/contract/test_brex_contract.py against the doc-sourced "
        "cursor fixture",
    ),
    ContractNeed(
        "miro", "api_response", "boards_page", "#10",
        "RESOLVED (Phase 3, additive): client follows links.next / offset+total "
        "pagination; single-page kept as fallback",
        "covered by tests/contract/test_miro_contract.py against the doc-sourced "
        "pagination envelope fixture",
    ),
    ContractNeed(
        "notion", "api_response", "search_page", "#36",
        "RESOLVED (Phase 3, additive): client loops has_more/next_cursor "
        "(start_cursor) until exhausted; single-call kept as fallback",
        "covered by tests/contract/test_notion_contract.py against the doc-sourced "
        "has_more/next_cursor fixture",
    ),
    ContractNeed(
        "gmail", "api_response", "watch_and_history", "#9",
        "RESOLVED (Phase 3): watch_scheduler stores GREATEST(stored, returned) "
        "historyId on renewal so the cursor never moves backwards",
        "covered by tests/contract/test_gmail_contract.py against the doc-sourced "
        "watch() response",
    ),
    # --- C/D. Idempotency / lifecycle / replay ------------------------------
    ContractNeed(
        "github", "webhook", "pull_request_opened", "#1",
        "RESOLVED (Phase 3): external_id = idempotency.github_object(node_id, "
        "action) = `{node_id}:{action}` so a PR/issue's opened/closed don't "
        "collapse onto one observation (node_id is identical across the lifecycle); "
        "synthetic twin updated in lockstep",
        "covered by tests/contract/test_github_contract.py (opened≠closed external_id; "
        "same-action redelivery dedups) — opened + closed fixtures share one node_id",
    ),
    ContractNeed(
        "github", "webhook", "pull_request_closed", "#1",
        "RESOLVED (Phase 3): see pull_request_opened — the SAME node_id at "
        "action=closed/merged yields a DISTINCT external_id, so the merge "
        "state-change is no longer lost to dedup",
        "covered by tests/contract/test_github_contract.py against the closed "
        "fixture (same node_id as opened)",
    ),
    ContractNeed(
        "jira", "webhook", "issue_updated_nonstatus", "#32",
        "RESOLVED (Phase 3, additive): handler now emits an observation for a "
        "non-status changelog change (summary/assignee/etc.) instead of dropping "
        "it; the status-transition path is unchanged",
        "covered by tests/contract/test_jira_contract.py against the doc-sourced "
        "non-status changelog fixture",
    ),
    # --- E. OAuth lifecycle (official docs acceptable) ----------------------
    ContractNeed(
        "quickbooks", "oauth_token", "refresh", "#24",
        "RESOLVED (Phase 3): oauth_refresh.refresh_access_token does the Basic-"
        "auth grant_type=refresh_token exchange + persists the ROTATED refresh "
        "token; clients re-mint reactively on 401",
        "covered by tests/contract/test_oauth_refresh_contract.py against the "
        "doc-sourced Intuit refresh fixture (request + rotated-token response)",
    ),
    ContractNeed(
        "ramp", "oauth_token", "client_credentials", "#26",
        "RESOLVED (Phase 3): Ramp has NO refresh grant — Basic-auth "
        "grant_type=client_credentials exchange + persist; reactive 401 "
        "re-mint in RampClient",
        "covered by tests/contract/test_oauth_refresh_contract.py against the "
        "doc-sourced Ramp client_credentials fixture",
    ),
    ContractNeed(
        "gusto", "oauth_token", "refresh", "#38",
        "RESOLVED (Phase 3): body-cred grant_type=refresh_token exchange + "
        "persist; reactive 401 re-mint in GustoClient",
        "covered by tests/contract/test_oauth_refresh_contract.py against the "
        "doc-sourced Gusto refresh fixture",
    ),
    ContractNeed(
        "carta", "oauth_token", "client_credentials", "#40",
        "RESOLVED (Phase 3): Carta has NO refresh grant — oauth_refresh re-mints "
        "via grant_type=client_credentials (client secret from the per-install "
        "refresh_secret_ref); reactive 401 re-mint in CartaClient",
        "covered by tests/contract/test_oauth_refresh_contract.py against the "
        "doc-sourced Carta client_credentials fixture",
    ),
]


def outstanding() -> list[ContractNeed]:
    """ContractNeeds whose fixture file is not yet present on disk."""
    from tests.contract.framework import has_fixture

    return [n for n in REGISTRY if not has_fixture(n.provider, n.kind, n.fixture)]
