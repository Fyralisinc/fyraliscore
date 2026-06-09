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
        "handler/fetcher read camelCase; fetcher expects int-ms event time",
        "the REAL botocore LookupEvents response: PascalCase Events[].CloudTrailEvent "
        "(a JSON string) + EventTime as a datetime, not int-ms",
    ),
    ContractNeed(
        "fireflies", "api_response", "transcripts_query", "#5",
        "client issues REST GET paths (cloned from Brex)",
        "the GraphQL POST /graphql request body + the `data.transcripts` response shape",
    ),
    ContractNeed(
        "brex", "api_response", "transactions_page", "#3",
        "offset pagination + `resp.get('total', len(txns))` first-page sentinel",
        "real cursor-token pagination (`next_cursor`) — confirm NO `total` field exists",
    ),
    ContractNeed(
        "miro", "api_response", "boards_page", "#10",
        "list_boards() fetches only the first page",
        "the real boards pagination envelope (size/offset/total or links.next/cursor)",
    ),
    ContractNeed(
        "notion", "api_response", "search_page", "#36",
        "latest_page_edit truncates at 50 results, no pagination",
        "the real `has_more` / `next_cursor` pagination envelope on search/list",
    ),
    ContractNeed(
        "gmail", "api_response", "watch_and_history", "#9",
        "watch_scheduler overwrites stored history_id with watch().historyId",
        "the users.watch() response (historyId) + history.list semantics that justify "
        "a GREATEST(stored, returned) guard",
    ),
    # --- C/D. Idempotency / lifecycle / replay ------------------------------
    ContractNeed(
        "github", "webhook", "pull_request_opened", "#1",
        "handler adopts node_id verbatim as external_id",
        "a real pull_request OPENED webhook (node_id + action + delivery timestamp)",
    ),
    ContractNeed(
        "github", "webhook", "pull_request_closed", "#1",
        "handler adopts node_id verbatim as external_id",
        "the SAME PR's CLOSED/MERGED webhook — to prove node_id is byte-identical "
        "across lifecycle actions (=> external_id must encode action/state)",
    ),
    ContractNeed(
        "jira", "webhook", "issue_updated_nonstatus", "#32",
        "handler routes only status/resolution changelog to _transition_draft",
        "a real issue_updated webhook whose changelog item is a NON-status field change",
    ),
    # --- E. OAuth lifecycle (official docs acceptable) ----------------------
    ContractNeed(
        "quickbooks", "oauth_token", "refresh", "#24",
        "no refresh-token exchange implemented",
        "Intuit refresh: POST oauth2/v1/tokens/bearer request + response "
        "(access_token, refresh_token, expires_in, x_refresh_token_expires_in)",
    ),
    ContractNeed(
        "ramp", "oauth_token", "refresh", "#26",
        "no refresh-token exchange implemented",
        "Ramp token refresh request + response shape (grant_type=refresh_token)",
    ),
    ContractNeed(
        "gusto", "oauth_token", "refresh", "#38",
        "no refresh-token exchange implemented",
        "Gusto token refresh request + response shape",
    ),
    ContractNeed(
        "carta", "oauth_token", "client_credentials", "#40",
        "no token re-mint implemented",
        "Carta client_credentials token request + response (expires_in) for 401 re-mint",
    ),
]


def outstanding() -> list[ContractNeed]:
    """ContractNeeds whose fixture file is not yet present on disk."""
    from tests.contract.framework import has_fixture

    return [n for n in REGISTRY if not has_fixture(n.provider, n.kind, n.fixture)]
