"""Wave-B Provider Lab adapters for the provider API surfaces Fyralis uses.

Only the pinned production-client route surfaces are declared here. Production
client calls outside those surfaces continue to receive the lab's strict 501.
"""

from __future__ import annotations

import base64
import copy
import math
import re
import threading
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import parse_qs, urlencode

from .protocol import (
    ProviderOperationBinding,
    ProviderProtocolSurface,
    ProviderRequest,
    ProviderResponse,
    ProviderRoute,
)
from .renewal_lifecycle import (
    LifecycleWatchRegistry,
    lifecycle_resource_id,
    lifecycle_token_response,
    lifecycle_watch_expiration,
    require_lifecycle_access_token,
    validate_lifecycle_client_credentials,
    validate_lifecycle_refresh_grant,
)


def _scope(request: ProviderRequest) -> str:
    return request.headers.get("x-provider-lab-scope") or "global"


def _params(request: ProviderRequest) -> dict[str, str]:
    return {key: value for key, value in request.query_items}


def _integer(
    value: Any,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


class _WaveBAdapter:
    routes: tuple[ProviderRoute, ...]
    protocol_surfaces: tuple[ProviderProtocolSurface, ...] = ()

    def default_state(self) -> Mapping[str, Any]:
        return {}

    def resolve_scope(self, request: ProviderRequest) -> str:
        return _scope(request)


class BrexAdapter(_WaveBAdapter):
    source = "brex"
    routes = (
        ProviderRoute(
            "brex.cash_accounts",
            "/v2/accounts/cash",
            operation_ids=("accounts.cash.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "brex.card_accounts",
            "/v2/accounts/card",
            operation_ids=("accounts.card.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "brex.cash_transactions",
            "/v2/transactions/cash/{account_id}",
            operation_ids=("transactions.cash.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "brex.card_transactions",
            "/v2/transactions/card/primary",
            operation_ids=("transactions.card.list",),
            quota_bucket="rest",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"accounts": {}}

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        fixtures = request.source_state.get("accounts") or {}
        route_id = request.route.route_id
        params = _params(request)
        if route_id in {"brex.cash_accounts", "brex.card_accounts"}:
            kind = "card" if route_id == "brex.card_accounts" else "cash"
            accounts = _financial_accounts(fixtures, kind)
            if kind == "card":
                return ProviderResponse.json({"items": accounts})
            page, cursor = _offset_page(accounts, params, default_limit=1000)
            return ProviderResponse.json({"items": page, "next_cursor": cursor})
        if route_id == "brex.cash_transactions":
            account_id = str(request.path_params["account_id"])
            fixture = fixtures.get(account_id)
            if not isinstance(fixture, dict):
                pool: list[dict[str, Any]] = []
            elif params.get("posted_at_start") and isinstance(
                fixture.get("delta"), list
            ):
                pool = list(fixture["delta"])
            else:
                pool = list(fixture.get("transactions", []))
            return ProviderResponse.json(_brex_transaction_page(pool, params))
        if route_id == "brex.card_transactions":
            use_delta = bool(params.get("posted_at_start"))
            pool = []
            for fixture in fixtures.values():
                if not isinstance(fixture, dict):
                    continue
                account = fixture.get("account") or {}
                if _account_kind(account) != "card":
                    continue
                rows = (
                    fixture.get("delta")
                    if use_delta and isinstance(fixture.get("delta"), list)
                    else fixture.get("transactions")
                )
                if isinstance(rows, list):
                    pool.extend(row for row in rows if isinstance(row, dict))
            return ProviderResponse.json(_brex_transaction_page(pool, params))
        raise RuntimeError(f"unhandled Brex route {route_id}")


def _account_kind(account: Mapping[str, Any]) -> str:
    value = str(
        account.get("_fyralis_account_kind")
        or account.get("type")
        or account.get("kind")
        or "cash"
    ).lower()
    return "card" if value in {"card", "credit_card", "primary_card"} else "cash"


def _financial_accounts(fixtures: Mapping[str, Any], kind: str) -> list[dict[str, Any]]:
    accounts = []
    for account_id, fixture in fixtures.items():
        if not isinstance(fixture, dict):
            continue
        account = dict(fixture.get("account") or {"id": account_id})
        if _account_kind(account) != kind:
            continue
        account.setdefault("id", account_id)
        account.setdefault("_fyralis_account_kind", kind)
        account.pop("transactions", None)
        accounts.append(account)
    return accounts


def _offset_page(
    items: list[dict[str, Any]],
    params: Mapping[str, str],
    *,
    default_limit: int,
) -> tuple[list[dict[str, Any]], str | None]:
    limit = _integer(params.get("limit"), default_limit)
    offset = _integer(params.get("cursor"), 0)
    page = items[offset : offset + limit]
    next_offset = offset + len(page)
    return page, str(next_offset) if page and next_offset < len(items) else None


def _brex_transaction_page(
    pool: list[dict[str, Any]], params: Mapping[str, str]
) -> dict[str, Any]:
    floor = params.get("posted_at_start")
    if floor:
        pool = [
            row
            for row in pool
            if str(
                row.get("postedAt")
                or row.get("posted_at")
                or row.get("createdAt")
                or row.get("created_at")
                or ""
            )[:10]
            >= floor[:10]
        ]
    page, cursor = _offset_page(pool, params, default_limit=100)
    return {"items": page, "next_cursor": cursor}


_CARTA_COLLECTIONS = {
    "stakeholders": "stakeholder",
    "shareClasses": "shareClass",
    "optionGrants": "optionGrant",
    "convertibleNotes": "convertibleNote",
}


class CartaAdapter(_WaveBAdapter):
    source = "carta"
    routes = (
        ProviderRoute(
            "carta.oauth_token",
            "/o/access_token/",
            operation_ids=("oauth.token.mint",),
            methods=("POST",),
            quota_bucket=None,
        ),
        ProviderRoute(
            "carta.oauth_token_no_slash",
            "/o/access_token",
            methods=("POST",),
            quota_bucket=None,
        ),
        ProviderRoute(
            "carta.issuers",
            "/v1alpha1/issuers",
            operation_ids=("issuers.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "carta.issuer",
            "/v1alpha1/issuers/{issuer_id}",
            operation_ids=("issuers.get",),
            quota_bucket="rest",
        ),
        *tuple(
            ProviderRoute(
                f"carta.{collection}",
                f"/v1alpha1/issuers/{{issuer_id}}/{collection}",
                operation_ids=(
                    {
                        "stakeholders": "stakeholders.list",
                        "shareClasses": "share_classes.list",
                        "optionGrants": "option_grants.list",
                        "convertibleNotes": "convertible_notes.list",
                    }[collection],
                ),
                quota_bucket="rest",
            )
            for collection in _CARTA_COLLECTIONS
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"issuers": {}}

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = request.headers.get("x-provider-lab-scope")
        if explicit and explicit != "global":
            return explicit
        authorization = request.headers.get("authorization", "")
        if authorization.lower().startswith("bearer "):
            token = authorization[7:].strip()
            for prefix in ("lab-carta::", "spam-carta::"):
                if token.startswith(prefix):
                    issuer_id = token[len(prefix) :]
                    if issuer_id:
                        return issuer_id
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        if request.route.route_id in {
            "carta.oauth_token",
            "carta.oauth_token_no_slash",
        }:
            rejected = validate_lifecycle_client_credentials(request)
            if rejected is not None:
                return rejected
            lifecycle = lifecycle_token_response(
                request,
                token_type="Bearer",
                extra={
                    "scope": (
                        "read_issuer_info read_issuer_stakeholders "
                        "read_issuer_shareclasses read_issuer_securities"
                    )
                },
            )
            if lifecycle is not None:
                return ProviderResponse.json(lifecycle)
            return ProviderResponse.json(
                {
                    "access_token": "mock-carta-access-token",
                    "expires_in": 3600,
                    "scope": (
                        "read_issuer_info read_issuer_stakeholders "
                        "read_issuer_shareclasses read_issuer_securities"
                    ),
                    "token_type": "Bearer",
                }
            )
        rejected = require_lifecycle_access_token(request)
        if rejected is not None:
            return rejected
        if not request.headers.get("authorization"):
            return ProviderResponse.json(
                {"error": "missing Authorization header"}, status_code=401
            )

        fixtures = request.source_state.get("issuers") or {}
        if not isinstance(fixtures, dict):
            fixtures = {}
        route_id = request.route.route_id
        if route_id == "carta.issuers":
            visible = (
                {request.scope: fixtures.get(request.scope)}
                if request.scope != "global"
                else fixtures
            )
            issuers = [
                _carta_issuer(fixture, issuer_id)
                for issuer_id, fixture in visible.items()
                if isinstance(fixture, dict)
            ]
            return ProviderResponse.json({"issuers": issuers})

        issuer_id = str(request.path_params["issuer_id"])
        fixture = fixtures.get(issuer_id)
        if not isinstance(fixture, dict) or request.scope not in {"global", issuer_id}:
            return ProviderResponse.json({"error": "issuer not found"}, status_code=404)
        issuer = _carta_issuer(fixture, issuer_id)
        if route_id == "carta.issuer":
            return ProviderResponse.json({"issuer": issuer})

        collection = route_id.split(".", 1)[1]
        rows = list(
            (fixture.get("entities") or {}).get(
                _CARTA_COLLECTIONS[collection],
                [],
            )
        )
        params = _params(request)
        bound = params.get("lastModifiedDatetimeAfter")
        if bound and collection == "optionGrants":
            rows = [
                row
                for row in rows
                if str(((row.get("lastModifiedDatetime") or {}).get("value")) or "")
                > bound
            ]
        page_size = _integer(params.get("pageSize"), 25, minimum=1, maximum=100)
        token = params.get("pageToken")
        if token:
            if not token.startswith("off:"):
                return ProviderResponse.json(
                    {"error": "malformed pageToken"}, status_code=400
                )
            try:
                offset = max(0, int(token[4:]))
            except ValueError:
                return ProviderResponse.json(
                    {"error": "malformed pageToken"}, status_code=400
                )
        else:
            offset = 0
        page = rows[offset : offset + page_size]
        end = offset + len(page)
        body: dict[str, Any] = {collection: page}
        if page and end < len(rows):
            body["nextPageToken"] = f"off:{end}"
        return ProviderResponse.json(body)


def _carta_issuer(
    fixture: Mapping[str, Any],
    issuer_id: str,
) -> dict[str, Any]:
    issuer = fixture.get("issuer")
    if isinstance(issuer, dict) and issuer.get("id"):
        return dict(issuer)
    return {"id": issuer_id, "legalName": "Sandbox Issuer"}


class DeelAdapter(_WaveBAdapter):
    source = "deel"
    routes = (
        ProviderRoute(
            "deel.contracts",
            "/contracts",
            operation_ids=("contracts.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "deel.contract",
            "/contracts/{contract_id}",
            operation_ids=("contracts.get",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "deel.invoices",
            "/invoices",
            operation_ids=("invoices.list",),
            quota_bucket="rest",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"contracts": {}}

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        fixtures = request.source_state.get("contracts") or {}
        params = _params(request)
        route_id = request.route.route_id
        if route_id == "deel.contracts":
            items = [
                dict(fixture.get("contract") or {"id": contract_id})
                for contract_id, fixture in fixtures.items()
                if isinstance(fixture, dict)
            ]
            return ProviderResponse.json(_deel_page(items, params))
        if route_id == "deel.contract":
            contract_id = str(request.path_params["contract_id"])
            fixture = fixtures.get(contract_id)
            if not isinstance(fixture, dict):
                return ProviderResponse.json(
                    {"error": f"no contract {contract_id}"}, status_code=404
                )
            return ProviderResponse.json(
                {"data": fixture.get("contract") or {"id": contract_id}}
            )
        if route_id == "deel.invoices":
            contract_filter = params.get("contract_id")
            floor = params.get("created_after")
            invoices: list[dict[str, Any]] = []
            for contract_id, fixture in fixtures.items():
                if not isinstance(fixture, dict):
                    continue
                if contract_filter and contract_filter != contract_id:
                    continue
                key = (
                    "delta"
                    if floor and isinstance(fixture.get("delta"), list)
                    else "payments"
                )
                for invoice in fixture.get(key, []):
                    if not isinstance(invoice, dict):
                        continue
                    row = dict(invoice)
                    row.setdefault("contract_id", contract_id)
                    date = str(
                        row.get("createdAt")
                        or row.get("created_at")
                        or row.get("issued_at")
                        or row.get("invoice_date")
                        or ""
                    )
                    if not floor or date[:10] >= floor[:10]:
                        invoices.append(row)
            return ProviderResponse.json(_deel_page(invoices, params))
        raise RuntimeError(f"unhandled Deel route {route_id}")


def _deel_page(
    items: list[dict[str, Any]], params: Mapping[str, str]
) -> dict[str, Any]:
    limit = _integer(params.get("limit"), 100)
    offset = _integer(params.get("offset"), 0)
    return {
        "data": items[offset : offset + limit],
        "page": {"cursor": None, "total_rows": len(items)},
    }


class FigmaAdapter(_WaveBAdapter):
    source = "figma"
    routes = (
        ProviderRoute(
            "figma.me",
            "/v1/me",
            operation_ids=("users.me.get",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "figma.team_projects",
            "/v1/teams/{team_id}/projects",
            operation_ids=("teams.projects.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "figma.project_files",
            "/v1/projects/{project_id}/files",
            operation_ids=("projects.files.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "figma.file_versions",
            "/v1/files/{file_key}/versions",
            operation_ids=("file_versions.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "figma.file_comments",
            "/v1/files/{file_key}/comments",
            operation_ids=("file_comments.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "figma.file",
            "/v1/files/{file_key}",
            operation_ids=("files.get",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "figma.oauth_token",
            "/v1/oauth/token",
            operation_ids=(
                "oauth.token.exchange",
                "oauth.token.refresh",
            ),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="oauth.token.exchange",
                    method="POST",
                    headers=(
                        (
                            "Content-Type",
                            "application/x-www-form-urlencoded",
                        ),
                    ),
                    body=(
                        b"grant_type=authorization_code&code=provider-lab"
                        b"&redirect_uri=https%3A%2F%2Fprovider-lab.test"
                        b"&code_verifier=provider-lab"
                    ),
                ),
                ProviderOperationBinding(
                    operation_id="oauth.token.refresh",
                    method="POST",
                    headers=(
                        (
                            "Content-Type",
                            "application/x-www-form-urlencoded",
                        ),
                    ),
                    body=(
                        b"grant_type=refresh_token"
                        b"&refresh_token=provider-lab"
                    ),
                ),
            ),
            methods=("POST",),
            quota_bucket="oauth",
        ),
    )

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._hits: dict[str, int] = {}

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()

    def default_state(self) -> Mapping[str, Any]:
        return {"files": {}}

    def _hit(self, key: str) -> int:
        with self._lock:
            self._hits[key] = self._hits.get(key, 0) + 1
            return self._hits[key]

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        files = request.source_state.get("files") or {}
        route_id = request.route.route_id
        if route_id == "figma.oauth_token":
            form = parse_qs(request.body.decode("utf-8", "replace"))
            grant_type = (form.get("grant_type") or ["authorization_code"])[0]
            return ProviderResponse.json(
                {
                    "access_token": "lab-figma-access-token",
                    "refresh_token": "lab-figma-refresh-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                    "grant_type": grant_type,
                }
            )
        if route_id == "figma.me":
            return ProviderResponse.json(
                {
                    "id": "provider-lab-user",
                    "email": "figma-user@provider-lab.test",
                    "handle": "Provider Lab",
                }
            )
        if route_id == "figma.team_projects":
            return ProviderResponse.json(
                {"projects": [{"id": "mock-project", "name": "Synthetic"}]}
            )
        if route_id == "figma.project_files":
            output = []
            for file_key, fixture in files.items():
                item = dict(fixture.get("file") or {"key": file_key})
                item.setdefault("key", file_key)
                output.append(item)
            return ProviderResponse.json({"files": output})

        file_key = str(request.path_params["file_key"])
        fixture = files.get(file_key)
        if route_id == "figma.file":
            if not isinstance(fixture, dict):
                return ProviderResponse.json(
                    {"error": f"no file {file_key}"}, status_code=404
                )
            return ProviderResponse.json(fixture.get("file") or {"key": file_key})
        if not isinstance(fixture, dict):
            events: list[dict[str, Any]] = []
        else:
            self._hit(f"{route_id}:{file_key}")
            with self._lock:
                use_delta = (
                    self._hits.get(f"figma.file_versions:{file_key}", 0) > 1
                    or self._hits.get(f"figma.file_comments:{file_key}", 0) > 1
                )
            key = (
                "delta"
                if use_delta and isinstance(fixture.get("delta"), list)
                else "events"
            )
            events = [
                event for event in fixture.get(key, []) if isinstance(event, dict)
            ]
        if route_id == "figma.file_versions":
            versions = [
                {
                    "id": event.get("version") or event.get("id"),
                    "label": event.get("label"),
                    "description": event.get("description"),
                    "user": event.get("triggered_by") or event.get("user"),
                    "created_at": event.get("created_at") or event.get("createdAt"),
                }
                for event in events
                if str(event.get("event_type") or event.get("type") or "").upper()
                != "FILE_COMMENT"
            ]
            return ProviderResponse.json(
                {"versions": versions, "pagination": {"next_page": None}}
            )
        comments = [
            {
                "id": event.get("id"),
                "message": event.get("message") or event.get("label"),
                "user": event.get("triggered_by") or event.get("user"),
                "created_at": event.get("created_at") or event.get("createdAt"),
                "updated_at": (
                    event.get("updated_at")
                    or event.get("created_at")
                    or event.get("createdAt")
                ),
            }
            for event in events
            if str(event.get("event_type") or event.get("type") or "").upper()
            == "FILE_COMMENT"
        ]
        return ProviderResponse.json({"comments": comments})


class FirefliesAdapter(_WaveBAdapter):
    source = "fireflies"
    routes = (
        ProviderRoute(
            "fireflies.graphql",
            "/graphql",
            operation_ids=(
                "user.get",
                "transcript.get",
                "transcripts.list",
            ),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="user.get",
                    method="POST",
                    headers=(("Content-Type", "application/json"),),
                    body=(
                        b'{"query":"query { user { id email name } }",'
                        b'"variables":{}}'
                    ),
                ),
                ProviderOperationBinding(
                    operation_id="transcript.get",
                    method="POST",
                    headers=(("Content-Type", "application/json"),),
                    body=(
                        b'{"query":"query($id: String!) { transcript(id: $id) '
                        b'{ id } }","variables":{"id":"provider-lab"}}'
                    ),
                ),
                ProviderOperationBinding(
                    operation_id="transcripts.list",
                    method="POST",
                    headers=(("Content-Type", "application/json"),),
                    body=(
                        b'{"query":"query { transcripts { id } }",'
                        b'"variables":{"limit":50,"skip":0}}'
                    ),
                ),
            ),
            methods=("POST",),
            quota_bucket="graphql",
            transport="graphql",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"transcripts": []}

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        try:
            body = request.json()
        except (ValueError, TypeError):
            return ProviderResponse.json(
                {"errors": [{"message": "invalid json"}]}, status_code=400
            )
        if not isinstance(body, dict):
            body = {}
        query = str(body.get("query") or "")
        variables = (
            body.get("variables") if isinstance(body.get("variables"), dict) else {}
        )
        state = request.source_state
        if (
            "user" in query
            and "transcripts" not in query
            and "transcript(" not in query
        ):
            workspace = state.get("workspace")
            if isinstance(workspace, dict):
                user = {
                    "id": (
                        workspace.get("id")
                        or workspace.get("workspace_id")
                        or state.get("workspace_id")
                        or "ws-mock"
                    ),
                    "email": workspace.get("email") or "mock-fireflies@example.com",
                    "name": (
                        workspace.get("name")
                        or workspace.get("workspace_name")
                        or "Synthetic Workspace"
                    ),
                }
            else:
                user = {
                    "id": state.get("workspace_id") or "ws-mock",
                    "email": "mock-fireflies@example.com",
                    "name": state.get("workspace_name") or "Synthetic Workspace",
                }
            return ProviderResponse.json({"data": {"user": user}})
        if "transcript(" in query:
            transcript_id = str(variables.get("id") or "")
            transcript = next(
                (
                    item
                    for item in state.get("transcripts", [])
                    if isinstance(item, dict)
                    and str(
                        item.get("id")
                        or item.get("transcript_id")
                        or item.get("transcriptId")
                        or ""
                    )
                    == transcript_id
                ),
                None,
            )
            return ProviderResponse.json({"data": {"transcript": transcript}})
        if "transcripts" in query:
            floor = variables.get("fromDate")
            key = (
                "delta"
                if isinstance(floor, str)
                and floor
                and isinstance(state.get("delta"), list)
                else "transcripts"
            )
            items = [item for item in state.get(key, []) if isinstance(item, dict)]
            if isinstance(floor, str) and floor:
                items = [item for item in items if _fireflies_date(item) >= floor[:10]]
            skip = _integer(variables.get("skip"), 0)
            limit = _integer(variables.get("limit"), 50)
            return ProviderResponse.json(
                {"data": {"transcripts": items[skip : skip + limit]}}
            )
        return ProviderResponse.json({"data": {}})


def _fireflies_date(item: Mapping[str, Any]) -> str:
    value = item.get("dateTime") or item.get("date") or item.get("createdAt") or ""
    if isinstance(value, (int, float)):
        return (
            datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).date().isoformat()
        )
    return value[:10] if isinstance(value, str) else ""


def _google_directory_routes(source: str) -> tuple[ProviderRoute, ...]:
    return (
        ProviderRoute(
            f"{source}.directory_users",
            "/admin/directory/v1/users",
            operation_ids=(
                "directory.users.list",
                "directory.users_by_org_unit.list",
            ),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="directory.users.list",
                    method="GET",
                    query_items=(("domain", "provider-lab.test"),),
                ),
                ProviderOperationBinding(
                    operation_id="directory.users_by_org_unit.list",
                    method="GET",
                    query_items=(("query", "orgUnitPath=/"),),
                ),
            ),
            quota_bucket="directory-api",
        ),
        ProviderRoute(
            f"{source}.directory_groups",
            "/admin/directory/v1/groups",
            operation_ids=("directory.groups.list",),
            quota_bucket="directory-api",
        ),
        ProviderRoute(
            f"{source}.directory_group_members",
            "/admin/directory/v1/groups/{group_key}/members",
            operation_ids=("directory.group_members.list",),
            quota_bucket="directory-api",
        ),
        ProviderRoute(
            f"{source}.directory_org_units",
            "/admin/directory/v1/customer/{customer_id}/orgunits",
            operation_ids=("directory.org_units.list",),
            quota_bucket="directory-api",
        ),
    )


def _google_directory_response(request: ProviderRequest) -> ProviderResponse:
    directory = request.source_state.get("directory") or {}
    suffix = request.route.route_id.split(".", 1)[1]
    if suffix == "directory_org_units":
        return ProviderResponse.json(
            {"organizationUnits": list(directory.get("org_units") or [])}
        )
    if suffix == "directory_group_members":
        group_key = str(request.path_params["group_key"]).lower()
        rows = list((directory.get("group_members") or {}).get(group_key, []))
        return ProviderResponse.json({"members": rows})
    if suffix == "directory_groups":
        return ProviderResponse.json(
            {"groups": list(directory.get("groups") or [])}
        )
    if suffix == "directory_users":
        rows = list(directory.get("users") or [])
        query = request.query_one("query", "") or ""
        if query.startswith("orgUnitPath="):
            path = query.removeprefix("orgUnitPath=")
            rows = [
                row
                for row in rows
                if str(row.get("orgUnitPath") or "") == path
            ]
        return ProviderResponse.json({"users": rows})
    raise RuntimeError(f"unhandled Google Directory route {request.route.route_id}")


class GoogleCalendarAdapter(_WaveBAdapter):
    source = "google_calendar"
    routes = (
        ProviderRoute(
            "google_calendar.token",
            "/token",
            operation_ids=("dwd.token.exchange",),
            methods=("POST",),
            quota_bucket=None,
        ),
        *_google_directory_routes("google_calendar"),
        ProviderRoute(
            "google_calendar.calendar_list",
            "/users/me/calendarList",
            operation_ids=("calendarList.list",),
            quota_bucket="calendar-api",
        ),
        ProviderRoute(
            "google_calendar.events",
            "/calendars/{calendar_id}/events",
            operation_ids=("events.list",),
            quota_bucket="calendar-api",
        ),
        ProviderRoute(
            "google_calendar.events_watch",
            "/calendars/{calendar_id}/events/watch",
            operation_ids=("events.watch",),
            methods=("POST",),
            quota_bucket="calendar-api",
        ),
        ProviderRoute(
            "google_calendar.channels_stop",
            "/channels/stop",
            operation_ids=("channels.stop",),
            methods=("POST",),
            quota_bucket="calendar-api",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"calendars": {}}

    def __init__(self) -> None:
        self._lifecycle_watches = LifecycleWatchRegistry(self.source)

    def reset(self) -> None:
        self._lifecycle_watches.reset()

    def reset_lifecycle_watches(self) -> None:
        """Discard opt-in watch state when the control-plane fixture changes."""

        self._lifecycle_watches.reset()

    def watch_lifecycle_snapshot(
        self,
        *,
        now: datetime,
        source_state: Mapping[str, Any],
        scope: str | None = None,
        channel_id: str | None = None,
        resource_id: str | None = None,
    ) -> dict[str, Any]:
        return self._lifecycle_watches.snapshot(
            now=now,
            source_state=source_state,
            scope=scope,
            channel_id=channel_id,
            resource_id=resource_id,
        )

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        if request.route.route_id == "google_calendar.token":
            lifecycle = lifecycle_token_response(
                request,
                token_type="Bearer",
            )
            if lifecycle is not None:
                return ProviderResponse.json(lifecycle)
            return ProviderResponse.json(
                {
                    "access_token": "sandbox-access-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                }
            )
        rejected = require_lifecycle_access_token(request)
        if rejected is not None:
            return rejected
        if ".directory_" in request.route.route_id:
            return _google_directory_response(request)
        if request.route.route_id == "google_calendar.calendar_list":
            calendars = request.source_state.get("calendars") or {}
            return ProviderResponse.json(
                {
                    "items": [
                        {"id": calendar_id, "summary": calendar_id}
                        for calendar_id in sorted(calendars)
                    ]
                }
            )
        if request.route.route_id == "google_calendar.channels_stop":
            # Do not parse static-fixture requests: this route historically
            # returned an idempotent success for the compact fixture surface.
            # Lifecycle mode validates the real id/resource stop payload.
            if lifecycle_watch_expiration(request) is not None:
                try:
                    body = request.json()
                except (TypeError, ValueError):
                    body = None
                channel_id = (
                    str(body.get("id"))
                    if isinstance(body, Mapping) and body.get("id")
                    else None
                )
                resource_id = (
                    str(body.get("resourceId"))
                    if isinstance(body, Mapping) and body.get("resourceId")
                    else None
                )
                try:
                    self._lifecycle_watches.stop(
                        request,
                        channel_id=channel_id,
                        resource_id=resource_id,
                    )
                except ValueError as exc:
                    return ProviderResponse.json(
                        {"error": {"code": 400, "message": str(exc)}},
                        status_code=400,
                    )
            return ProviderResponse.json({})

        calendar_id = str(request.path_params["calendar_id"])
        if request.route.route_id == "google_calendar.events_watch":
            body = request.json()
            channel_id = (
                str(body.get("id"))
                if isinstance(body, dict) and body.get("id")
                else "provider-lab-calendar-channel"
            )
            expiration = lifecycle_watch_expiration(request)
            if expiration is not None:
                resource_id = lifecycle_resource_id(
                    request,
                    resource_prefix=f"calendar-resource:{calendar_id}",
                )
                self._lifecycle_watches.register(
                    request,
                    target=f"calendars/{calendar_id}/events",
                    channel_id=channel_id,
                    resource_id=resource_id,
                )
                return ProviderResponse.json(
                    {
                        "id": channel_id,
                        "resourceId": resource_id,
                        "resourceUri": (
                            "https://www.googleapis.com/calendar/v3/calendars/"
                            f"{calendar_id}/events"
                        ),
                        "expiration": expiration,
                    }
                )
            return ProviderResponse.json(
                {
                    "id": channel_id,
                    "resourceId": f"calendar-resource:{calendar_id}",
                    "resourceUri": (
                        "https://www.googleapis.com/calendar/v3/calendars/"
                        f"{calendar_id}/events"
                    ),
                    "expiration": "4102444800000",
                }
            )
        fixture = (request.source_state.get("calendars") or {}).get(calendar_id)
        if not isinstance(fixture, dict):
            return ProviderResponse.json({"items": [], "nextSyncToken": "sync-empty"})
        params = _params(request)
        events = list(fixture.get("events", []))
        delta = list(fixture.get("delta", []))
        if "syncToken" in params:
            if params["syncToken"] == "EXPIRED":
                return ProviderResponse.json(
                    {
                        "error": {
                            "code": 410,
                            "message": "Sync token is no longer valid.",
                        }
                    },
                    status_code=410,
                )
            return ProviderResponse.json({"items": delta, "nextSyncToken": "sync-2"})
        if "updatedMin" in params:
            bound = params["updatedMin"]
            rows = [
                item for item in events + delta if str(item.get("updated", "")) > bound
            ]
            maximum = _integer(params.get("maxResults"), 250)
            return ProviderResponse.json({"items": rows[:maximum]})
        return ProviderResponse.json({"items": events, "nextSyncToken": "sync-1"})


class GoogleDriveAdapter(_WaveBAdapter):
    source = "google_drive"
    routes = (
        ProviderRoute(
            "google_drive.token",
            "/token",
            operation_ids=("dwd.token.exchange",),
            methods=("POST",),
            quota_bucket=None,
        ),
        *_google_directory_routes("google_drive"),
        ProviderRoute(
            "google_drive.start_page_token",
            "/changes/startPageToken",
            operation_ids=("changes.getStartPageToken",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.changes",
            "/changes",
            operation_ids=("changes.list",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.changes_watch",
            "/changes/watch",
            operation_ids=("changes.watch",),
            methods=("POST",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.channels_stop",
            "/channels/stop",
            operation_ids=("channels.stop",),
            methods=("POST",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.drives",
            "/drives",
            operation_ids=("drives.list",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.file_export",
            "/files/{file_id}/export",
            operation_ids=("files.export",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.file_comments",
            "/files/{file_id}/comments",
            operation_ids=("comments.list",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.file_revisions",
            "/files/{file_id}/revisions",
            operation_ids=("revisions.list",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.file_media",
            "/files/{file_id}",
            operation_ids=("files.get",),
            quota_bucket="drive-api",
        ),
        ProviderRoute(
            "google_drive.files",
            "/files",
            operation_ids=("files.list",),
            quota_bucket="drive-api",
        ),
    )

    def __init__(self) -> None:
        self._lifecycle_watches = LifecycleWatchRegistry(self.source)

    def reset(self) -> None:
        self._lifecycle_watches.reset()

    def reset_lifecycle_watches(self) -> None:
        """Discard opt-in watch state when the control-plane fixture changes."""

        self._lifecycle_watches.reset()

    def watch_lifecycle_snapshot(
        self,
        *,
        now: datetime,
        source_state: Mapping[str, Any],
        scope: str | None = None,
        channel_id: str | None = None,
        resource_id: str | None = None,
    ) -> dict[str, Any]:
        return self._lifecycle_watches.snapshot(
            now=now,
            source_state=source_state,
            scope=scope,
            channel_id=channel_id,
            resource_id=resource_id,
        )

    def default_state(self) -> Mapping[str, Any]:
        return {
            "files": [],
            "changes": [],
            "exports": {},
            "comments": {},
            "revisions": {},
            "shared_drives": [],
            "user_drives": {},
            "shared_drive_content": {},
            "start_page_token": "spt-1",
            "new_start_page_token": "spt-2",
        }

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = request.headers.get("x-provider-lab-scope")
        if explicit:
            return explicit.lower()
        authorization = request.headers.get("authorization", "")
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer":
            for prefix in ("lab-gmail::", "lab-gdrive::", "wstok:"):
                if token.startswith(prefix):
                    return token[len(prefix) :].lower() or "global"
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        state = request.source_state
        route_id = request.route.route_id
        if route_id == "google_drive.token":
            lifecycle = lifecycle_token_response(
                request,
                token_type="Bearer",
            )
            if lifecycle is not None:
                return ProviderResponse.json(lifecycle)
            return ProviderResponse.json(
                {
                    "access_token": "sandbox-access-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                }
            )
        rejected = require_lifecycle_access_token(request)
        if rejected is not None:
            return rejected
        if ".directory_" in route_id:
            return _google_directory_response(request)
        fixture = self._fixture(request)
        if route_id == "google_drive.start_page_token":
            return ProviderResponse.json(
                {
                    "startPageToken": fixture.get(
                        "start_page_token",
                        state.get("start_page_token", "spt-1"),
                    )
                }
            )
        if route_id == "google_drive.changes":
            params = _params(request)
            if params.get("pageToken") == "EXPIRED":
                return ProviderResponse.json(
                    {
                        "error": {
                            "code": 410,
                            "message": "Page token expired.",
                        }
                    },
                    status_code=410,
                )
            page_size = _integer(params.get("pageSize"), 200)
            return ProviderResponse.json(
                {
                    "changes": list(fixture.get("changes", []))[:page_size],
                    "newStartPageToken": fixture.get(
                        "new_start_page_token",
                        state.get("new_start_page_token", "spt-2"),
                    ),
                }
            )
        if route_id == "google_drive.changes_watch":
            body = request.json()
            channel_id = (
                str(body.get("id"))
                if isinstance(body, dict) and body.get("id")
                else "provider-lab-drive-channel"
            )
            expiration = lifecycle_watch_expiration(request)
            if expiration is not None:
                resource_id = lifecycle_resource_id(
                    request,
                    resource_prefix="drive-change-resource",
                )
                drive_id = _params(request).get("driveId") or "my-drive"
                self._lifecycle_watches.register(
                    request,
                    target=f"changes/{drive_id}",
                    channel_id=channel_id,
                    resource_id=resource_id,
                )
                return ProviderResponse.json(
                    {
                        "id": channel_id,
                        "resourceId": resource_id,
                        "resourceUri": "https://www.googleapis.com/drive/v3/changes",
                        "expiration": expiration,
                    }
                )
            return ProviderResponse.json(
                {
                    "id": channel_id,
                    "resourceId": "drive-change-resource",
                    "resourceUri": "https://www.googleapis.com/drive/v3/changes",
                    "expiration": "4102444800000",
                }
            )
        if route_id == "google_drive.channels_stop":
            # Keep the non-lifecycle fixture's historical idempotent response
            # shape, while lifecycle mode validates and records a real stop.
            if lifecycle_watch_expiration(request) is not None:
                try:
                    body = request.json()
                except (TypeError, ValueError):
                    body = None
                channel_id = (
                    str(body.get("id"))
                    if isinstance(body, Mapping) and body.get("id")
                    else None
                )
                resource_id = (
                    str(body.get("resourceId"))
                    if isinstance(body, Mapping) and body.get("resourceId")
                    else None
                )
                try:
                    self._lifecycle_watches.stop(
                        request,
                        channel_id=channel_id,
                        resource_id=resource_id,
                    )
                except ValueError as exc:
                    return ProviderResponse.json(
                        {"error": {"code": 400, "message": str(exc)}},
                        status_code=400,
                    )
            return ProviderResponse.json({})
        if route_id == "google_drive.drives":
            return ProviderResponse.json(
                {"drives": list(state.get("shared_drives", []))}
            )
        if route_id == "google_drive.files":
            return ProviderResponse.json({"files": list(fixture.get("files", []))})

        file_id = str(request.path_params["file_id"])
        content_fixture = self._fixture_for_file(request, file_id)
        if route_id == "google_drive.file_comments":
            return ProviderResponse.json(
                {"comments": (content_fixture.get("comments") or {}).get(file_id, [])}
            )
        if route_id == "google_drive.file_revisions":
            return ProviderResponse.json(
                {"revisions": (content_fixture.get("revisions") or {}).get(file_id, [])}
            )
        if (
            route_id == "google_drive.file_media"
            and request.query_one("alt") != "media"
        ):
            return ProviderResponse.json(
                {
                    "error": {
                        "message": (
                            "file metadata GET is outside the ported mock surface"
                        )
                    }
                },
                status_code=404,
            )
        export = (content_fixture.get("exports") or {}).get(file_id, "")
        if isinstance(export, dict) and "base64" in export:
            raw = base64.b64decode(str(export["base64"]))
            content_type = str(export.get("content_type") or "application/pdf")
        else:
            raw = str(export).encode("utf-8")
            content_type = "text/plain; charset=UTF-8"
        return ProviderResponse(
            status_code=200,
            raw_body=raw,
            media_type=content_type,
        )

    @staticmethod
    def _fixture(request: ProviderRequest) -> Mapping[str, Any]:
        state = request.source_state
        drive_id = request.query_one("driveId")
        if drive_id:
            shared = state.get("shared_drive_content") or {}
            fixture = shared.get(drive_id)
            if isinstance(fixture, Mapping):
                return fixture
        users = state.get("user_drives") or {}
        fixture = users.get(request.scope)
        if isinstance(fixture, Mapping):
            return fixture

        # Preserve the convenient unscoped single-fixture surface used by
        # focused Provider Lab tests, but fail closed once sibling corpora are
        # present. Production shared-drive requests select by driveId above;
        # My Drive requests select by their DWD-derived bearer scope.
        candidates = [
            candidate
            for collection_name in ("user_drives", "shared_drive_content")
            for candidate in (state.get(collection_name) or {}).values()
            if isinstance(candidate, Mapping)
        ]
        return candidates[0] if len(candidates) == 1 else state

    def _fixture_for_file(
        self,
        request: ProviderRequest,
        file_id: str,
    ) -> Mapping[str, Any]:
        selected = self._fixture(request)
        if self._contains_file(selected, file_id):
            return selected
        state = request.source_state
        for collection_name in ("user_drives", "shared_drive_content"):
            for fixture in (state.get(collection_name) or {}).values():
                if isinstance(fixture, Mapping) and self._contains_file(
                    fixture, file_id
                ):
                    return fixture
        return selected

    @staticmethod
    def _contains_file(fixture: Mapping[str, Any], file_id: str) -> bool:
        if file_id in (fixture.get("exports") or {}):
            return True
        return any(
            str(file.get("id")) == file_id
            for file in fixture.get("files", [])
            if isinstance(file, Mapping)
        )


class GrafanaAdapter(_WaveBAdapter):
    source = "grafana"
    routes = (
        ProviderRoute(
            "grafana.annotations",
            "/api/annotations",
            operation_ids=("annotations.list",),
            quota_bucket="http-api",
        ),
        ProviderRoute(
            "grafana.org",
            "/api/org",
            operation_ids=("org.get",),
            quota_bucket="http-api",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"annotations": [], "instances": {}}

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = request.headers.get("x-provider-lab-scope")
        if explicit:
            return explicit
        authorization = request.headers.get("authorization", "")
        scheme, separator, token = authorization.partition(" ")
        if separator and scheme.lower() == "bearer":
            for prefix in ("lab-grafana::", "spam-grafana::"):
                if token.startswith(prefix):
                    instance = token[len(prefix) :]
                    return instance.rstrip("/") or "global"
        return "global"

    @staticmethod
    def _fixture(request: ProviderRequest) -> Mapping[str, Any]:
        state = request.source_state
        instances = state.get("instances") or {}
        fixture = instances.get(request.scope)
        if isinstance(fixture, Mapping):
            return fixture
        candidates = [
            candidate
            for candidate in instances.values()
            if isinstance(candidate, Mapping)
        ]
        return candidates[0] if len(candidates) == 1 else state

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        fixture = self._fixture(request)
        if request.route.route_id == "grafana.org":
            return ProviderResponse.json(
                {
                    "id": fixture.get("org_id", 1),
                    "name": fixture.get("org_name", "Sandbox Org"),
                }
            )
        params = _params(request)

        def optional_int(name: str) -> int | None:
            value = params.get(name)
            if value is None or value == "":
                return None
            try:
                return int(value)
            except ValueError:
                return None

        from_ms = optional_int("from")
        to_ms = optional_int("to")
        limit = optional_int("limit") or 100
        rows = [
            item
            for item in fixture.get("annotations", [])
            if (from_ms is None or int(item.get("time", 0)) >= from_ms)
            and (to_ms is None or int(item.get("time", 0)) <= to_ms)
        ]
        rows.sort(key=lambda item: int(item.get("time", 0)), reverse=True)
        return ProviderResponse.json(rows[:limit])


class GustoAdapter(_WaveBAdapter):
    source = "gusto"
    routes = (
        ProviderRoute(
            "gusto.employees",
            "/v1/companies/{company_uuid}/employees",
            operation_ids=("employees.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "gusto.payrolls",
            "/v1/companies/{company_uuid}/payrolls",
            operation_ids=("payrolls.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "gusto.company",
            "/v1/companies/{company_uuid}",
            operation_ids=("companies.get",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "gusto.oauth_token",
            "/oauth/token",
            operation_ids=("oauth.token.refresh",),
            methods=("POST",),
            quota_bucket="oauth",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"employee": [], "payroll": []}

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        state = request.source_state
        route_id = request.route.route_id
        if route_id == "gusto.oauth_token":
            rejected = validate_lifecycle_refresh_grant(request)
            if rejected is not None:
                return rejected
            lifecycle = lifecycle_token_response(
                request,
                token_type="Bearer",
                include_refresh_token=True,
            )
            if lifecycle is not None:
                return ProviderResponse.json(lifecycle)
            return ProviderResponse.json(
                {
                    "access_token": "lab-gusto-access-token",
                    "refresh_token": "lab-gusto-refresh-token",
                    "expires_in": 7200,
                    "token_type": "Bearer",
                }
            )
        rejected = require_lifecycle_access_token(request)
        if rejected is not None:
            return rejected
        company_uuid = str(request.path_params["company_uuid"])
        if route_id == "gusto.company":
            company = state.get("company")
            if not isinstance(company, dict):
                company = {
                    "uuid": company_uuid,
                    "name": "Sandbox Co",
                    "company_status": "Approved",
                }
            return ProviderResponse.json(company)

        kind = "employee" if route_id == "gusto.employees" else "payroll"
        rows = list(state.get(kind, []))
        params = _params(request)
        if kind == "payroll":
            start_date = params.get("start_date")
            end_date = params.get("end_date")
            if start_date:
                rows = [
                    row
                    for row in rows
                    if str(row.get("check_date") or "") >= start_date
                ]
            if end_date:
                rows = [
                    row for row in rows if str(row.get("check_date") or "") <= end_date
                ]
        page = _integer(params.get("page"), 1, minimum=1)
        per = _integer(params.get("per"), 25, minimum=1, maximum=100)
        total = len(rows)
        selected = rows[(page - 1) * per : page * per]
        return ProviderResponse.json(
            selected,
            headers={
                "X-Total-Count": str(total),
                "X-Page": str(page),
                "X-Per-Page": str(per),
                "X-Total-Pages": str(max(1, math.ceil(total / per))),
            },
        )


_JIRA_PROJECT_RE = re.compile(r'project\s*=\s*"([^"]+)"', re.IGNORECASE)


class JiraAdapter(_WaveBAdapter):
    source = "jira"
    routes = (
        ProviderRoute(
            "jira.search",
            "/rest/api/3/search/jql",
            operation_ids=("issues.search",),
            methods=("POST",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "jira.approximate_count",
            "/rest/api/3/search/approximate-count",
            operation_ids=("issues.approximate_count",),
            methods=("POST",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "jira.project_search",
            "/rest/api/3/project/search",
            operation_ids=("projects.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "jira.myself",
            "/rest/api/3/myself",
            operation_ids=("users.myself.get",),
            quota_bucket="rest",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"projects": {}}

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        route_id = request.route.route_id
        if route_id == "jira.myself":
            return ProviderResponse.json(
                {
                    "accountId": "sandbox-account",
                    "emailAddress": "sandbox@acme.example",
                    "displayName": "Sandbox Bot",
                }
            )
        if route_id == "jira.project_search":
            params = _params(request)
            start = _integer(params.get("startAt"), 0)
            maximum = _integer(params.get("maxResults"), 50)
            keys = sorted((request.source_state.get("projects") or {}).keys())
            projects = [
                {"id": str(1000 + index), "key": key, "name": f"{key} project"}
                for index, key in enumerate(keys)
            ]
            page = projects[start : start + maximum]
            return ProviderResponse.json(
                {
                    "startAt": start,
                    "maxResults": maximum,
                    "total": len(projects),
                    "isLast": start + len(page) >= len(projects),
                    "values": page,
                }
            )
        try:
            body = request.json()
        except (ValueError, TypeError):
            body = {}
        if not isinstance(body, dict):
            body = {}
        pool = self._pool(request.source_state, str(body.get("jql") or ""))
        if route_id == "jira.approximate_count":
            return ProviderResponse.json({"count": len(pool)})
        maximum = _integer(body.get("maxResults"), 50)
        offset = _integer(body.get("nextPageToken"), 0)
        page = pool[offset : offset + maximum]
        next_offset = offset + len(page)
        last = next_offset >= len(pool)
        response: dict[str, Any] = {"issues": page, "isLast": last}
        if not last:
            response["nextPageToken"] = str(next_offset)
        return ProviderResponse.json(response)

    @staticmethod
    def _pool(state: Mapping[str, Any], jql: str) -> list[dict[str, Any]]:
        match = _JIRA_PROJECT_RE.search(jql)
        project = match.group(1) if match else None
        fixture = (state.get("projects") or {}).get(project) if project else None
        if not isinstance(fixture, dict):
            return []
        incremental = "updated >" in jql.lower() or "updated>" in jql.lower()
        return list(fixture.get("delta" if incremental else "issues", []))


class MercuryAdapter(_WaveBAdapter):
    source = "mercury"
    routes = (
        ProviderRoute(
            "mercury.accounts",
            "/accounts",
            operation_ids=("accounts.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "mercury.account_transactions",
            "/account/{account_id}/transactions",
            operation_ids=("transactions.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "mercury.account",
            "/account/{account_id}",
            operation_ids=("accounts.get",),
            quota_bucket="rest",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"accounts": {}}

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        fixtures = request.source_state.get("accounts") or {}
        route_id = request.route.route_id
        if route_id == "mercury.accounts":
            accounts = [
                fixture.get("account") or {"id": account_id}
                for account_id, fixture in fixtures.items()
                if isinstance(fixture, dict)
            ]
            return ProviderResponse.json({"accounts": accounts, "total": len(accounts)})
        account_id = str(request.path_params["account_id"])
        fixture = fixtures.get(account_id)
        if route_id == "mercury.account":
            if not isinstance(fixture, dict):
                return ProviderResponse.json(
                    {"error": f"no account {account_id}"}, status_code=404
                )
            return ProviderResponse.json(fixture.get("account") or {"id": account_id})
        params = _params(request)
        if not isinstance(fixture, dict):
            rows = []
        else:
            rows = list(fixture.get("transactions", []))
            start = params.get("start")
            if start:
                floor = str(start)[:10]
                rows = [
                    row
                    for row in rows
                    if isinstance(row, dict) and _mercury_transaction_date(row) >= floor
                ]
        limit = _integer(params.get("limit"), 100)
        offset = _integer(params.get("offset"), 0)
        return ProviderResponse.json(
            {
                "transactions": rows[offset : offset + limit],
                "total": len(rows),
            }
        )


class MiroAdapter(_WaveBAdapter):
    source = "miro"
    routes = (
        ProviderRoute(
            "miro.boards",
            "/boards",
            operation_ids=("boards.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "miro.board_items",
            "/boards/{board_id}/items",
            operation_ids=("board_items.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "miro.board",
            "/boards/{board_id}",
            operation_ids=("boards.get",),
            quota_bucket="rest",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"boards": {}}

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        fixtures = request.source_state.get("boards") or {}
        route_id = request.route.route_id
        if route_id == "miro.boards":
            boards = [
                fixture.get("board") or {"id": board_id}
                for board_id, fixture in fixtures.items()
                if isinstance(fixture, dict)
            ]
            return ProviderResponse.json({"data": boards, "total": len(boards)})
        board_id = str(request.path_params["board_id"])
        fixture = fixtures.get(board_id)
        if route_id == "miro.board":
            if not isinstance(fixture, dict):
                return ProviderResponse.json(
                    {"error": f"no board {board_id}"}, status_code=404
                )
            return ProviderResponse.json(fixture.get("board") or {"id": board_id})
        if not isinstance(fixture, dict):
            return ProviderResponse.json({"data": [], "total": 0})
        params = _params(request)
        cursor = params.get("cursor")
        if cursor and cursor.startswith("miro-cursor:"):
            offset = _integer(cursor[len("miro-cursor:") :], 0)
        else:
            offset = 0
        limit = _integer(params.get("limit"), 50)
        rows = list(fixture.get("items", []))
        page = rows[offset : offset + limit]
        next_offset = offset + len(page)
        body: dict[str, Any] = {"data": page, "total": len(rows)}
        if page and next_offset < len(rows):
            body["cursor"] = f"miro-cursor:{next_offset}"
        return ProviderResponse.json(body)


_QBO_FROM_RE = re.compile(r"\bFROM\s+(\w+)", re.IGNORECASE)
_QBO_START_RE = re.compile(r"STARTPOSITION\s+(\d+)", re.IGNORECASE)
_QBO_MAX_RE = re.compile(r"MAXRESULTS\s+(\d+)", re.IGNORECASE)
_QBO_INCREMENTAL_RE = re.compile(r"LastUpdatedTime\s*>", re.IGNORECASE)


class QuickBooksAdapter(_WaveBAdapter):
    source = "quickbooks"
    routes = (
        ProviderRoute(
            "quickbooks.query",
            "/v3/company/{realm_id}/query",
            operation_ids=("entities.query",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "quickbooks.company_info",
            "/v3/company/{realm_id}/companyinfo/{company_realm_id}",
            operation_ids=("company_info.get",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "quickbooks.oauth_token",
            "/oauth2/v1/tokens/bearer",
            operation_ids=("oauth.token.refresh",),
            methods=("POST",),
            quota_bucket="oauth",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"entities": {}}

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        if request.route.route_id == "quickbooks.oauth_token":
            rejected = validate_lifecycle_refresh_grant(request)
            if rejected is not None:
                return rejected
            lifecycle = lifecycle_token_response(
                request,
                token_type="bearer",
                include_refresh_token=True,
                refresh_expiry_field="x_refresh_token_expires_in",
            )
            if lifecycle is not None:
                return ProviderResponse.json(lifecycle)
            return ProviderResponse.json(
                {
                    "access_token": "lab-quickbooks-access-token",
                    "refresh_token": "lab-quickbooks-refresh-token",
                    "expires_in": 3600,
                    "x_refresh_token_expires_in": 8_726_400,
                    "token_type": "bearer",
                }
            )
        rejected = require_lifecycle_access_token(request)
        if rejected is not None:
            return rejected
        if request.route.route_id == "quickbooks.company_info":
            return ProviderResponse.json({"CompanyInfo": {"CompanyName": "Sandbox Co"}})
        sql = request.query_one("query", "") or ""
        match = _QBO_FROM_RE.search(sql)
        entity = match.group(1) if match else None
        fixture = (
            (request.source_state.get("entities") or {}).get(entity) if entity else None
        )
        if not isinstance(fixture, dict):
            return ProviderResponse.json(
                {
                    "QueryResponse": {
                        "startPosition": 1,
                        "maxResults": 0,
                    }
                }
            )
        pool = list(
            fixture.get("delta" if _QBO_INCREMENTAL_RE.search(sql) else "rows", [])
        )
        start_match = _QBO_START_RE.search(sql)
        max_match = _QBO_MAX_RE.search(sql)
        start = int(start_match.group(1)) if start_match else 1
        maximum = int(max_match.group(1)) if max_match else 100
        page = pool[start - 1 : start - 1 + maximum]
        return ProviderResponse.json(
            {
                "QueryResponse": {
                    entity: page,
                    "startPosition": start,
                    "maxResults": len(page),
                },
                "time": "2026-01-01T00:00:00.000-08:00",
            }
        )


_RAMP_RESOURCES = ("transactions", "reimbursements", "cards", "users")
_RAMP_WINDOW = {
    "transactions": "from_date",
    "reimbursements": "updated_after",
}


class RampAdapter(_WaveBAdapter):
    source = "ramp"
    routes = (
        ProviderRoute(
            "ramp.token",
            "/token",
            operation_ids=("oauth.token.mint",),
            methods=("POST",),
            quota_bucket=None,
        ),
        ProviderRoute(
            "ramp.business",
            "/business",
            operation_ids=("business.get",),
            quota_bucket="rest",
        ),
        *tuple(
            ProviderRoute(
                f"ramp.{resource}",
                f"/{resource}",
                operation_ids=(f"{resource}.list",),
                quota_bucket="rest",
            )
            for resource in _RAMP_RESOURCES
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {
            "business_id": "mock-ramp-business",
            "resources": {},
        }

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        route_id = request.route.route_id
        state = request.source_state
        if route_id == "ramp.token":
            rejected = validate_lifecycle_client_credentials(request)
            if rejected is not None:
                return rejected
            lifecycle = lifecycle_token_response(
                request,
                token_type="Bearer",
                extra={
                    "scope": (
                        "transactions:read reimbursements:read "
                        "cards:read users:read business:read"
                    )
                },
            )
            if lifecycle is not None:
                return ProviderResponse.json(lifecycle)
            return ProviderResponse.json(
                {
                    "access_token": "mock-ramp-access-token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": (
                        "transactions:read reimbursements:read "
                        "cards:read users:read business:read"
                    ),
                }
            )
        rejected = require_lifecycle_access_token(request)
        if rejected is not None:
            return rejected
        if route_id == "ramp.business":
            return ProviderResponse.json(
                {
                    "id": state.get("business_id") or "mock-ramp-business",
                    "business_name_legal": "Sandbox Co",
                    "business_name_on_card": "Sandbox Co",
                    "active": True,
                }
            )
        resource = route_id.split(".", 1)[1]
        params = _params(request)
        fixture = (state.get("resources") or {}).get(resource) or {
            "rows": [],
            "delta": [],
        }
        window = _RAMP_WINDOW.get(resource)
        rows = list(
            fixture.get("delta" if window and params.get(window) else "rows", [])
        )
        page_size = _integer(params.get("page_size"), 20, minimum=2, maximum=100)
        position = 0
        start = params.get("start")
        if start:
            for index, row in enumerate(rows):
                if str(row.get("id")) == start:
                    position = index + 1
                    break
        page = rows[position : position + page_size]
        next_url = None
        if page and len(page) == page_size and position + page_size < len(rows):
            next_params = dict(params)
            next_params["start"] = str(page[-1].get("id"))
            next_params["page_size"] = str(page_size)
            next_url = f"{request.url.split('?', 1)[0]}?{urlencode(next_params)}"
        return ProviderResponse.json({"data": page, "page": {"next": next_url}})


def wave_b_adapters() -> dict[str, Any]:
    """Return fresh stateful adapter instances for the Wave-B sources."""

    return {
        "brex": BrexAdapter(),
        "carta": CartaAdapter(),
        "deel": DeelAdapter(),
        "figma": FigmaAdapter(),
        "fireflies": FirefliesAdapter(),
        "google_calendar": GoogleCalendarAdapter(),
        "google_drive": GoogleDriveAdapter(),
        "grafana": GrafanaAdapter(),
        "gusto": GustoAdapter(),
        "jira": JiraAdapter(),
        "mercury": MercuryAdapter(),
        "miro": MiroAdapter(),
        "quickbooks": QuickBooksAdapter(),
        "ramp": RampAdapter(),
    }


def seed_wave_b_fixtures(
    fixtures: Mapping[str, list[Mapping[str, Any]]],
) -> dict[str, Mapping[str, Any]]:
    """Translate harness-generator and legacy mock-server fixture shapes."""

    seeded: dict[str, Mapping[str, Any]] = {}
    for source in wave_b_adapters():
        entries = fixtures.get(source)
        if not entries:
            continue
        converter = _SEEDERS[source]
        seeded[source] = converter(entries)
    return seeded


def _seed_brex(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    accounts: dict[str, Any] = {}
    for entry in entries:
        if isinstance(entry.get("accounts"), dict):
            for account_id, raw in entry["accounts"].items():
                account = copy.deepcopy(raw)
                transactions = account.pop("transactions", [])
                accounts[str(account_id)] = {
                    "account": account,
                    "transactions": transactions,
                    "delta": copy.deepcopy(raw.get("delta", [])),
                }
        else:
            accounts.update(copy.deepcopy(dict(entry)))
    return {"accounts": accounts}


def _seed_carta(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    return {
        "issuers": {
            str(entry.get("firm_id") or f"issuer-{index}"): copy.deepcopy(dict(entry))
            for index, entry in enumerate(entries)
        }
    }


def _seed_deel(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    contracts: dict[str, Any] = {}
    for entry in entries:
        if isinstance(entry.get("contracts"), dict):
            for contract_id, raw in entry["contracts"].items():
                contract = copy.deepcopy(raw)
                payments = contract.pop("payments", [])
                contracts[str(contract_id)] = {
                    "contract": contract,
                    "payments": payments,
                    "delta": copy.deepcopy(raw.get("delta", [])),
                }
        else:
            contracts.update(copy.deepcopy(dict(entry)))
    return {"contracts": contracts}


def _seed_figma(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    files: dict[str, Any] = {}
    for entry in entries:
        raw_files = entry.get("files")
        if isinstance(raw_files, dict):
            for file_key, raw in raw_files.items():
                file = copy.deepcopy(raw)
                events = file.pop("events", [])
                files[str(file_key)] = {
                    "file": file,
                    "events": events,
                    "delta": copy.deepcopy(raw.get("delta", [])),
                }
        else:
            files.update(copy.deepcopy(dict(entry)))
    return {"files": files}


def _seed_fireflies(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    return copy.deepcopy(dict(entries[0]))


def _seed_calendar(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    calendars: dict[str, Any] = {}
    for entry in entries:
        if isinstance(entry.get("events"), dict):
            delta = entry.get("delta") or {}
            for calendar_id, events in entry["events"].items():
                calendars[str(calendar_id)] = {
                    "events": copy.deepcopy(events),
                    "delta": copy.deepcopy(delta.get(calendar_id, [])),
                }
        else:
            calendars.update(copy.deepcopy(dict(entry)))
    return {"calendars": calendars}


def _seed_drive(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    files: list[Any] = []
    changes: list[Any] = []
    exports: dict[str, Any] = {}
    comments: dict[str, Any] = {}
    revisions: dict[str, Any] = {}
    shared_drives: list[Any] = []
    user_drives: dict[str, Any] = {}
    shared_drive_content: dict[str, Any] = {}
    start_token = "spt-1"
    new_token = "spt-2"

    def insert_unique(
        target: dict[str, Any],
        key: str,
        fixture: Mapping[str, Any],
        *,
        identity_kind: str,
    ) -> None:
        if key in target:
            raise ValueError(
                "google_drive fixtures contain duplicate "
                f"{identity_kind} identity {key!r}"
            )
        target[key] = _normalize_drive_fixture(fixture)

    for entry in entries:
        raw_user_drives = entry.get("drive_my")
        raw_shared_content = entry.get("drive_shared")
        if isinstance(raw_user_drives, Mapping) or isinstance(
            raw_shared_content, Mapping
        ):
            if isinstance(raw_user_drives, Mapping):
                for email, fixture in raw_user_drives.items():
                    if isinstance(fixture, Mapping):
                        insert_unique(
                            user_drives,
                            str(email).lower(),
                            fixture,
                            identity_kind="owner",
                        )
            if isinstance(raw_shared_content, Mapping):
                for drive_id, fixture in raw_shared_content.items():
                    if isinstance(fixture, Mapping):
                        insert_unique(
                            shared_drive_content,
                            str(drive_id),
                            fixture,
                            identity_kind="shared-drive",
                        )
            shared_drives.extend(copy.deepcopy(entry.get("shared_drives", [])))
            start_token = str(entry.get("start_page_token") or start_token)
            new_token = str(entry.get("new_start_page_token") or new_token)
            continue
        targets = entry.get("targets")
        sources = targets if isinstance(targets, list) else [entry]
        for target in sources:
            if not isinstance(target, dict):
                continue
            drive_id = target.get("drive_id")
            drive_kind = target.get("drive_kind") or (
                "shared_drive" if drive_id and drive_id != "my-drive" else "my_drive"
            )
            owner_email = target.get("owner_email")
            if drive_kind == "shared_drive" and drive_id:
                drive_key = str(drive_id)
                insert_unique(
                    shared_drive_content,
                    drive_key,
                    target,
                    identity_kind="shared-drive",
                )
                shared_drives.append(
                    {"id": drive_key, "name": target.get("name") or drive_key}
                )
            elif isinstance(owner_email, str) and owner_email:
                insert_unique(
                    user_drives,
                    owner_email.lower(),
                    target,
                    identity_kind="owner",
                )
            else:
                # Legacy flat fixtures predate target identity. Keep their
                # single-corpus behavior without allowing canonical target
                # fixtures to bleed into this global fallback.
                files.extend(copy.deepcopy(target.get("files", [])))
                changes.extend(copy.deepcopy(target.get("changes", [])))
                comments.update(copy.deepcopy(target.get("comments", {})))
                revisions.update(copy.deepcopy(target.get("revisions", {})))
                extracted = target.get("exports") or target.get("extracted_text") or {}
                for file_id, value in extracted.items():
                    if isinstance(value, bytes):
                        exports[file_id] = {
                            "base64": base64.b64encode(value).decode("ascii"),
                            "content_type": "application/pdf",
                        }
                    else:
                        exports[file_id] = copy.deepcopy(value)
            start_token = str(target.get("start_page_token") or start_token)
            new_token = str(target.get("new_start_page_token") or new_token)
    return {
        "files": files,
        "changes": changes,
        "exports": exports,
        "comments": comments,
        "revisions": revisions,
        "shared_drives": shared_drives,
        "user_drives": user_drives,
        "shared_drive_content": shared_drive_content,
        "start_page_token": start_token,
        "new_start_page_token": new_token,
    }


def _normalize_drive_fixture(fixture: Mapping[str, Any]) -> dict[str, Any]:
    normalized = copy.deepcopy(dict(fixture))
    exports: dict[str, Any] = {}
    extracted = fixture.get("exports") or fixture.get("extracted_text") or {}
    for file_id, value in extracted.items():
        if isinstance(value, bytes):
            exports[str(file_id)] = {
                "base64": base64.b64encode(value).decode("ascii"),
                "content_type": "application/pdf",
            }
        else:
            exports[str(file_id)] = copy.deepcopy(value)
    normalized["exports"] = exports
    normalized.setdefault("files", [])
    normalized.setdefault("changes", [])
    normalized.setdefault("comments", {})
    normalized.setdefault("revisions", {})
    return normalized


def _seed_grafana(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    annotations: list[Any] = []
    instances: dict[str, Any] = {}
    for entry in entries:
        if isinstance(entry, list):
            annotations.extend(copy.deepcopy(entry))
        elif "time" in entry and "annotations" not in entry:
            # Also accept Grafana's flat annotation fixture list.
            annotations.append(copy.deepcopy(dict(entry)))
        elif entry.get("base_url"):
            instance = str(entry["base_url"]).rstrip("/")
            if instance in instances:
                raise ValueError(
                    "grafana fixtures contain duplicate instance identity "
                    f"{instance!r}"
                )
            instances[instance] = copy.deepcopy(dict(entry))
        else:
            annotations.extend(copy.deepcopy(entry.get("annotations", [])))
    return {"annotations": annotations, "instances": instances}


def _seed_gusto(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    entry = entries[0]
    state = copy.deepcopy(dict(entry.get("entities") or entry))
    if isinstance(entry.get("company"), dict):
        state["company"] = copy.deepcopy(entry["company"])
    return state


def _seed_jira(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    projects: dict[str, Any] = {}
    for entry in entries:
        if isinstance(entry.get("projects"), list):
            for project in entry["projects"]:
                key = str(project["project_key"])
                projects[key] = {
                    "issues": copy.deepcopy(project.get("issues", [])),
                    "delta": copy.deepcopy(project.get("delta", [])),
                }
        else:
            projects.update(copy.deepcopy(dict(entry)))
    return {"projects": projects}


def _seed_mercury(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    return _seed_brex(entries)


def _mercury_transaction_date(transaction: Mapping[str, Any]) -> str:
    """Return Mercury's date-granular transaction timestamp for ``start``."""
    value = transaction.get("postedAt") or transaction.get("createdAt") or ""
    return str(value)[:10]


def _seed_miro(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    boards: dict[str, Any] = {}
    for entry in entries:
        if isinstance(entry.get("boards"), dict):
            for board_id, raw in entry["boards"].items():
                board = copy.deepcopy(raw)
                items = board.pop("items", [])
                boards[str(board_id)] = {
                    "board": board,
                    "items": items,
                    "delta": copy.deepcopy(raw.get("delta", [])),
                }
        else:
            boards.update(copy.deepcopy(dict(entry)))
    return {"boards": boards}


def _seed_quickbooks(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    entities: dict[str, Any] = {}
    for entry in entries:
        raw = (
            entry.get("entities") if isinstance(entry.get("entities"), dict) else entry
        )
        for entity, rows in raw.items():
            if isinstance(rows, dict) and ("rows" in rows or "delta" in rows):
                entities[str(entity)] = copy.deepcopy(rows)
            elif isinstance(rows, list):
                entities[str(entity)] = {"rows": copy.deepcopy(rows), "delta": []}
    return {"entities": entities}


def _seed_ramp(entries: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    entry = entries[0]
    raw = entry.get("entities") if isinstance(entry.get("entities"), dict) else entry
    singular_to_plural = {
        "transaction": "transactions",
        "reimbursement": "reimbursements",
        "card": "cards",
        "user": "users",
    }
    resources: dict[str, Any] = {}
    for name, value in raw.items():
        resource = singular_to_plural.get(str(name), str(name))
        if resource not in _RAMP_RESOURCES:
            continue
        if isinstance(value, dict):
            resources[resource] = copy.deepcopy(value)
        elif isinstance(value, list):
            resources[resource] = {
                "rows": copy.deepcopy(value),
                "delta": [],
            }
    return {
        "business_id": entry.get("business_id") or "mock-ramp-business",
        "resources": resources,
    }


_SEEDERS = {
    "brex": _seed_brex,
    "carta": _seed_carta,
    "deel": _seed_deel,
    "figma": _seed_figma,
    "fireflies": _seed_fireflies,
    "google_calendar": _seed_calendar,
    "google_drive": _seed_drive,
    "grafana": _seed_grafana,
    "gusto": _seed_gusto,
    "jira": _seed_jira,
    "mercury": _seed_mercury,
    "miro": _seed_miro,
    "quickbooks": _seed_quickbooks,
    "ramp": _seed_ramp,
}


__all__ = ["seed_wave_b_fixtures", "wave_b_adapters"]
