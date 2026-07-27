"""Wave-C/D Provider Lab adapters.

These adapters deliberately stop at the outbound or ingress surfaces Fyralis
currently uses:

* Notion, HiBob, Ashby, LinkedIn, AWS, and Facebook Pages expose their
  provider-shaped HTTP operations.
* Telegram exposes only the four operations wrapped by ``TelegramClient``.
  It is not a general MTProto server.
* Signal exposes only the pinned signal-cli JSON-RPC subscription/group
  operations and a finite SSE replay for subscription tests.  It does not
  invent a deep-history API.
* WhatsApp exposes the Meta webhook handshake/delivery contract only; Fyralis
  has no WhatsApp history or outbound Graph client today.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import re
import threading
from typing import Any, Mapping
from urllib.parse import parse_qs, unquote

from .protocol import (
    ProviderOperationBinding,
    ProviderProtocolSurface,
    ProviderRequest,
    ProviderResponse,
    ProviderRoute,
)


def _params(request: ProviderRequest) -> dict[str, str]:
    return {key: value for key, value in request.query_items}


def _explicit_scope(request: ProviderRequest) -> str | None:
    value = request.headers.get("x-provider-lab-scope")
    return value[:256] if value else None


def _authorization(request: ProviderRequest) -> tuple[str, str] | None:
    raw = request.headers.get("authorization")
    if not raw:
        return None
    scheme, separator, value = raw.partition(" ")
    if not separator or not value.strip():
        return None
    return scheme.lower(), value.strip()


def _bearer(request: ProviderRequest) -> str | None:
    authorization = _authorization(request)
    if authorization is None or authorization[0] != "bearer":
        return None
    return authorization[1]


def _session(request: ProviderRequest) -> str | None:
    authorization = _authorization(request)
    if authorization is None or authorization[0] != "session":
        return None
    return authorization[1]


def _basic(request: ProviderRequest) -> tuple[str, str] | None:
    authorization = _authorization(request)
    if authorization is None or authorization[0] != "basic":
        return None
    try:
        decoded = base64.b64decode(authorization[1], validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None
    username, separator, password = decoded.partition(":")
    if not separator:
        return None
    return username, password


def _json_object(request: ProviderRequest) -> dict[str, Any] | None:
    try:
        body = request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return body if isinstance(body, dict) else None


def _integer(
    value: Any,
    default: int,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    parsed = max(minimum, parsed)
    return min(maximum, parsed) if maximum is not None else parsed


def _decode_offset(value: Any) -> int:
    if value is None or value == "":
        return 0
    rendered = str(value)
    if rendered.startswith("off:"):
        rendered = rendered[4:]
    return _integer(rendered, 0)


def _page(
    rows: list[dict[str, Any]],
    *,
    cursor: Any,
    limit: Any,
    default_limit: int,
    maximum: int,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    start = _decode_offset(cursor)
    page_size = _integer(
        limit,
        default_limit,
        minimum=1,
        maximum=maximum,
    )
    page = rows[start : start + page_size]
    next_offset = start + len(page)
    has_more = bool(page) and next_offset < len(rows)
    return page, (f"off:{next_offset}" if has_more else None), has_more


def _unauthorized(message: str) -> ProviderResponse:
    return ProviderResponse.json(
        {"error": {"code": "unauthorized", "message": message}},
        status_code=401,
    )


def _bad_request(code: str, message: str) -> ProviderResponse:
    return ProviderResponse.json(
        {"error": {"code": code, "message": message}},
        status_code=400,
    )


def _only_value(values: Mapping[str, Any]) -> Mapping[str, Any] | None:
    if len(values) != 1:
        return None
    value = next(iter(values.values()))
    return value if isinstance(value, Mapping) else None


class _WaveCDAdapter:
    routes: tuple[ProviderRoute, ...]
    protocol_surfaces: tuple[ProviderProtocolSurface, ...] = ()

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        authorization = request.headers.get("authorization") or ""
        match = re.search(
            r"\bCredential=AKIDLAB([^/,\s]+)/\d{8}/([^/,\s]+)/",
            authorization,
        )
        if match:
            return f"{match.group(1)}::{match.group(2)}"
        return "global"


# ---------------------------------------------------------------------------
# Notion
# ---------------------------------------------------------------------------


class NotionAdapter(_WaveCDAdapter):
    source = "notion"
    routes = (
        ProviderRoute(
            "notion.oauth_token",
            "/v1/oauth/token",
            operation_ids=("oauth.token.exchange",),
            methods=("POST",),
            quota_bucket="oauth",
        ),
        ProviderRoute(
            "notion.search",
            "/v1/search",
            operation_ids=("search",),
            methods=("POST",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "notion.query_database",
            "/v1/databases/{database_id}/query",
            operation_ids=("databases.query",),
            methods=("POST",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "notion.block_children",
            "/v1/blocks/{block_id}/children",
            operation_ids=("blocks.children.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "notion.comments",
            "/v1/comments",
            operation_ids=("comments.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "notion.retrieve_page",
            "/v1/pages/{page_id}",
            operation_ids=("pages.retrieve",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "notion.bot_user",
            "/v1/users/me",
            operation_ids=("users.me",),
            quota_bucket="rest",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"workspaces": {}}

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        token = _bearer(request) or ""
        for prefix in ("lab-notion::", "spam-notion::"):
            if token.startswith(prefix):
                return token[len(prefix) :] or "global"
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        route_id = request.route.route_id
        if route_id == "notion.oauth_token":
            authorization = request.headers.get("authorization", "")
            if not authorization.lower().startswith("basic "):
                return _unauthorized(
                    "Notion OAuth token exchange requires Basic client auth",
                )
            body = _json_object(request)
            if body is None:
                return _bad_request("invalid_json", "Expected a JSON object")
            if body.get("grant_type") != "authorization_code" or not body.get("code"):
                return _bad_request(
                    "invalid_grant",
                    "authorization_code grant and code are required",
                )
            workspaces = request.source_state.get("workspaces") or {}
            workspace = _only_value(workspaces) or {}
            workspace_id = str(
                workspace.get("workspace_id") or "lab-workspace",
            )
            return ProviderResponse.json(
                {
                    "access_token": f"lab-notion::{workspace_id}",
                    "workspace_id": workspace_id,
                    "workspace_name": f"Provider Lab {workspace_id}",
                    "bot_id": f"bot-{workspace_id}",
                    "owner": {"type": "workspace", "workspace": True},
                },
            )

        if not _bearer(request):
            return _unauthorized("Notion requires a Bearer integration token")
        if not request.headers.get("notion-version"):
            return _bad_request(
                "missing_notion_version",
                "Notion-Version is required",
            )

        workspaces = request.source_state.get("workspaces") or {}
        workspace = self._workspace(workspaces, request.scope)
        if route_id == "notion.search":
            body = _json_object(request)
            if body is None:
                return _bad_request("invalid_json", "Expected a JSON object")
            object_filter = None
            filter_body = body.get("filter")
            if isinstance(filter_body, dict):
                object_filter = filter_body.get("value")
            rows = self._search_rows(workspace, workspaces, object_filter)
            sort = body.get("sort")
            if isinstance(sort, dict) and sort.get("direction") == "descending":
                rows.sort(key=_notion_edit, reverse=True)
            return ProviderResponse.json(
                _notion_list(
                    rows,
                    cursor=body.get("start_cursor"),
                    limit=body.get("page_size"),
                    maximum=self._page_size(workspace),
                )
            )

        if route_id == "notion.query_database":
            body = _json_object(request)
            if body is None:
                return _bad_request("invalid_json", "Expected a JSON object")
            database_id = str(request.path_params["database_id"])
            rows = self._database_rows(workspace, workspaces, database_id)
            if isinstance(body.get("sorts"), list):
                rows.sort(key=_notion_edit, reverse=True)
            return ProviderResponse.json(
                _notion_list(
                    rows,
                    cursor=body.get("start_cursor"),
                    limit=body.get("page_size"),
                    maximum=self._page_size(workspace),
                )
            )

        if route_id == "notion.block_children":
            block_id = str(request.path_params["block_id"])
            rows = self._by_object(
                workspace,
                workspaces,
                "blocks_by_page",
                block_id,
            )
            params = _params(request)
            return ProviderResponse.json(
                _notion_list(
                    rows,
                    cursor=params.get("start_cursor"),
                    limit=params.get("page_size"),
                    maximum=self._page_size(workspace),
                )
            )

        if route_id == "notion.comments":
            params = _params(request)
            block_id = params.get("block_id")
            if not block_id:
                return _bad_request("missing_block_id", "block_id is required")
            rows = self._by_object(
                workspace,
                workspaces,
                "comments_by_page",
                block_id,
            )
            return ProviderResponse.json(
                _notion_list(
                    rows,
                    cursor=params.get("start_cursor"),
                    limit=params.get("page_size"),
                    maximum=self._page_size(workspace),
                )
            )

        if route_id == "notion.retrieve_page":
            page_id = str(request.path_params["page_id"])
            page = self._find_page(workspace, workspaces, page_id)
            if page is None:
                return ProviderResponse.json(
                    {
                        "object": "error",
                        "status": 404,
                        "code": "object_not_found",
                        "message": "Page was not found",
                    },
                    status_code=404,
                )
            return ProviderResponse.json(page)

        if route_id == "notion.bot_user":
            workspace_id = (
                request.scope
                if request.scope != "global"
                else str(workspace.get("workspace_id") or "lab-workspace")
            )
            bot_user = workspace.get("bot_user")
            if isinstance(bot_user, dict):
                return ProviderResponse.json(bot_user)
            return ProviderResponse.json(
                {
                    "object": "user",
                    "id": f"bot-{workspace_id}",
                    "type": "bot",
                    "bot": {
                        "owner": {"type": "workspace", "workspace": True},
                        "workspace_name": f"Provider Lab {workspace_id}",
                    },
                }
            )
        raise RuntimeError(f"unhandled Notion route {route_id}")

    @staticmethod
    def _workspace(
        workspaces: Mapping[str, Any],
        scope: str,
    ) -> Mapping[str, Any]:
        scoped = workspaces.get(scope)
        if isinstance(scoped, Mapping):
            return scoped
        return _only_value(workspaces) or {}

    @staticmethod
    def _page_size(workspace: Mapping[str, Any]) -> int:
        return _integer(workspace.get("page_size"), 100, minimum=1, maximum=100)

    @staticmethod
    def _search_rows(
        workspace: Mapping[str, Any],
        workspaces: Mapping[str, Any],
        object_filter: Any,
    ) -> list[dict[str, Any]]:
        selected = (
            [workspace]
            if workspace
            else [item for item in workspaces.values() if isinstance(item, Mapping)]
        )
        rows: list[dict[str, Any]] = []
        for item in selected:
            if object_filter in (None, "database"):
                for database in item.get("databases") or []:
                    summary = database.get("object_summary")
                    if isinstance(summary, dict):
                        rows.append(copy.deepcopy(summary))
            if object_filter in (None, "page"):
                rows.extend(
                    copy.deepcopy(row)
                    for row in item.get("loose_pages") or []
                    if isinstance(row, dict)
                )
        return rows

    @staticmethod
    def _database_rows(
        workspace: Mapping[str, Any],
        workspaces: Mapping[str, Any],
        database_id: str,
    ) -> list[dict[str, Any]]:
        selected = (
            [workspace]
            if workspace
            else [item for item in workspaces.values() if isinstance(item, Mapping)]
        )
        for item in selected:
            for database in item.get("databases") or []:
                if str(database.get("database_id")) == database_id:
                    return [
                        copy.deepcopy(row)
                        for row in database.get("rows") or []
                        if isinstance(row, dict)
                    ]
        return []

    @staticmethod
    def _by_object(
        workspace: Mapping[str, Any],
        workspaces: Mapping[str, Any],
        collection: str,
        object_id: str,
    ) -> list[dict[str, Any]]:
        selected = (
            [workspace]
            if workspace
            else [item for item in workspaces.values() if isinstance(item, Mapping)]
        )
        for item in selected:
            values = item.get(collection)
            if isinstance(values, Mapping) and object_id in values:
                return [
                    copy.deepcopy(row)
                    for row in values.get(object_id) or []
                    if isinstance(row, dict)
                ]
        return []

    @staticmethod
    def _find_page(
        workspace: Mapping[str, Any],
        workspaces: Mapping[str, Any],
        page_id: str,
    ) -> dict[str, Any] | None:
        selected = (
            [workspace]
            if workspace
            else [item for item in workspaces.values() if isinstance(item, Mapping)]
        )
        for item in selected:
            pages = list(item.get("loose_pages") or [])
            for database in item.get("databases") or []:
                pages.extend(database.get("rows") or [])
            for page in pages:
                if isinstance(page, dict) and str(page.get("id")) == page_id:
                    return copy.deepcopy(page)
        return None


def _notion_edit(row: Mapping[str, Any]) -> str:
    return str(row.get("last_edited_time") or row.get("created_time") or "")


def _notion_list(
    rows: list[dict[str, Any]],
    *,
    cursor: Any,
    limit: Any,
    maximum: int,
) -> dict[str, Any]:
    page, next_cursor, has_more = _page(
        rows,
        cursor=cursor,
        limit=limit,
        default_limit=100,
        maximum=maximum,
    )
    return {
        "object": "list",
        "results": page,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


# ---------------------------------------------------------------------------
# HiBob
# ---------------------------------------------------------------------------


class HibobAdapter(_WaveCDAdapter):
    source = "hibob"
    routes = (
        ProviderRoute(
            "hibob.people_search",
            "/v1/people/search",
            operation_ids=("people.search",),
            methods=("POST",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "hibob.timeoff_changes",
            "/v1/timeoff/requests/changes",
            operation_ids=("timeoff.changes.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "hibob.salaries",
            "/v1/bulk/people/salaries",
            operation_ids=("people.salaries.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "hibob.work",
            "/v1/bulk/people/work",
            operation_ids=("people.work.list",),
            quota_bucket="rest",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"companies": {}}

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        credentials = _basic(request)
        if credentials:
            _service_user_id, token = credentials
            for prefix in ("lab-hibob::", "spam-hibob::"):
                if token.startswith(prefix):
                    company_id = token[len(prefix) :]
                    return company_id[:256] if company_id else "global"
            if _service_user_id:
                return _service_user_id[:256]
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        credentials = _basic(request)
        if credentials is None or not all(credentials):
            return _unauthorized(
                "HiBob requires Basic service_user_id:token authentication"
            )
        companies = request.source_state.get("companies") or {}
        company = companies.get(request.scope)
        if not isinstance(company, Mapping):
            company = _only_value(companies) or {}
        entities = company.get("entities") or {}
        route_id = request.route.route_id
        if route_id == "hibob.people_search":
            if _json_object(request) is None:
                return _bad_request("invalid_json", "Expected a JSON object")
            return ProviderResponse.json(
                {"employees": copy.deepcopy(list(entities.get("employee") or []))}
            )
        if route_id == "hibob.timeoff_changes":
            params = _params(request)
            rows = list(entities.get("timeoff") or [])
            since = params.get("since")
            to = params.get("to")
            if since:
                rows = [row for row in rows if _modified(row) > since]
            if to:
                rows = [row for row in rows if _modified(row) <= to]
            return ProviderResponse.json({"requests": copy.deepcopy(rows)})

        kind = "payroll" if route_id == "hibob.salaries" else "lifecycle"
        rows = [
            copy.deepcopy(row)
            for row in entities.get(kind) or []
            if isinstance(row, dict)
        ]
        params = _params(request)
        maximum = _integer(
            company.get("page_size"),
            100,
            minimum=1,
            maximum=200,
        )
        page, cursor, _has_more = _page(
            rows,
            cursor=params.get("cursor"),
            limit=params.get("limit"),
            default_limit=100,
            maximum=maximum,
        )
        return ProviderResponse.json(
            {
                "results": page,
                "response_metadata": {"next_cursor": cursor or ""},
            }
        )


def _modified(row: Mapping[str, Any]) -> str:
    for key in (
        "modified",
        "modifiedAt",
        "lastModified",
        "updatedAt",
        "updated",
    ):
        value = row.get(key)
        if isinstance(value, str):
            return value
    return ""


# ---------------------------------------------------------------------------
# Ashby
# ---------------------------------------------------------------------------


_ASHBY_METHODS: dict[str, str] = {
    "applicationFeedback": "application_feedback",
    "candidateTag": "candidate_tag",
    "feedbackFormDefinition": "feedback_form_definition",
    "interviewPlan": "interview_plan",
    "interviewSchedule": "interview_schedule",
    "interviewStageGroup": "interview_stage_group",
    "jobPosting": "job_posting",
    "sourceTrackingLink": "source_tracking_link",
    "surveyFormDefinition": "survey_form_definition",
    "surveyRequest": "survey_request",
}
_ASHBY_NONPAGINATED = {"interview_stage_group", "job_posting"}


class AshbyAdapter(_WaveCDAdapter):
    source = "ashby"
    routes = (
        ProviderRoute(
            "ashby.list",
            "/{method_name}.list",
            operation_ids=("entities.list",),
            methods=("POST",),
            quota_bucket="rpc",
        ),
        ProviderRoute(
            "ashby.info",
            "/{method_name}.info",
            operation_ids=("entities.info",),
            methods=("POST",),
            quota_bucket="rpc",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"organizations": {}}

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        credentials = _basic(request)
        token = credentials[0] if credentials else ""
        # Basic auth appends ``:`` as the empty-password separator, so a
        # provider-lab key cannot safely embed its scope with another colon.
        # Use a colon-free marker for deterministic multi-org client tests.
        for prefix in (
            "lab-ashby--",
            "spam-ashby--",
            "lab-ashby::",
            "spam-ashby::",
        ):
            if token.startswith(prefix):
                return token[len(prefix) :] or "global"
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        credentials = _basic(request)
        if credentials is None or not credentials[0] or credentials[1] != "":
            return _unauthorized(
                "Ashby requires Basic authentication with the API key as "
                "username and an empty password"
            )
        body = _json_object(request)
        if body is None:
            return _bad_request("invalid_json", "Expected a JSON object")
        method_name = str(request.path_params["method_name"])
        category = _ashby_category(method_name, body)
        organizations = request.source_state.get("organizations") or {}
        organization = organizations.get(request.scope)
        if not isinstance(organization, Mapping):
            organization = _only_value(organizations) or {}
        entities = organization.get("entities") or {}
        if category not in entities:
            return ProviderResponse.json(
                {
                    "success": False,
                    "errors": [
                        {
                            "code": "unknown_method",
                            "message": f"Unsupported Ashby RPC {method_name}",
                        }
                    ],
                },
                status_code=404,
            )
        rows = [
            copy.deepcopy(row)
            for row in entities.get(category) or []
            if isinstance(row, dict)
        ]
        if request.route.route_id == "ashby.info":
            entity_id = str(body.get("id") or "")
            for row in rows:
                if str(row.get("id") or row.get("Id") or "") == entity_id:
                    return ProviderResponse.json({"success": True, "results": row})
            return ProviderResponse.json(
                {
                    "success": False,
                    "errors": [{"code": "not_found", "message": "Entity not found"}],
                },
                status_code=404,
            )

        sync_token = body.get("syncToken")
        if isinstance(sync_token, str) and sync_token:
            rows = [row for row in rows if _ashby_updated(row) > sync_token]
        maximum = _integer(
            organization.get("page_size"),
            100,
            minimum=1,
            maximum=100,
        )
        if category in _ASHBY_NONPAGINATED:
            page = rows
            next_cursor = None
            has_more = False
        else:
            page, next_cursor, has_more = _page(
                rows,
                cursor=body.get("cursor"),
                limit=body.get("limit"),
                default_limit=100,
                maximum=maximum,
            )
        refreshed = max(
            (_ashby_updated(row) for row in rows),
            default=(sync_token if isinstance(sync_token, str) else ""),
        )
        response: dict[str, Any] = {
            "success": True,
            "results": page,
            "moreDataAvailable": has_more,
            "nextCursor": (
                next_cursor[4:]
                if next_cursor and next_cursor.startswith("off:")
                else next_cursor
            ),
        }
        # The production client declares these single-shot metadata surfaces
        # as non-incremental. Returning a token that the client can never send
        # back makes reconciliation interpret every full response as a gap.
        if refreshed and category not in _ASHBY_NONPAGINATED:
            response["syncToken"] = refreshed
        return ProviderResponse.json(response)


def _ashby_category(method_name: str, body: Mapping[str, Any]) -> str:
    if method_name == "surveySubmission":
        if body.get("surveyType") == "Questionnaire":
            return "survey_submission_questionnaire"
        return "survey_submission_candidate_experience"
    return _ASHBY_METHODS.get(method_name, method_name)


def _ashby_updated(row: Mapping[str, Any]) -> str:
    for key in ("updatedAt", "updated_at", "createdAt", "created_at"):
        value = row.get(key)
        if isinstance(value, str):
            return value
    return ""


# ---------------------------------------------------------------------------
# LinkedIn
# ---------------------------------------------------------------------------


class LinkedinAdapter(_WaveCDAdapter):
    source = "linkedin"
    routes = (
        ProviderRoute(
            "linkedin.posts",
            "/posts",
            operation_ids=("posts.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "linkedin.share_statistics",
            "/organizationalEntityShareStatistics",
            operation_ids=("share_statistics.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "linkedin.follower_statistics",
            "/organizationalEntityFollowerStatistics",
            operation_ids=("follower_statistics.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "linkedin.organization",
            "/organizations/{organization_id}",
            operation_ids=("organizations.get",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "linkedin.oauth_token",
            "/oauth/v2/accessToken",
            operation_ids=("oauth.token.refresh",),
            methods=("POST",),
            quota_bucket="oauth",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"organizations": {}}

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        params = _params(request)
        organization = params.get("author") or params.get("organizationalEntity")
        if organization:
            return unquote(organization)[:256]
        organization_id = request.path_params.get("organization_id")
        if organization_id:
            return str(organization_id)[:256]
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        if request.route.route_id == "linkedin.oauth_token":
            return ProviderResponse.json(
                {
                    "access_token": "lab-linkedin-access-token",
                    "refresh_token": "lab-linkedin-refresh-token",
                    "expires_in": 3600,
                    "refresh_token_expires_in": 31_536_000,
                }
            )
        if not _bearer(request):
            return _unauthorized("LinkedIn requires a Bearer access token")
        version = request.headers.get("linkedin-version")
        restli = request.headers.get("x-restli-protocol-version")
        if not version or not re.fullmatch(r"\d{6}", version):
            return _bad_request(
                "invalid_linkedin_version",
                "LinkedIn-Version must be YYYYMM",
            )
        if restli != "2.0.0":
            return _bad_request(
                "invalid_restli_version",
                "X-Restli-Protocol-Version must be 2.0.0",
            )

        params = _params(request)
        organizations = request.source_state.get("organizations") or {}
        organization = _linkedin_organization(
            organizations,
            request.scope,
            request.path_params.get("organization_id"),
        )
        entities = organization.get("entities") or {}
        route_id = request.route.route_id
        if route_id == "linkedin.posts":
            if params.get("q") != "author" or not params.get("author"):
                return _bad_request(
                    "invalid_finder",
                    "posts requires q=author and author",
                )
            rows = sorted(
                (
                    copy.deepcopy(row)
                    for row in entities.get("post") or []
                    if isinstance(row, dict)
                ),
                key=lambda row: int(
                    row.get("lastModifiedAt") or row.get("createdAt") or 0
                ),
                reverse=True,
            )
            start = _integer(params.get("start"), 0)
            maximum = _integer(
                organization.get("page_size"),
                100,
                minimum=1,
                maximum=100,
            )
            count = _integer(
                params.get("count"),
                100,
                minimum=1,
                maximum=maximum,
            )
            page = rows[start : start + count]
            links: list[dict[str, Any]] = []
            if page and start + len(page) < len(rows):
                next_start = start + len(page)
                links.append(
                    {
                        "rel": "next",
                        "href": (
                            f"{request.path}?q=author&start={next_start}"
                            f"&count={count}"
                        ),
                    }
                )
            return ProviderResponse.json(
                {
                    "elements": page,
                    "paging": {
                        "start": start,
                        "count": count,
                        "total": len(rows),
                        "links": links,
                    },
                }
            )

        if route_id in {
            "linkedin.share_statistics",
            "linkedin.follower_statistics",
        }:
            kind = (
                "share_statistics"
                if route_id == "linkedin.share_statistics"
                else "follower_statistics"
            )
            rows = [
                copy.deepcopy(row)
                for row in entities.get(kind) or []
                if isinstance(row, dict)
            ]
            start_ms, end_ms = _linkedin_interval(params.get("timeIntervals"))
            rows = [row for row in rows if _in_linkedin_window(row, start_ms, end_ms)]
            return ProviderResponse.json({"elements": rows})

        organization_id = str(request.path_params["organization_id"])
        configured = organization.get("organization")
        if isinstance(configured, dict):
            return ProviderResponse.json(copy.deepcopy(configured))
        return ProviderResponse.json(
            {
                "id": organization_id,
                "localizedName": f"Provider Lab Organization {organization_id}",
                "vanityName": f"provider-lab-{organization_id}",
                "$URN": f"urn:li:organization:{organization_id}",
            }
        )


def _linkedin_organization(
    organizations: Mapping[str, Any],
    scope: str,
    path_id: Any,
) -> Mapping[str, Any]:
    candidates = [scope]
    if scope.startswith("urn:li:organization:"):
        candidates.append(scope.rpartition(":")[2])
    elif scope != "global":
        candidates.append(f"urn:li:organization:{scope}")
    if path_id is not None:
        candidates.extend([str(path_id), f"urn:li:organization:{path_id}"])
    for candidate in candidates:
        value = organizations.get(candidate)
        if isinstance(value, Mapping):
            return value
    return _only_value(organizations) or {}


def _linkedin_interval(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    decoded = unquote(value)
    start = re.search(r"start:(\d+)", decoded)
    end = re.search(r"end:(\d+)", decoded)
    return (
        int(start.group(1)) if start else None,
        int(end.group(1)) if end else None,
    )


def _in_linkedin_window(
    row: Mapping[str, Any],
    start_ms: int | None,
    end_ms: int | None,
) -> bool:
    time_range = row.get("timeRange")
    bucket = time_range.get("start") if isinstance(time_range, Mapping) else None
    if start_ms is not None and (not isinstance(bucket, int) or bucket < start_ms):
        return False
    return not (end_ms is not None and isinstance(bucket, int) and bucket >= end_ms)


# ---------------------------------------------------------------------------
# AWS JSON/Query protocols
# ---------------------------------------------------------------------------


class AwsAdapter(_WaveCDAdapter):
    source = "aws"
    routes = (
        ProviderRoute(
            "aws.service_operation",
            "/",
            operation_ids=(
                "sts.get_caller_identity",
                "cloudtrail.lookup_events",
                "sts.assume_role",
            ),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="sts.get_caller_identity",
                    method="POST",
                    headers=(
                        (
                            "Authorization",
                            "AWS4-HMAC-SHA256 Credential=AKIDLABprovider/"
                            "20250101/us-east-1/sts/aws4_request, "
                            "SignedHeaders=host;x-amz-date, "
                            "Signature=provider-lab",
                        ),
                        (
                            "Content-Type",
                            "application/x-www-form-urlencoded",
                        ),
                    ),
                    body=b"Action=GetCallerIdentity&Version=2011-06-15",
                ),
                ProviderOperationBinding(
                    operation_id="cloudtrail.lookup_events",
                    method="POST",
                    headers=(
                        (
                            "Authorization",
                            "AWS4-HMAC-SHA256 Credential=AKIDLABprovider/"
                            "20250101/us-east-1/cloudtrail/aws4_request, "
                            "SignedHeaders=host;x-amz-date;x-amz-target, "
                            "Signature=provider-lab",
                        ),
                        ("Content-Type", "application/x-amz-json-1.1"),
                        (
                            "X-Amz-Target",
                            "com.amazonaws.cloudtrail.v20131101."
                            "CloudTrail_20131101.LookupEvents",
                        ),
                    ),
                    body=b'{"MaxResults":50}',
                ),
                ProviderOperationBinding(
                    operation_id="sts.assume_role",
                    method="POST",
                    headers=(
                        (
                            "Authorization",
                            "AWS4-HMAC-SHA256 Credential=AKIDLABprovider/"
                            "20250101/us-east-1/sts/aws4_request, "
                            "SignedHeaders=host;x-amz-date, "
                            "Signature=provider-lab",
                        ),
                        (
                            "Content-Type",
                            "application/x-www-form-urlencoded",
                        ),
                    ),
                    body=(
                        b"Action=AssumeRole&Version=2011-06-15"
                        b"&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000"
                        b"%3Arole%2Fprovider-lab"
                        b"&RoleSessionName=fyralis-ingest"
                    ),
                ),
            ),
            methods=("POST",),
            quota_bucket="service-api",
            transport="aws_sigv4",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"accounts": {}}

    def resolve_scope(self, request: ProviderRequest) -> str:
        # Real CloudTrail requests carry their account/region identity in the
        # SigV4 Credential scope. Reuse the base parser so multiple seeded AWS
        # accounts cannot collapse to the Lab's global fallback.
        return super().resolve_scope(request)

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        authorization = request.headers.get("authorization") or ""
        if not authorization.startswith("AWS4-HMAC-SHA256 "):
            return ProviderResponse.json(
                {
                    "__type": "UnrecognizedClientException",
                    "message": "A structurally valid SigV4 Authorization is required",
                },
                status_code=403,
            )
        accounts = request.source_state.get("accounts") or {}
        account = accounts.get(request.scope)
        if not isinstance(account, Mapping):
            account = _only_value(accounts) or {}

        target = request.headers.get("x-amz-target") or ""
        if target.endswith(".LookupEvents"):
            body = _json_object(request)
            if body is None:
                return _bad_request("invalid_json", "Expected AWS JSON payload")
            rows = [
                copy.deepcopy(row)
                for row in account.get("events") or []
                if isinstance(row, dict)
            ]
            start_ms = _aws_time_ms(body.get("StartTime"))
            end_ms = _aws_time_ms(body.get("EndTime"))
            rows = [row for row in rows if _aws_in_window(row, start_ms, end_ms)]
            rows.sort(key=_aws_event_ms, reverse=True)
            maximum = _integer(
                account.get("per_page"),
                50,
                minimum=1,
                maximum=50,
            )
            page, cursor, _has_more = _page(
                rows,
                cursor=body.get("NextToken"),
                limit=body.get("MaxResults"),
                default_limit=50,
                maximum=maximum,
            )
            response: dict[str, Any] = {
                "Events": [_aws_wire_event(row) for row in page]
            }
            if cursor:
                response["NextToken"] = cursor
            return ProviderResponse.json(response)

        form = parse_qs(request.body.decode("utf-8", errors="replace"))
        if form.get("Action") == ["AssumeRole"]:
            account_id = str(account.get("account_id") or "000000000000")
            role_arn = str(
                (form.get("RoleArn") or [""])[0]
                or f"arn:aws:iam::{account_id}:role/provider-lab"
            )
            role_name = role_arn.rpartition("/")[2] or "provider-lab"
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<AssumeRoleResponse "
                'xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
                "<AssumeRoleResult><Credentials>"
                "<AccessKeyId>ASIALABPROVIDER0001</AccessKeyId>"
                "<SecretAccessKey>provider-lab-assumed-secret</SecretAccessKey>"
                "<SessionToken>provider-lab-session-token</SessionToken>"
                "<Expiration>2030-01-01T00:00:00Z</Expiration>"
                "</Credentials><AssumedRoleUser>"
                f"<Arn>arn:aws:sts::{account_id}:assumed-role/"
                f"{role_name}/fyralis-ingest</Arn>"
                "<AssumedRoleId>AROALAB:provider-lab</AssumedRoleId>"
                "</AssumedRoleUser></AssumeRoleResult>"
                "<ResponseMetadata><RequestId>provider-lab</RequestId>"
                "</ResponseMetadata></AssumeRoleResponse>"
            )
            return ProviderResponse(
                raw_body=xml.encode("utf-8"),
                media_type="text/xml",
            )
        if form.get("Action") == ["GetCallerIdentity"]:
            account_id = str(account.get("account_id") or "000000000000")
            arn = str(
                account.get("arn")
                or f"arn:aws:sts::{account_id}:assumed-role/provider-lab"
            )
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<GetCallerIdentityResponse "
                'xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
                "<GetCallerIdentityResult>"
                f"<Arn>{arn}</Arn>"
                f"<UserId>provider-lab:{account_id}</UserId>"
                f"<Account>{account_id}</Account>"
                "</GetCallerIdentityResult>"
                "<ResponseMetadata><RequestId>provider-lab</RequestId>"
                "</ResponseMetadata></GetCallerIdentityResponse>"
            )
            return ProviderResponse(
                raw_body=xml.encode("utf-8"),
                media_type="text/xml",
            )
        return ProviderResponse.json(
            {
                "__type": "UnknownOperationException",
                "message": "Only CloudTrail LookupEvents and STS AssumeRole/"
                "GetCallerIdentity are implemented",
            },
            status_code=400,
        )


def _aws_time_ms(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value if value >= 1_000_000_000_000 else value * 1000)


def _aws_event_ms(event: Mapping[str, Any]) -> int:
    value = event.get("eventTime")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    return 0


def _aws_in_window(
    event: Mapping[str, Any],
    start_ms: int | None,
    end_ms: int | None,
) -> bool:
    stamp = _aws_event_ms(event)
    if start_ms is not None and stamp < start_ms:
        return False
    return end_ms is None or stamp <= end_ms


def _aws_wire_event(event: Mapping[str, Any]) -> dict[str, Any]:
    cloud_trail = event.get("cloudTrailEvent")
    if not isinstance(cloud_trail, str):
        cloud_trail = json.dumps(cloud_trail or {}, separators=(",", ":"))
    wire = {
        "EventId": event.get("eventId"),
        "EventName": event.get("eventName"),
        "EventSource": event.get("eventSource"),
        # AWS JSON timestamps are epoch seconds on the wire.
        "EventTime": _aws_event_ms(event) / 1000,
        "Username": (
            (event.get("userIdentity") or {}).get("userName")
            if isinstance(event.get("userIdentity"), Mapping)
            else None
        ),
        "CloudTrailEvent": cloud_trail,
    }
    return {key: value for key, value in wire.items() if value is not None}


# ---------------------------------------------------------------------------
# Telegram transport operations (not a general MTProto server)
# ---------------------------------------------------------------------------


class TelegramAdapter(_WaveCDAdapter):
    source = "telegram"
    protocol_surfaces = (
        ProviderProtocolSurface(
            "telegram.session_transport",
            transport="injected_transport",
            operation_ids=(
                "session.connect",
                "session.is_user_authorized",
            ),
        ),
        ProviderProtocolSurface(
            "telegram.gateway_transport",
            transport="injected_transport",
            operation_ids=(
                "gateway.connect",
                "gateway.is_user_authorized",
                "updates.catch_up",
                "updates.get_state",
            ),
        ),
    )
    routes = (
        ProviderRoute(
            "telegram.get_history",
            "/transport/get_history",
            operation_ids=("get_history",),
            methods=("POST",),
            quota_bucket="transport",
        ),
        ProviderRoute(
            "telegram.iter_dialogs",
            "/transport/iter_dialogs",
            operation_ids=("iter_dialogs",),
            methods=("POST",),
            quota_bucket="transport",
        ),
        ProviderRoute(
            "telegram.has_history_since",
            "/transport/has_history_since",
            operation_ids=("has_history_since",),
            methods=("POST",),
            quota_bucket="transport",
        ),
        ProviderRoute(
            "telegram.me",
            "/transport/me",
            operation_ids=("me",),
            methods=("POST",),
            quota_bucket="transport",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {
            "dialogs": {},
            "dialog_order": [],
            "page_size": 100,
            "identity": {
                "id": 1,
                "username": "provider_lab",
                "phone": None,
            },
        }

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        session = _session(request) or ""
        for prefix in ("lab-telegram::", "spam-telegram::"):
            if session.startswith(prefix):
                return session[len(prefix) :] or "global"
        digest = hashlib.sha256(session.encode("utf-8")).hexdigest()
        return f"session:{digest[:16]}" if session else "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        if not _session(request):
            return _unauthorized(
                "Telegram transport requires the linked Session credential"
            )
        body = _json_object(request)
        if body is None:
            return _bad_request("invalid_json", "Expected a JSON object")
        state = request.source_state
        route_id = request.route.route_id
        if route_id == "telegram.iter_dialogs":
            limit = _integer(body.get("limit"), 200, minimum=1, maximum=200)
            dialogs = state.get("dialogs") or {}
            out = []
            for dialog_id in state.get("dialog_order") or []:
                dialog = dialogs.get(str(dialog_id))
                if not isinstance(dialog, Mapping):
                    continue
                out.append(
                    {
                        "dialog_id": dialog.get("dialog_id"),
                        "dialog_kind": dialog.get("dialog_kind", "chat"),
                        "access_hash": dialog.get("access_hash"),
                        "title": dialog.get("title"),
                    }
                )
            return ProviderResponse.json({"dialogs": out[:limit]})
        if route_id == "telegram.me":
            return ProviderResponse.json(
                copy.deepcopy(dict(state.get("identity") or {}))
            )

        dialog_id = _integer(body.get("dialog_id"), -1, minimum=-1)
        dialog = (state.get("dialogs") or {}).get(str(dialog_id)) or {}
        messages = [
            copy.deepcopy(message)
            for message in dialog.get("messages") or []
            if isinstance(message, dict)
        ]
        min_id = _integer(body.get("min_id"), 0)
        if route_id == "telegram.has_history_since":
            return ProviderResponse.json(
                {
                    "has_history": any(
                        _integer(message.get("id"), 0) > min_id for message in messages
                    )
                }
            )

        offset_id = _integer(body.get("offset_id"), 0)
        maximum = _integer(
            state.get("page_size"),
            100,
            minimum=1,
            maximum=100,
        )
        limit = _integer(
            body.get("limit"),
            100,
            minimum=1,
            maximum=maximum,
        )
        candidates = [
            message
            for message in messages
            if (offset_id == 0 or _integer(message.get("id"), 0) < offset_id)
            and _integer(message.get("id"), 0) > min_id
        ]
        candidates.sort(
            key=lambda message: _integer(message.get("id"), 0),
            reverse=True,
        )
        page = candidates[:limit]
        next_offset_id = (
            min(_integer(message.get("id"), 0) for message in page) if page else None
        )
        return ProviderResponse.json(
            {
                "messages": page,
                "next_offset_id": next_offset_id,
                "is_last": len(candidates) <= limit,
            }
        )


# ---------------------------------------------------------------------------
# Signal pinned JSON-RPC + finite SSE replay
# ---------------------------------------------------------------------------


class SignalAdapter(_WaveCDAdapter):
    source = "signal"
    routes = (
        ProviderRoute(
            "signal.json_rpc",
            "/jsonrpc",
            operation_ids=(
                "list_groups",
                "receive_poll",
                "subscribe_receive",
                "unsubscribe_receive",
            ),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="list_groups",
                    method="POST",
                    headers=(
                        ("Authorization", "Session provider-lab"),
                        ("Content-Type", "application/json"),
                    ),
                    body=(
                        b'{"jsonrpc":"2.0","id":"list-groups",'
                        b'"method":"listGroups","params":{}}'
                    ),
                ),
                ProviderOperationBinding(
                    operation_id="receive_poll",
                    method="POST",
                    headers=(
                        ("Authorization", "Session provider-lab"),
                        ("Content-Type", "application/json"),
                    ),
                    body=(
                        b'{"jsonrpc":"2.0","id":"receive",'
                        b'"method":"receive","params":{}}'
                    ),
                ),
                ProviderOperationBinding(
                    operation_id="subscribe_receive",
                    method="POST",
                    headers=(
                        ("Authorization", "Session provider-lab"),
                        ("Content-Type", "application/json"),
                    ),
                    body=(
                        b'{"jsonrpc":"2.0","id":"subscribe",'
                        b'"method":"subscribeReceive","params":{}}'
                    ),
                ),
                ProviderOperationBinding(
                    operation_id="unsubscribe_receive",
                    method="POST",
                    headers=(
                        ("Authorization", "Session provider-lab"),
                        ("Content-Type", "application/json"),
                    ),
                    body=(
                        b'{"jsonrpc":"2.0","id":"unsubscribe",'
                        b'"method":"unsubscribeReceive","params":'
                        b'{"subscription":"nonexistent"}}'
                    ),
                ),
            ),
            methods=("POST",),
            quota_bucket="json-rpc",
            transport="json_rpc",
        ),
        ProviderRoute(
            "signal.subscription_events",
            "/events/{subscription_id}",
            operation_ids=("events_stream",),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="events_stream",
                    method="GET",
                    path_values=(("subscription_id", "sub-1"),),
                    headers=(("Authorization", "Session provider-lab"),),
                ),
            ),
            quota_bucket="json-rpc",
            transport="sse",
        ),
    )

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._subscriptions: dict[str, str] = {}
        self._next_subscription = 1

    def reset(self) -> None:
        with self._lock:
            self._subscriptions.clear()
            self._next_subscription = 1

    def default_state(self) -> Mapping[str, Any]:
        return {
            "threads": {},
            "thread_order": [],
            "events": [],
        }

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        session = _session(request) or ""
        for prefix in ("lab-signal::", "spam-signal::"):
            if session.startswith(prefix):
                return session[len(prefix) :] or "global"
        digest = hashlib.sha256(session.encode("utf-8")).hexdigest()
        return f"session:{digest[:16]}" if session else "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        if not _session(request):
            return _unauthorized(
                "Signal JSON-RPC requires the linked-device Session credential"
            )
        if request.route.route_id == "signal.subscription_events":
            subscription_id = str(request.path_params["subscription_id"])
            with self._lock:
                owner = self._subscriptions.get(subscription_id)
            if owner != request.scope:
                return ProviderResponse.json(
                    {
                        "error": {
                            "code": "subscription_not_found",
                            "message": "Unknown subscription",
                        }
                    },
                    status_code=404,
                )
            payload = _signal_sse(request.source_state)
            return ProviderResponse(
                raw_body=payload,
                headers={"Cache-Control": "no-cache"},
                media_type="text/event-stream",
            )

        body = _json_object(request)
        if body is None or body.get("jsonrpc") != "2.0":
            return _signal_rpc_error(None, -32600, "Invalid Request")
        request_id = body.get("id")
        method = body.get("method")
        params = body.get("params")
        params = params if isinstance(params, dict) else {}
        if method == "listGroups":
            groups = []
            threads = request.source_state.get("threads") or {}
            for thread_id in request.source_state.get("thread_order") or []:
                thread = threads.get(str(thread_id))
                if not isinstance(thread, Mapping):
                    continue
                if thread.get("thread_kind", "direct") != "group":
                    continue
                groups.append(
                    {
                        "id": str(thread.get("thread_id")),
                        "name": thread.get("title"),
                    }
                )
            return _signal_rpc_result(request_id, groups)
        if method == "subscribeReceive":
            with self._lock:
                subscription_id = f"sub-{self._next_subscription}"
                self._next_subscription += 1
                self._subscriptions[subscription_id] = request.scope
            return _signal_rpc_result(
                request_id,
                {"subscription": subscription_id},
            )
        if method == "unsubscribeReceive":
            subscription_id = str(
                params.get("subscription") or params.get("subscriptionId") or ""
            )
            with self._lock:
                removed = self._subscriptions.get(subscription_id) == request.scope
                if removed:
                    self._subscriptions.pop(subscription_id, None)
            return _signal_rpc_result(request_id, removed)
        if method == "receive":
            return _signal_rpc_result(
                request_id,
                _signal_events(request.source_state),
            )
        return _signal_rpc_error(
            request_id,
            -32601,
            "Method not found; Provider Lab pins listGroups, "
            "subscribeReceive, unsubscribeReceive, and receive only",
        )


def _signal_rpc_result(request_id: Any, result: Any) -> ProviderResponse:
    return ProviderResponse.json({"jsonrpc": "2.0", "id": request_id, "result": result})


def _signal_rpc_error(
    request_id: Any,
    code: int,
    message: str,
) -> ProviderResponse:
    return ProviderResponse.json(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }
    )


def _signal_events(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    configured = state.get("events")
    if isinstance(configured, list) and configured:
        return [copy.deepcopy(item) for item in configured if isinstance(item, dict)]
    events: list[dict[str, Any]] = []
    threads = state.get("threads") or {}
    for thread_id in state.get("thread_order") or []:
        thread = threads.get(str(thread_id))
        if not isinstance(thread, Mapping):
            continue
        for message in thread.get("messages") or []:
            if not isinstance(message, Mapping):
                continue
            stamp_ms = _integer(message.get("date"), 0) * 1000
            data_message: dict[str, Any] = {
                "timestamp": stamp_ms,
                "message": message.get("message", ""),
            }
            if thread.get("thread_kind") == "group":
                data_message["groupInfo"] = {"groupId": str(thread_id)}
            sender = message.get("from_id")
            source = sender.get("user_id") if isinstance(sender, Mapping) else None
            if thread.get("thread_kind") != "group":
                # For a direct conversation signal-cli identifies the thread
                # by the remote peer. Keep that identity equal to the seeded
                # planner thread id; otherwise the production client correctly
                # caches the message under a different conversation and the
                # requested shard observes an empty page.
                source = thread_id
            events.append(
                {
                    "envelope": {
                        "timestamp": stamp_ms,
                        "sourceUuid": str(source or ""),
                        **(
                            {"sourceName": str(message["sender_username"])}
                            if message.get("sender_username")
                            else {}
                        ),
                        "dataMessage": data_message,
                    }
                }
            )
    return events


def _signal_sse(state: Mapping[str, Any]) -> bytes:
    chunks = []
    for event in _signal_events(state):
        chunks.append(
            "event: receive\n"
            + "data: "
            + json.dumps(event, separators=(",", ":"), sort_keys=True)
            + "\n\n"
        )
    return "".join(chunks).encode("utf-8")


# ---------------------------------------------------------------------------
# Meta webhook and Facebook Graph surfaces
# ---------------------------------------------------------------------------


class WhatsappAdapter(_WaveCDAdapter):
    source = "whatsapp"
    routes = (
        ProviderRoute(
            "whatsapp.webhook_verify",
            "/webhook",
            quota_bucket=None,
        ),
        ProviderRoute(
            "whatsapp.webhook_delivery",
            "/webhook",
            methods=("POST",),
            quota_bucket="webhook",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {
            "verify_tokens": ["provider-lab-verify"],
            "app_secrets": {"global": "provider-lab-secret"},
            "installations": {},
        }

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        body = _json_object(request)
        if body is not None:
            phone_number_id = _whatsapp_phone_number_id(body)
            if phone_number_id:
                return phone_number_id[:256]
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        state = request.source_state
        if request.route.route_id == "whatsapp.webhook_verify":
            return _meta_verify(request, state)
        body = _json_object(request)
        if body is None:
            return ProviderResponse.json(
                {"status": "bad_json"},
                status_code=400,
            )
        if body.get("object") != "whatsapp_business_account":
            return ProviderResponse.json(
                {"status": "bad_payload"},
                status_code=400,
            )
        phone_number_id = _whatsapp_phone_number_id(body)
        if not phone_number_id:
            return ProviderResponse.json(
                {"status": "no_phone_number_id"},
                status_code=400,
            )
        installation = (state.get("installations") or {}).get(phone_number_id)
        if isinstance(installation, Mapping) and not installation.get(
            "enabled",
            True,
        ):
            return ProviderResponse.json(
                {
                    "status": "ignored",
                    "reason": "unknown_or_disabled_installation",
                }
            )
        secret = _meta_secret(state, phone_number_id)
        if secret and not _valid_meta_signature(request, secret):
            return ProviderResponse.json(
                {"status": "signature_invalid"},
                status_code=401,
            )
        messages = 0
        statuses = 0
        for entry in body.get("entry") or []:
            if not isinstance(entry, Mapping):
                continue
            for change in entry.get("changes") or []:
                value = change.get("value") if isinstance(change, Mapping) else None
                if not isinstance(value, Mapping):
                    continue
                messages += sum(
                    isinstance(item, Mapping) for item in value.get("messages") or []
                )
                statuses += sum(
                    isinstance(item, Mapping) for item in value.get("statuses") or []
                )
        return ProviderResponse.json(
            {
                "status": "accepted",
                "phone_number_id": phone_number_id,
                "messages": messages,
                "statuses": statuses,
            }
        )


class FacebookPagesAdapter(_WaveCDAdapter):
    source = "facebook_pages"
    routes = (
        ProviderRoute(
            "facebook_pages.oauth_token",
            "/{version}/oauth/access_token",
            operation_ids=(
                "oauth.token.exchange",
                "oauth.user_token.extend",
            ),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="oauth.token.exchange",
                    method="GET",
                    path_values=(("version", "v23.0"),),
                    query_items=(
                        ("client_id", "provider-lab"),
                        ("client_secret", "provider-lab"),
                        (
                            "redirect_uri",
                            "https://provider-lab.test/facebook/callback",
                        ),
                        ("code", "provider-lab-code"),
                    ),
                ),
                ProviderOperationBinding(
                    operation_id="oauth.user_token.extend",
                    method="GET",
                    path_values=(("version", "v23.0"),),
                    query_items=(
                        ("grant_type", "fb_exchange_token"),
                        ("client_id", "provider-lab"),
                        ("client_secret", "provider-lab"),
                        ("fb_exchange_token", "provider-lab-short-user-token"),
                    ),
                ),
            ),
            quota_bucket=None,
        ),
        ProviderRoute(
            "facebook_pages.accounts",
            "/{version}/me/accounts",
            operation_ids=("pages.list",),
            quota_bucket="graph",
        ),
        ProviderRoute(
            "facebook_pages.subscribe",
            "/{version}/{page_id}/subscribed_apps",
            operation_ids=("pages.subscribe",),
            methods=("POST",),
            quota_bucket="graph",
        ),
        ProviderRoute(
            "facebook_pages.conversations",
            "/{version}/{page_id}/conversations",
            operation_ids=("conversations.list",),
            quota_bucket="graph",
        ),
        ProviderRoute(
            "facebook_pages.messages",
            "/{version}/{conversation_id}/messages",
            operation_ids=("messages.list",),
            quota_bucket="graph",
        ),
        ProviderRoute(
            "facebook_pages.webhook_verify",
            "/webhook",
            quota_bucket=None,
        ),
        ProviderRoute(
            "facebook_pages.webhook_delivery",
            "/webhook",
            methods=("POST",),
            quota_bucket="webhook",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {
            "pages": {},
            "user_pages": {},
            "conversations": {},
            "messages": {},
            "verify_tokens": ["provider-lab-verify"],
            "app_secrets": {"global": "provider-lab-secret"},
            "installations": {},
        }

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        token_scope = _facebook_page_scope_from_token(
            _params(request).get("access_token"),
        )
        if token_scope:
            return token_scope
        page_id = request.path_params.get("page_id")
        if page_id:
            return str(page_id)[:256]
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        route_id = request.route.route_id
        state = request.source_state
        if route_id == "facebook_pages.webhook_verify":
            return _meta_verify(request, state)
        if route_id == "facebook_pages.webhook_delivery":
            return self._webhook_delivery(request, state)

        version = str(request.path_params["version"])
        if not re.fullmatch(r"v\d+\.\d+", version):
            return _bad_request(
                "unsupported_graph_version",
                "Expected a versioned Meta Graph path",
            )
        params = _params(request)
        if route_id == "facebook_pages.oauth_token":
            is_extension = params.get("grant_type") == "fb_exchange_token"
            required = (
                ("client_id", "client_secret", "fb_exchange_token")
                if is_extension
                else ("client_id", "client_secret", "redirect_uri", "code")
            )
            if any(not params.get(key) for key in required):
                return _bad_request(
                    "invalid_oauth_request",
                    "Required Facebook Login token parameters are missing",
                )
            return ProviderResponse.json(
                {
                    "access_token": (
                        f"lab-facebook-long-user::{params['client_id']}"
                        if is_extension
                        else f"lab-facebook-short-user::{params['client_id']}"
                    ),
                    "token_type": "bearer",
                    "expires_in": 5_184_000,
                }
            )
        if not params.get("access_token"):
            return _unauthorized("Meta Graph requires access_token")
        token_page_id = _facebook_page_scope_from_token(
            params["access_token"],
        )

        if route_id == "facebook_pages.accounts":
            configured = state.get("user_pages") or {}
            rows = configured.get(params["access_token"])
            if not isinstance(rows, list):
                if token_page_id:
                    page = (state.get("pages") or {}).get(token_page_id)
                    rows = [copy.deepcopy(page)] if isinstance(page, dict) else []
                else:
                    rows = [
                        copy.deepcopy(page)
                        for page in (state.get("pages") or {}).values()
                        if isinstance(page, dict)
                    ]
            return ProviderResponse.json(_graph_page(rows, params, default_limit=100))
        if route_id == "facebook_pages.subscribe":
            page_id = str(request.path_params["page_id"])
            if token_page_id and token_page_id != page_id:
                return ProviderResponse.json(
                    {"error": {"code": 10, "message": "Page token scope mismatch"}},
                    status_code=403,
                )
            if page_id not in (state.get("pages") or {}):
                return ProviderResponse.json(
                    {
                        "error": {
                            "code": 100,
                            "message": "Unsupported or unknown Page",
                        }
                    },
                    status_code=404,
                )
            return ProviderResponse.json({"success": True})
        if route_id == "facebook_pages.conversations":
            page_id = str(request.path_params["page_id"])
            if token_page_id and token_page_id != page_id:
                return ProviderResponse.json(
                    {"error": {"code": 10, "message": "Page token scope mismatch"}},
                    status_code=403,
                )
            rows = (state.get("conversations") or {}).get(page_id, [])
            return ProviderResponse.json(_graph_page(rows, params, default_limit=100))
        conversation_id = str(request.path_params["conversation_id"])
        owner_page_id = _facebook_conversation_page(state, conversation_id)
        if (
            token_page_id
            and owner_page_id is not None
            and token_page_id != owner_page_id
        ):
            return ProviderResponse.json(
                {"error": {"code": 10, "message": "Page token scope mismatch"}},
                status_code=403,
            )
        rows = (state.get("messages") or {}).get(conversation_id, [])
        return ProviderResponse.json(_graph_page(rows, params, default_limit=100))

    @staticmethod
    def _webhook_delivery(
        request: ProviderRequest,
        state: Mapping[str, Any],
    ) -> ProviderResponse:
        body = _json_object(request)
        if body is None:
            return ProviderResponse.json(
                {"status": "bad_json"},
                status_code=400,
            )
        if body.get("object") != "page":
            return ProviderResponse.json(
                {"status": "bad_payload"},
                status_code=400,
            )
        page_id = _facebook_page_id(body)
        if not page_id:
            return ProviderResponse.json(
                {"status": "no_page_id"},
                status_code=400,
            )
        installation = (state.get("installations") or {}).get(page_id)
        if isinstance(installation, Mapping) and not installation.get(
            "enabled",
            True,
        ):
            return ProviderResponse.json(
                {
                    "status": "ignored",
                    "reason": "unknown_or_disabled_installation",
                }
            )
        secret = _meta_secret(state, page_id)
        if secret and not _valid_meta_signature(request, secret):
            return ProviderResponse.json(
                {"status": "signature_invalid"},
                status_code=401,
            )
        messages = 0
        for entry in body.get("entry") or []:
            if not isinstance(entry, Mapping):
                continue
            for messaging in entry.get("messaging") or []:
                if not isinstance(messaging, Mapping):
                    continue
                if isinstance(messaging.get("message"), Mapping) or isinstance(
                    messaging.get("postback"),
                    Mapping,
                ):
                    messages += 1
        return ProviderResponse.json(
            {
                "status": "accepted",
                "page_id": page_id,
                "messages": messages,
            }
        )


def _graph_page(
    rows: Any,
    params: Mapping[str, str],
    *,
    default_limit: int,
) -> dict[str, Any]:
    items = [copy.deepcopy(row) for row in rows or [] if isinstance(row, dict)]
    page, cursor, _has_more = _page(
        items,
        cursor=params.get("after"),
        limit=params.get("limit"),
        default_limit=default_limit,
        maximum=100,
    )
    body: dict[str, Any] = {"data": page}
    if cursor:
        body["paging"] = {
            "cursors": {"after": cursor},
            "next": f"?after={cursor}",
        }
    return body


def _meta_verify(
    request: ProviderRequest,
    state: Mapping[str, Any],
) -> ProviderResponse:
    params = _params(request)
    presented = params.get("hub.verify_token")
    configured = {
        str(value) for value in state.get("verify_tokens") or [] if value is not None
    }
    challenge = params.get("hub.challenge")
    if (
        params.get("hub.mode") == "subscribe"
        and presented in configured
        and challenge is not None
    ):
        return ProviderResponse(
            raw_body=challenge.encode("utf-8"),
            media_type="text/plain",
        )
    return ProviderResponse(
        status_code=403,
        raw_body=b"verification failed",
        media_type="text/plain",
    )


def _meta_secret(state: Mapping[str, Any], identity: str) -> str | None:
    secrets = state.get("app_secrets")
    if not isinstance(secrets, Mapping):
        return None
    value = secrets.get(identity) or secrets.get("global")
    return str(value) if value else None


def _valid_meta_signature(
    request: ProviderRequest,
    secret: str,
) -> bool:
    presented = request.headers.get("x-hub-signature-256") or ""
    expected = (
        "sha256="
        + hmac.new(
            secret.encode("utf-8"),
            request.body,
            hashlib.sha256,
        ).hexdigest()
    )
    return hmac.compare_digest(presented, expected)


def _whatsapp_phone_number_id(body: Mapping[str, Any]) -> str | None:
    for entry in body.get("entry") or []:
        if not isinstance(entry, Mapping):
            continue
        for change in entry.get("changes") or []:
            value = change.get("value") if isinstance(change, Mapping) else None
            metadata = value.get("metadata") if isinstance(value, Mapping) else None
            phone_number_id = (
                metadata.get("phone_number_id")
                if isinstance(metadata, Mapping)
                else None
            )
            if isinstance(phone_number_id, str) and phone_number_id:
                return phone_number_id
    return None


def _facebook_page_id(body: Mapping[str, Any]) -> str | None:
    for entry in body.get("entry") or []:
        if not isinstance(entry, Mapping):
            continue
        page_id = entry.get("id")
        if isinstance(page_id, str) and page_id:
            return page_id
    return None


def _facebook_page_scope_from_token(token: str | None) -> str | None:
    if not token:
        return None
    for prefix in (
        "lab-facebook-pages::",
        "spam-facebook-pages::",
    ):
        if token.startswith(prefix):
            page_id = token[len(prefix) :]
            return page_id[:256] if page_id else None
    return None


def _facebook_conversation_page(
    state: Mapping[str, Any],
    conversation_id: str,
) -> str | None:
    conversations = state.get("conversations") or {}
    if not isinstance(conversations, Mapping):
        return None
    for page_id, rows in conversations.items():
        if not isinstance(rows, list):
            continue
        if any(
            isinstance(row, Mapping) and str(row.get("id") or "") == conversation_id
            for row in rows
        ):
            return str(page_id)
    return None


# ---------------------------------------------------------------------------
# Registry + fixture translation
# ---------------------------------------------------------------------------


def wave_cd_adapters() -> dict[str, Any]:
    return {
        "ashby": AshbyAdapter(),
        "aws": AwsAdapter(),
        "facebook_pages": FacebookPagesAdapter(),
        "hibob": HibobAdapter(),
        "linkedin": LinkedinAdapter(),
        "notion": NotionAdapter(),
        "signal": SignalAdapter(),
        "telegram": TelegramAdapter(),
        "whatsapp": WhatsappAdapter(),
    }


def seed_wave_cd_fixtures(
    fixtures: Mapping[str, list[Mapping[str, Any]]],
) -> dict[str, Mapping[str, Any]]:
    seeded: dict[str, Mapping[str, Any]] = {}
    if fixtures.get("notion"):
        seeded["notion"] = {
            "workspaces": {
                str(entry.get("workspace_id") or f"workspace-{index}"): copy.deepcopy(
                    dict(entry)
                )
                for index, entry in enumerate(fixtures["notion"])
            }
        }
    if fixtures.get("hibob"):
        seeded["hibob"] = {
            "companies": {
                str(entry.get("company_id") or f"company-{index}"): copy.deepcopy(
                    dict(entry)
                )
                for index, entry in enumerate(fixtures["hibob"])
            }
        }
    if fixtures.get("ashby"):
        seeded["ashby"] = {
            "organizations": {
                str(entry.get("org_id") or f"organization-{index}"): copy.deepcopy(
                    dict(entry)
                )
                for index, entry in enumerate(fixtures["ashby"])
            }
        }
    if fixtures.get("linkedin"):
        seeded["linkedin"] = {
            "organizations": {
                str(
                    entry.get("organization_urn") or f"organization-{index}"
                ): copy.deepcopy(dict(entry))
                for index, entry in enumerate(fixtures["linkedin"])
            }
        }
    if fixtures.get("aws"):
        seeded["aws"] = {
            "accounts": {
                (
                    f"{entry.get('account_id')}::{entry.get('region')}"
                    if len(fixtures["aws"]) > 1
                    else "global"
                ): copy.deepcopy(dict(entry))
                for entry in fixtures["aws"]
            }
        }
    if fixtures.get("telegram"):
        seeded["telegram"] = _merge_gateway_fixtures(
            fixtures["telegram"],
            collection="dialogs",
            order="dialog_order",
            default_page_size=100,
        )
        seeded["telegram"]["identity"] = {
            "id": 1,
            "username": "provider_lab",
            "phone": None,
        }
    if fixtures.get("signal"):
        seeded["signal"] = _merge_gateway_fixtures(
            fixtures["signal"],
            collection="threads",
            order="thread_order",
            default_page_size=100,
        )
        seeded["signal"]["events"] = []
    if fixtures.get("whatsapp"):
        seeded["whatsapp"] = copy.deepcopy(dict(fixtures["whatsapp"][0]))
    if fixtures.get("facebook_pages"):
        seeded["facebook_pages"] = _merge_facebook_pages_fixtures(
            fixtures["facebook_pages"],
        )
    return seeded


def _merge_facebook_pages_fixtures(
    entries: list[Mapping[str, Any]],
) -> dict[str, Any]:
    """Merge independently scoped Page fixtures without sibling overwrite."""

    merged: dict[str, Any] = {
        "pages": {},
        "user_pages": {},
        "conversations": {},
        "messages": {},
        "verify_tokens": [],
        "app_secrets": {},
        "installations": {},
        "page_size": 100,
    }
    for index, entry in enumerate(entries):
        for field in ("pages", "conversations", "messages", "installations"):
            values = entry.get(field) or {}
            if not isinstance(values, Mapping):
                raise ValueError(
                    f"facebook_pages fixture {index} field {field!r} "
                    "must be a mapping"
                )
            target = merged[field]
            duplicates = set(target).intersection(str(key) for key in values)
            if duplicates:
                raise ValueError(
                    "facebook_pages fixtures contain duplicate "
                    f"{field} ids: {sorted(duplicates)!r}"
                )
            target.update(
                {str(key): copy.deepcopy(value) for key, value in values.items()}
            )

        user_pages = entry.get("user_pages") or {}
        if not isinstance(user_pages, Mapping):
            raise ValueError(
                f"facebook_pages fixture {index} field 'user_pages' "
                "must be a mapping"
            )
        for token, raw_pages in user_pages.items():
            if not isinstance(raw_pages, list):
                raise ValueError("facebook_pages user_pages entries must be lists")
            target_pages = merged["user_pages"].setdefault(str(token), [])
            existing_ids = {
                str(page.get("id"))
                for page in target_pages
                if isinstance(page, Mapping) and page.get("id") is not None
            }
            for page in raw_pages:
                if not isinstance(page, Mapping):
                    raise ValueError("facebook_pages user_pages must contain mappings")
                page_id = str(page.get("id") or "")
                if page_id and page_id in existing_ids:
                    raise ValueError(
                        "facebook_pages fixtures contain duplicate user-page "
                        f"id {page_id!r} for token {token!r}"
                    )
                target_pages.append(copy.deepcopy(dict(page)))
                if page_id:
                    existing_ids.add(page_id)

        for token in entry.get("verify_tokens") or []:
            if token not in merged["verify_tokens"]:
                merged["verify_tokens"].append(copy.deepcopy(token))

        secrets = entry.get("app_secrets") or {}
        if not isinstance(secrets, Mapping):
            raise ValueError(
                f"facebook_pages fixture {index} field 'app_secrets' "
                "must be a mapping"
            )
        for identity, secret in secrets.items():
            key = str(identity)
            if key in merged["app_secrets"] and merged["app_secrets"][key] != secret:
                raise ValueError(
                    "facebook_pages fixtures contain conflicting app secret "
                    f"for {key!r}"
                )
            merged["app_secrets"][key] = copy.deepcopy(secret)

        merged["page_size"] = min(
            merged["page_size"],
            _integer(entry.get("page_size"), 100, minimum=1),
        )
    return merged


def _merge_gateway_fixtures(
    entries: list[Mapping[str, Any]],
    *,
    collection: str,
    order: str,
    default_page_size: int,
) -> dict[str, Any]:
    merged: dict[str, Any] = {
        collection: {},
        order: [],
        "page_size": default_page_size,
    }
    for entry in entries:
        values = entry.get(collection)
        if isinstance(values, Mapping):
            merged[collection].update(copy.deepcopy(dict(values)))
        for value in entry.get(order) or []:
            if value not in merged[order]:
                merged[order].append(value)
        merged["page_size"] = min(
            _integer(entry.get("page_size"), default_page_size, minimum=1),
            merged["page_size"],
        )
    return merged


__all__ = ["seed_wave_cd_fixtures", "wave_cd_adapters"]
