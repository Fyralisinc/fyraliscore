"""Provider Lab adapters for all canonical sources.

Each registered source owns a finite provider-shaped route surface. Unknown
routes fail closed; the registry never invents a successful placeholder.
"""

from __future__ import annotations

import base64
import copy
import json
import threading
from typing import Any, Mapping
from urllib.parse import parse_qs

from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS

from .protocol import (
    AdapterRegistry,
    ProviderOperationBinding,
    ProviderProtocolSurface,
    ProviderRequest,
    ProviderResponse,
    ProviderRoute,
)
from .wave_b import seed_wave_b_fixtures, wave_b_adapters
from .wave_cd import seed_wave_cd_fixtures, wave_cd_adapters


def _bearer(headers: Mapping[str, str]) -> str | None:
    authorization = headers.get("authorization")
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() not in {"bearer", "bot", "token"}:
        return None
    return parts[1].strip()


def _explicit_scope(request: ProviderRequest) -> str | None:
    value = request.headers.get("x-provider-lab-scope")
    if value:
        return value[:256]
    return None


def _decode_jwt_sub(assertion: str) -> str | None:
    try:
        _header, payload_b64, _signature = assertion.split(".")
        padding = "=" * (-len(payload_b64) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(payload_b64 + padding).decode("utf-8")
        )
        sub = payload.get("sub")
        return str(sub) if sub else None
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return None


class SlackAdapter:
    source = "slack"
    protocol_surfaces: tuple[ProviderProtocolSurface, ...] = ()
    routes = (
        ProviderRoute(
            "slack.conversations_list",
            "/api/conversations.list",
            operation_ids=("conversations.list",),
            quota_bucket="web-api",
        ),
        ProviderRoute(
            "slack.conversations_history",
            "/api/conversations.history",
            operation_ids=("conversations.history",),
            quota_bucket="web-api",
        ),
        ProviderRoute(
            "slack.conversations_info",
            "/api/conversations.info",
            operation_ids=("conversations.info",),
            quota_bucket="web-api",
        ),
        ProviderRoute(
            "slack.users_info",
            "/api/users.info",
            operation_ids=("users.info",),
            quota_bucket="web-api",
        ),
        ProviderRoute(
            "slack.chat_post_message",
            "/api/chat.postMessage",
            operation_ids=("chat.postMessage",),
            methods=("POST",),
            quota_bucket="web-api",
        ),
        ProviderRoute(
            "slack.oauth_access",
            "/api/oauth.v2.access",
            operation_ids=("oauth.v2.access",),
            methods=("POST",),
            quota_bucket=None,
        ),
    )

    def __init__(self) -> None:
        self._posted_lock = threading.RLock()
        self._posted_messages: dict[str, list[dict[str, Any]]] = {}
        self._next_post = 1

    def reset(self) -> None:
        with self._posted_lock:
            self._posted_messages.clear()
            self._next_post = 1

    def default_state(self) -> Mapping[str, Any]:
        return {
            "teams": {},
            "messages": {},
            "direct_messages": {},
            "users": {},
        }

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        token = _bearer(request.headers) or ""
        for prefix in ("lab-slack::", "spam-slack::"):
            if token.startswith(prefix):
                return token[len(prefix) :] or "global"
        for prefix in ("lab-slack-user::", "spam-slack-user::"):
            if token.startswith(prefix):
                team, _, _user = token[len(prefix) :].partition("::")
                return team or "global"
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        state = request.source_state
        route_id = request.route.route_id
        if route_id == "slack.conversations_list":
            return self._conversations_list(request, state)
        if route_id == "slack.conversations_history":
            return self._conversations_history(request, state)
        if route_id == "slack.conversations_info":
            channel_id = request.query_one("channel", "") or ""
            channel = _find_slack_channel(state, channel_id)
            if channel is None:
                return ProviderResponse.json(
                    {"ok": False, "error": "channel_not_found"}
                )
            return ProviderResponse.json({"ok": True, "channel": channel})
        if route_id == "slack.users_info":
            user_id = request.query_one("user", "") or ""
            user = (state.get("users") or {}).get(user_id)
            if user is None:
                return ProviderResponse.json({"ok": False, "error": "user_not_found"})
            return ProviderResponse.json({"ok": True, "user": user})
        if route_id == "slack.chat_post_message":
            payload = request.json()
            if not isinstance(payload, Mapping):
                return ProviderResponse.json(
                    {"ok": False, "error": "invalid_arguments"}
                )
            channel_id = str(payload.get("channel") or "")
            text = str(payload.get("text") or "")
            if not channel_id or not text:
                return ProviderResponse.json(
                    {"ok": False, "error": "invalid_arguments"}
                )
            with self._posted_lock:
                ts = f"{self._next_post}.000000"
                self._next_post += 1
                message = {
                    "type": "message",
                    "channel": channel_id,
                    "text": text,
                    "ts": ts,
                }
                self._posted_messages.setdefault(
                    channel_id,
                    [],
                ).append(message)
            return ProviderResponse.json(
                {
                    "ok": True,
                    "channel": channel_id,
                    "ts": ts,
                    "message": message,
                }
            )
        if route_id == "slack.oauth_access":
            form = parse_qs(request.body.decode("utf-8", "replace"))
            code = (form.get("code") or ["provider-lab"])[0]
            team_id = f"T_{code[-12:].upper()}"
            return ProviderResponse.json(
                {
                    "ok": True,
                    "access_token": f"lab-slack::{team_id}",
                    "token_type": "bot",
                    "scope": "channels:read,channels:history",
                    "bot_user_id": "U_PROVIDER_LAB_BOT",
                    "app_id": "A_PROVIDER_LAB",
                    "team": {
                        "id": team_id,
                        "name": "Provider Lab",
                    },
                    "authed_user": {"id": "U_PROVIDER_LAB"},
                }
            )
        raise RuntimeError(f"unhandled Slack route {route_id}")

    def _conversations_list(
        self, request: ProviderRequest, state: Mapping[str, Any]
    ) -> ProviderResponse:
        token = _bearer(request.headers) or ""
        types = {
            value.strip()
            for value in (request.query_one("types", "public_channel") or "").split(",")
            if value.strip()
        }
        for prefix in ("lab-slack-user::", "spam-slack-user::"):
            if token.startswith(prefix) and types & {"im", "mpim"}:
                team, _, user = token[len(prefix) :].partition("::")
                key = f"{team}::{user}"
                conversations = list((state.get("direct_messages") or {}).get(key, []))
                filtered = [
                    conv
                    for conv in conversations
                    if ("im" in types and conv.get("is_im"))
                    or ("mpim" in types and conv.get("is_mpim"))
                ]
                return ProviderResponse.json({"ok": True, "channels": filtered})

        channels = list((state.get("teams") or {}).get(request.scope, []))
        if request.scope == "global":
            channels = [
                channel
                for team_channels in (state.get("teams") or {}).values()
                for channel in team_channels
            ]
        return ProviderResponse.json({"ok": True, "channels": channels})

    def _conversations_history(
        self, request: ProviderRequest, state: Mapping[str, Any]
    ) -> ProviderResponse:
        channel_id = request.query_one("channel", "") or ""
        messages = list((state.get("messages") or {}).get(channel_id, []))
        with self._posted_lock:
            messages.extend(
                copy.deepcopy(self._posted_messages.get(channel_id, []))
            )
        messages.sort(key=lambda item: float(item.get("ts", "0")), reverse=True)
        oldest = request.query_one("oldest")
        if oldest is not None:
            messages = [
                item for item in messages if float(item.get("ts", "0")) > float(oldest)
            ]
        cursor = int(request.query_one("cursor", "0") or "0")
        limit = max(1, min(1_000, int(request.query_one("limit", "100") or "100")))
        page = messages[cursor : cursor + limit]
        body: dict[str, Any] = {"ok": True, "messages": page}
        if cursor + limit < len(messages):
            body["response_metadata"] = {"next_cursor": str(cursor + limit)}
        return ProviderResponse.json(body)


def _find_slack_channel(
    state: Mapping[str, Any], channel_id: str
) -> Mapping[str, Any] | None:
    for channels in (state.get("teams") or {}).values():
        for channel in channels:
            if channel.get("id") == channel_id:
                return channel
    return None


def _github_event_operation_bindings(
    event_type: str,
) -> tuple[ProviderOperationBinding, ...]:
    return (
        ProviderOperationBinding(
            operation_id=f"repo_events.{event_type}.list",
            method="GET",
            query_items=(
                ("state", "all"),
                ("sort", "updated"),
                ("direction", "asc"),
                ("per_page", "30"),
                ("page", "1"),
            ),
        ),
        ProviderOperationBinding(
            operation_id=f"repo_events.{event_type}.head",
            method="GET",
            query_items=(
                ("state", "all"),
                ("sort", "updated"),
                ("direction", "desc"),
                ("per_page", "1"),
                ("page", "1"),
            ),
        ),
    )


class GithubAdapter:
    source = "github"
    protocol_surfaces: tuple[ProviderProtocolSurface, ...] = ()
    routes = (
        ProviderRoute(
            "github.installation_token",
            "/app/installations/{installation_id}/access_tokens",
            operation_ids=("installation_token.mint",),
            methods=("POST",),
            quota_bucket=None,
        ),
        ProviderRoute(
            "github.installation_repositories",
            "/installation/repositories",
            operation_ids=("installation_repositories.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "github.repo_issues",
            "/repos/{owner}/{repo}/issues",
            operation_ids=(
                "repo_events.issues.list",
                "repo_events.issues.head",
            ),
            operation_bindings=_github_event_operation_bindings("issues"),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "github.repo_pulls",
            "/repos/{owner}/{repo}/pulls",
            operation_ids=(
                "repo_events.pull_requests.list",
                "repo_events.pull_requests.head",
            ),
            operation_bindings=_github_event_operation_bindings(
                "pull_requests"
            ),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "github.repo_issue_comments",
            "/repos/{owner}/{repo}/issues/comments",
            operation_ids=(
                "repo_events.issue_comments.list",
                "repo_events.issue_comments.head",
            ),
            operation_bindings=_github_event_operation_bindings(
                "issue_comments"
            ),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "github.repo_commits",
            "/repos/{owner}/{repo}/commits",
            operation_ids=(
                "repo_events.commits.list",
                "repo_events.commits.head",
            ),
            operation_bindings=_github_event_operation_bindings("commits"),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "github.pull_reviews",
            "/repos/{owner}/{repo}/pulls/{pull_number}/reviews",
            operation_ids=("pull_reviews.list",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "github.check_runs",
            "/repos/{owner}/{repo}/commits/{ref}/check-runs",
            operation_ids=("check_runs.list",),
            quota_bucket="rest",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {"installations": {}, "repositories": {}}

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        installation_id = request.path_params.get("installation_id")
        if installation_id:
            return str(installation_id)
        token = _bearer(request.headers) or ""
        for prefix in ("lab-gh::", "spam-gh::"):
            if token.startswith(prefix):
                return token[len(prefix) :] or "global"
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        route_id = request.route.route_id
        if route_id == "github.installation_token":
            installation_id = str(request.path_params["installation_id"])
            return ProviderResponse.json(
                {
                    "token": f"lab-gh::{installation_id}",
                    "expires_at": "2099-01-01T00:00:00Z",
                },
                status_code=201,
            )
        if route_id == "github.installation_repositories":
            return self._repositories(request)
        event_types = {
            "github.repo_issues": "issues",
            "github.repo_pulls": "pull_requests",
            "github.repo_issue_comments": "issue_comments",
            "github.repo_commits": "commits",
        }
        if route_id in event_types:
            event_type = event_types[route_id]
            return self._repo_events(request, event_type)
        if route_id == "github.pull_reviews":
            return self._repo_child_events(
                request,
                event_type="pr_reviews",
                grouped_key="reviews_by_pull",
                parent_key="pull_number",
                parent_value=str(request.path_params["pull_number"]),
            )
        if route_id == "github.check_runs":
            response = self._repo_child_events(
                request,
                event_type="check_runs",
                grouped_key="check_runs_by_ref",
                parent_key="head_sha",
                parent_value=str(request.path_params["ref"]),
                wrapped_key="check_runs",
            )
            return response
        raise RuntimeError(f"unhandled GitHub route {route_id}")

    def _repositories(self, request: ProviderRequest) -> ProviderResponse:
        state = request.source_state
        names = list((state.get("installations") or {}).get(request.scope, []))
        per_page = max(1, min(100, int(request.query_one("per_page", "30") or "30")))
        page = max(1, int(request.query_one("page", "1") or "1"))
        start = (page - 1) * per_page
        selected = names[start : start + per_page]
        return ProviderResponse.json(
            {
                "total_count": len(names),
                "repository_selection": "selected",
                "repositories": [{"full_name": name} for name in selected],
            }
        )

    def _repo_events(
        self, request: ProviderRequest, event_type: str
    ) -> ProviderResponse:
        full_name = f"{request.path_params['owner']}/{request.path_params['repo']}"
        repo = (request.source_state.get("repositories") or {}).get(full_name, {})
        events = list((repo.get("events_by_type") or {}).get(event_type, []))
        return self._paginated_repo_response(
            request,
            rows=events,
            etag_key=f"{full_name}:{event_type}",
        )

    def _repo_child_events(
        self,
        request: ProviderRequest,
        *,
        event_type: str,
        grouped_key: str,
        parent_key: str,
        parent_value: str,
        wrapped_key: str | None = None,
    ) -> ProviderResponse:
        full_name = f"{request.path_params['owner']}/{request.path_params['repo']}"
        repo = (request.source_state.get("repositories") or {}).get(full_name, {})
        grouped = repo.get(grouped_key)
        if isinstance(grouped, Mapping):
            events = list(grouped.get(parent_value, []))
        else:
            events = [
                event
                for event in (repo.get("events_by_type") or {}).get(event_type, [])
                if str(event.get(parent_key) or "") == parent_value
            ]
        return self._paginated_repo_response(
            request,
            rows=events,
            etag_key=f"{full_name}:{event_type}:{parent_value}",
            wrapped_key=wrapped_key,
        )

    @staticmethod
    def _paginated_repo_response(
        request: ProviderRequest,
        *,
        rows: list[dict[str, Any]],
        etag_key: str,
        wrapped_key: str | None = None,
    ) -> ProviderResponse:
        etag = f'W/"{etag_key}:v{len(rows)}"'
        if request.headers.get("if-none-match") == etag:
            return ProviderResponse.empty(status_code=304, headers={"ETag": etag})

        per_page = max(1, min(100, int(request.query_one("per_page", "30") or "30")))
        page = max(1, int(request.query_one("page", "1") or "1"))
        start = (page - 1) * per_page
        end = start + per_page
        headers = {"ETag": etag}
        if end < len(rows):
            base = request.url.split("?", 1)[0]
            headers["Link"] = (
                f'<{base}?per_page={per_page}&page={page + 1}>; rel="next"'
            )
        selected = rows[start:end]
        body: Any = (
            {"total_count": len(rows), wrapped_key: selected}
            if wrapped_key is not None
            else selected
        )
        return ProviderResponse.json(body, headers=headers)


class GmailAdapter:
    source = "gmail"
    protocol_surfaces: tuple[ProviderProtocolSurface, ...] = ()
    routes = (
        ProviderRoute(
            "gmail.token",
            "/token",
            operation_ids=("dwd.token.exchange",),
            methods=("POST",),
            quota_bucket=None,
        ),
        ProviderRoute(
            "gmail.directory_users",
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
            "gmail.directory_groups",
            "/admin/directory/v1/groups",
            operation_ids=("directory.groups.list",),
            quota_bucket="directory-api",
        ),
        ProviderRoute(
            "gmail.directory_group_members",
            "/admin/directory/v1/groups/{group_key}/members",
            operation_ids=("directory.group_members.list",),
            quota_bucket="directory-api",
        ),
        ProviderRoute(
            "gmail.directory_org_units",
            "/admin/directory/v1/customer/{customer_id}/orgunits",
            operation_ids=("directory.org_units.list",),
            quota_bucket="directory-api",
        ),
        ProviderRoute(
            "gmail.profile",
            "/gmail/v1/users/me/profile",
            operation_ids=("profile.get",),
            quota_bucket="gmail-api",
        ),
        ProviderRoute(
            "gmail.messages_list",
            "/gmail/v1/users/me/messages",
            operation_ids=("messages.list",),
            quota_bucket="gmail-api",
        ),
        ProviderRoute(
            "gmail.message_get",
            "/gmail/v1/users/me/messages/{message_id}",
            operation_ids=("messages.get",),
            quota_bucket="gmail-api",
        ),
        ProviderRoute(
            "gmail.history_list",
            "/gmail/v1/users/me/history",
            operation_ids=("history.list",),
            quota_bucket="gmail-api",
        ),
        ProviderRoute(
            "gmail.watch",
            "/gmail/v1/users/me/watch",
            operation_ids=("watch.create",),
            methods=("POST",),
            quota_bucket="gmail-api",
        ),
        ProviderRoute(
            "gmail.stop",
            "/gmail/v1/users/me/stop",
            operation_ids=("watch.stop",),
            methods=("POST",),
            quota_bucket="gmail-api",
        ),
        ProviderRoute(
            "gmail.pubsub_topic",
            "/v1/projects/{project_id}/topics/{topic_id}",
            operation_ids=(
                "pubsub.topic.create",
                "pubsub.topic.delete",
            ),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="pubsub.topic.create",
                    method="PUT",
                    headers=(("Content-Type", "application/json"),),
                    body=b"{}",
                ),
                ProviderOperationBinding(
                    operation_id="pubsub.topic.delete",
                    method="DELETE",
                ),
            ),
            methods=("DELETE", "PUT"),
            quota_bucket="pubsub-admin",
        ),
        ProviderRoute(
            "gmail.pubsub_subscription",
            "/v1/projects/{project_id}/subscriptions/{subscription_id}",
            operation_ids=(
                "pubsub.subscription.create",
                "pubsub.subscription.delete",
            ),
            operation_bindings=(
                ProviderOperationBinding(
                    operation_id="pubsub.subscription.create",
                    method="PUT",
                    headers=(("Content-Type", "application/json"),),
                    body=b"{}",
                ),
                ProviderOperationBinding(
                    operation_id="pubsub.subscription.delete",
                    method="DELETE",
                ),
            ),
            methods=("DELETE", "PUT"),
            quota_bucket="pubsub-admin",
        ),
        ProviderRoute(
            "gmail.pubsub_iam_get",
            "/v1/projects/{project_id}/topics/{topic_id}:getIamPolicy",
            operation_ids=("pubsub.iam.get",),
            methods=("POST",),
            quota_bucket="pubsub-admin",
        ),
        ProviderRoute(
            "gmail.pubsub_iam_set",
            "/v1/projects/{project_id}/topics/{topic_id}:setIamPolicy",
            operation_ids=("pubsub.iam.set",),
            methods=("POST",),
            quota_bucket="pubsub-admin",
        ),
    )

    def __init__(self) -> None:
        self._pubsub_lock = threading.RLock()
        self._pubsub_iam_policies: dict[str, dict[str, Any]] = {}

    def reset(self) -> None:
        with self._pubsub_lock:
            self._pubsub_iam_policies.clear()

    def default_state(self) -> Mapping[str, Any]:
        return {
            "directory": {
                "domain": None,
                "users": [],
                "groups": [],
                "group_members": {},
                "org_units": [],
            },
            "mailboxes": {},
            "pubsub_iam_policy": {"bindings": []},
        }

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit.lower()
        if request.route.route_id == "gmail.token":
            form = parse_qs(request.body.decode("utf-8", "replace"))
            assertion = (form.get("assertion") or [""])[0]
            email = _decode_jwt_sub(assertion)
            if email:
                return email.lower()
        token = _bearer(request.headers) or ""
        for prefix in ("lab-gmail::", "spam::"):
            if token.startswith(prefix):
                return token[len(prefix) :].lower() or "global"
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        state = request.source_state
        route_id = request.route.route_id
        if route_id == "gmail.token":
            form = parse_qs(request.body.decode("utf-8", "replace"))
            assertion = (form.get("assertion") or [""])[0]
            email = (_decode_jwt_sub(assertion) or "unknown@provider-lab").lower()
            return ProviderResponse.json(
                {
                    "access_token": f"lab-gmail::{email}",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                }
            )

        if route_id.startswith("gmail.directory_"):
            return self._directory(request)

        mailbox = self._mailbox(request)
        if route_id == "gmail.profile":
            return ProviderResponse.json(
                {
                    "emailAddress": request.scope,
                    "historyId": str(mailbox["current_history_id"]),
                }
            )
        if route_id == "gmail.messages_list":
            messages = list(mailbox.get("messages", []))
            return ProviderResponse.json(
                {
                    "messages": [
                        {"id": item["id"], "threadId": item.get("threadId", item["id"])}
                        for item in messages
                    ],
                    "resultSizeEstimate": len(messages),
                }
            )
        if route_id == "gmail.message_get":
            message_id = request.path_params["message_id"]
            for message in mailbox.get("messages", []):
                if message.get("id") == message_id:
                    return ProviderResponse.json(message)
            return ProviderResponse.json(
                {"error": {"code": 404, "message": "not found"}},
                status_code=404,
            )
        if route_id == "gmail.history_list":
            return self._history(request, mailbox)
        if route_id == "gmail.watch":
            return ProviderResponse.json(
                {
                    "historyId": str(mailbox["current_history_id"]),
                    "expiration": "4102444800000",
                }
            )
        if route_id == "gmail.stop":
            return ProviderResponse.json({})
        if route_id in {
            "gmail.pubsub_topic",
            "gmail.pubsub_subscription",
        }:
            if request.method == "DELETE":
                return ProviderResponse.empty(status_code=200)
            return ProviderResponse.json(
                {
                    "name": request.path.removeprefix("/v1/"),
                }
            )
        if route_id == "gmail.pubsub_iam_get":
            topic = (
                f"{request.path_params['project_id']}/"
                f"{request.path_params['topic_id']}"
            )
            with self._pubsub_lock:
                policy = self._pubsub_iam_policies.get(topic)
            return ProviderResponse.json(
                copy.deepcopy(
                    policy
                    or state.get("pubsub_iam_policy")
                    or {"bindings": []}
                )
            )
        if route_id == "gmail.pubsub_iam_set":
            payload = request.json()
            if not isinstance(payload, Mapping):
                return ProviderResponse.json(
                    {"error": {"code": 400, "message": "invalid policy"}},
                    status_code=400,
                )
            policy = copy.deepcopy(
                payload.get("policy") or {"bindings": []}
            )
            topic = (
                f"{request.path_params['project_id']}/"
                f"{request.path_params['topic_id']}"
            )
            with self._pubsub_lock:
                self._pubsub_iam_policies[topic] = policy
            return ProviderResponse.json(policy)
        raise RuntimeError(f"unhandled Gmail route {route_id}")

    @staticmethod
    def _directory(request: ProviderRequest) -> ProviderResponse:
        directory = request.source_state.get("directory") or {}
        route_id = request.route.route_id
        if route_id == "gmail.directory_org_units":
            return ProviderResponse.json(
                {"organizationUnits": copy.deepcopy(directory.get("org_units") or [])}
            )
        if route_id == "gmail.directory_group_members":
            group_key = str(request.path_params["group_key"]).lower()
            rows = list((directory.get("group_members") or {}).get(group_key, []))
            return ProviderResponse.json(
                _google_directory_page(request, "members", rows)
            )
        if route_id == "gmail.directory_groups":
            rows = list(directory.get("groups") or [])
            domain = request.query_one("domain")
            if domain:
                suffix = f"@{domain.lower()}"
                rows = [
                    row
                    for row in rows
                    if str(row.get("email", "")).lower().endswith(suffix)
                ]
            return ProviderResponse.json(
                _google_directory_page(request, "groups", rows)
            )
        if route_id == "gmail.directory_users":
            rows = list(directory.get("users") or [])
            domain = request.query_one("domain")
            if domain:
                suffix = f"@{domain.lower()}"
                rows = [
                    row
                    for row in rows
                    if str(row.get("primaryEmail", "")).lower().endswith(suffix)
                ]
            query = request.query_one("query", "") or ""
            if query.startswith("orgUnitPath="):
                org_unit = query.removeprefix("orgUnitPath=")
                rows = [
                    row for row in rows if str(row.get("orgUnitPath", "")) == org_unit
                ]
            return ProviderResponse.json(_google_directory_page(request, "users", rows))
        raise RuntimeError(f"unhandled Gmail Directory route {route_id}")

    @staticmethod
    def _mailbox(request: ProviderRequest) -> Mapping[str, Any]:
        return (request.source_state.get("mailboxes") or {}).get(
            request.scope,
            {
                "messages": [],
                "history_events": [],
                "current_history_id": "1000",
            },
        )

    @staticmethod
    def _history(
        request: ProviderRequest, mailbox: Mapping[str, Any]
    ) -> ProviderResponse:
        start_history_id = request.query_one("startHistoryId", "0") or "0"
        if isinstance(mailbox.get("history"), list):
            history = [
                copy.deepcopy(event)
                for event in mailbox["history"]
                if int(str(event.get("id", "0"))) > int(start_history_id)
            ]
            return ProviderResponse.json(
                {
                    "history": history,
                    "historyId": str(mailbox["current_history_id"]),
                }
            )
        history = []
        for index, event in enumerate(mailbox.get("history_events", [])):
            if int(str(event.get("history_id", "0"))) <= int(start_history_id):
                continue
            message_id = event.get("message_id")
            history.append(
                {
                    "id": str(event.get("history_id", index)),
                    "messagesAdded": [{"message": {"id": message_id}}],
                }
            )
        return ProviderResponse.json(
            {
                "history": history,
                "historyId": str(mailbox["current_history_id"]),
            }
        )


def _google_directory_page(
    request: ProviderRequest,
    collection: str,
    rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    raw_token = request.query_one("pageToken", "0") or "0"
    if raw_token.startswith("offset:"):
        raw_token = raw_token.removeprefix("offset:")
    try:
        offset = max(0, int(raw_token))
    except ValueError:
        offset = 0
    try:
        maximum = int(request.query_one("maxResults", "200") or "200")
    except ValueError:
        maximum = 200
    maximum = max(1, min(maximum, 500))
    page = copy.deepcopy(rows[offset : offset + maximum])
    body: dict[str, Any] = {collection: page}
    next_offset = offset + len(page)
    if page and next_offset < len(rows):
        body["nextPageToken"] = f"offset:{next_offset}"
    return body


class DiscordAdapter:
    source = "discord"
    protocol_surfaces = (
        ProviderProtocolSurface(
            "discord.gateway",
            transport="websocket",
        ),
    )
    routes = (
        ProviderRoute(
            "discord.gateway_bot",
            "/api/v10/gateway/bot",
            operation_ids=("/gateway/bot",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.guilds",
            "/api/v10/users/@me/guilds",
            operation_ids=("/users/@me/guilds",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.guild_channels",
            "/api/v10/guilds/{guild_id}/channels",
            operation_ids=("/guilds/{guild_id}/channels",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.active_guild_threads",
            "/api/v10/guilds/{guild_id}/threads/active",
            operation_ids=("/guilds/{guild_id}/threads/active",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.archived_public_threads",
            "/api/v10/channels/{channel_id}/threads/archived/public",
            operation_ids=(
                "/channels/{channel_id}/threads/archived/public",
            ),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.archived_private_threads",
            "/api/v10/channels/{channel_id}/threads/archived/private",
            operation_ids=(
                "/channels/{channel_id}/threads/archived/private",
            ),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.channel_messages",
            "/api/v10/channels/{channel_id}/messages",
            operation_ids=("/channels/{channel_id}/messages",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.guild_member",
            "/api/v10/guilds/{guild_id}/members/{user_id}",
            operation_ids=("/guilds/{guild_id}/members/{user_id}",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.channel",
            "/api/v10/channels/{channel_id}",
            operation_ids=("/channels/{channel_id}",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.application_commands",
            "/api/v10/applications/{application_id}/commands",
            operation_ids=(
                "/applications/{application_id}/commands",
            ),
            methods=("POST",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.interaction_followup",
            "/api/v10/webhooks/{application_id}/{interaction_token}",
            operation_ids=(
                "/webhooks/{application_id}/{interaction_token}",
            ),
            methods=("POST",),
            quota_bucket="rest",
        ),
        ProviderRoute(
            "discord.oauth_token",
            "/api/v10/oauth2/token",
            operation_ids=("/oauth2/token",),
            methods=("POST",),
            quota_bucket="oauth",
        ),
    )

    def default_state(self) -> Mapping[str, Any]:
        return {
            "guilds": {},
            "messages": {},
            "active_threads": {},
            "archived_threads": {},
            "gateway_url": None,
            "gateway_session_id": "provider-lab-session",
            "gateway_heartbeat_interval_ms": 1_000,
            "gateway_events": [],
            "members": {},
        }

    def resolve_scope(self, request: ProviderRequest) -> str:
        explicit = _explicit_scope(request)
        if explicit:
            return explicit
        token = _bearer(request.headers) or ""
        for prefix in ("lab-discord::", "spam-bot::"):
            if token.startswith(prefix):
                return token[len(prefix) :] or "global"
        guild_id = request.path_params.get("guild_id")
        return str(guild_id) if guild_id else "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        state = request.source_state
        route_id = request.route.route_id
        guilds = state.get("guilds") or {}
        if route_id == "discord.gateway_bot":
            gateway_url = state.get("gateway_url")
            if not gateway_url:
                host = request.headers.get("host", "127.0.0.1:9191")
                gateway_url = f"ws://{host}/discord/gateway"
            return ProviderResponse.json(
                {
                    "url": gateway_url,
                    "shards": 1,
                    "session_start_limit": {
                        "total": 1000,
                        "remaining": 999,
                        "reset_after": 0,
                        "max_concurrency": 1,
                    },
                }
            )
        if route_id == "discord.guilds":
            ids = (
                [request.scope]
                if request.scope != "global" and request.scope in guilds
                else sorted(guilds)
            )
            return ProviderResponse.json([{"id": guild_id} for guild_id in ids])
        if route_id == "discord.guild_channels":
            guild_id = str(request.path_params["guild_id"])
            return ProviderResponse.json(
                list((guilds.get(guild_id) or {}).get("channels", []))
            )
        if route_id == "discord.guild_member":
            guild_id = str(request.path_params["guild_id"])
            user_id = str(request.path_params["user_id"])
            member = (
                (state.get("members") or {})
                .get(guild_id, {})
                .get(user_id)
            )
            if member is None:
                member = {
                    "user": {
                        "id": user_id,
                        "username": f"provider-lab-{user_id[-8:]}",
                    },
                    "roles": [],
                }
            return ProviderResponse.json(member)
        if route_id == "discord.channel":
            channel_id = str(request.path_params["channel_id"])
            for guild in guilds.values():
                for channel in guild.get("channels", []):
                    if str(channel.get("id")) == channel_id:
                        return ProviderResponse.json(channel)
            return ProviderResponse.json(
                {"message": "Unknown Channel", "code": 10003},
                status_code=404,
            )
        if route_id == "discord.application_commands":
            payload = request.json()
            if not isinstance(payload, Mapping):
                payload = {}
            return ProviderResponse.json(
                {
                    "id": "provider-lab-command",
                    "application_id": str(
                        request.path_params["application_id"]
                    ),
                    **copy.deepcopy(dict(payload)),
                },
                status_code=201,
            )
        if route_id == "discord.interaction_followup":
            payload = request.json()
            if not isinstance(payload, Mapping):
                payload = {}
            return ProviderResponse.json(
                {
                    "id": "provider-lab-followup",
                    "application_id": str(
                        request.path_params["application_id"]
                    ),
                    **copy.deepcopy(dict(payload)),
                }
            )
        if route_id == "discord.oauth_token":
            form = parse_qs(request.body.decode("utf-8", "replace"))
            code = (form.get("code") or ["provider-lab"])[0]
            guild_id = f"G_{code[-12:]}"
            return ProviderResponse.json(
                {
                    "access_token": f"lab-discord::{guild_id}",
                    "token_type": "Bearer",
                    "expires_in": 604800,
                    "scope": "applications.commands bot",
                    "guild": {"id": guild_id, "name": "Provider Lab"},
                }
            )
        if route_id == "discord.active_guild_threads":
            guild_id = str(request.path_params["guild_id"])
            return ProviderResponse.json(
                {
                    "threads": list(
                        (state.get("active_threads") or {}).get(guild_id, [])
                    ),
                    "members": [],
                }
            )
        if route_id in {
            "discord.archived_public_threads",
            "discord.archived_private_threads",
        }:
            channel_id = str(request.path_params["channel_id"])
            archive_kind = (
                "public" if route_id.endswith("public_threads") else "private"
            )
            archived = (state.get("archived_threads") or {}).get(
                channel_id,
                {},
            )
            return ProviderResponse.json(
                {
                    "threads": list(archived.get(archive_kind, [])),
                    "members": [],
                    "has_more": False,
                }
            )
        if route_id == "discord.channel_messages":
            channel_id = str(request.path_params["channel_id"])
            messages = list((state.get("messages") or {}).get(channel_id, []))
            messages.sort(key=lambda item: int(item["id"]), reverse=True)
            before = request.query_one("before")
            after = request.query_one("after")
            if before is not None:
                messages = [item for item in messages if int(item["id"]) < int(before)]
            if after is not None:
                messages = [item for item in messages if int(item["id"]) > int(after)]
            limit = max(1, min(100, int(request.query_one("limit", "100") or "100")))
            return ProviderResponse.json(messages[:limit])
        raise RuntimeError(f"unhandled Discord route {route_id}")


def build_lab_adapter_registry(
    *,
    expected_source_ids: tuple[str, ...] = CANONICAL_SOURCE_IDS,
) -> AdapterRegistry:
    """Build and validate the local registry.

    ``expected_source_ids`` is an explicit parity-test override. The default is
    always derived from the production source contract.
    """

    adapters: dict[str, Any] = {}
    adapters.update(wave_b_adapters())
    adapters.update(wave_cd_adapters())
    adapters.update(
        {
            "discord": DiscordAdapter(),
            "github": GithubAdapter(),
            "gmail": GmailAdapter(),
            "slack": SlackAdapter(),
        }
    )
    return AdapterRegistry(adapters, expected_sources=expected_source_ids)


def seed_reference_fixtures(
    fixtures: Mapping[str, list[Mapping[str, Any]]],
) -> dict[str, Mapping[str, Any]]:
    """Translate current synthetic fixture shapes into lab source state."""

    seeded: dict[str, Mapping[str, Any]] = {}
    seeded.update(seed_wave_b_fixtures(fixtures))
    seeded.update(seed_wave_cd_fixtures(fixtures))
    if fixtures.get("slack"):
        seeded["slack"] = _seed_slack(fixtures["slack"])
    if fixtures.get("github"):
        seeded["github"] = _seed_github(fixtures["github"])
    if fixtures.get("gmail"):
        seeded["gmail"] = _seed_gmail(fixtures["gmail"])
    if fixtures.get("discord"):
        seeded["discord"] = _seed_discord(fixtures["discord"])
    return seeded


def _seed_slack(fixtures: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    state: dict[str, Any] = {
        "teams": {},
        "messages": {},
        "direct_messages": {},
        "users": {},
    }
    for fixture in fixtures:
        team = str(fixture.get("team_id", "T_TEST"))
        team_channels = state["teams"].setdefault(team, [])
        for channel in fixture.get("channels", []):
            team_channels.append(
                {
                    "id": channel["id"],
                    "name": channel.get("name"),
                    "team_id": channel.get("team_id", team),
                }
            )
            state["messages"][channel["id"]] = copy.deepcopy(
                channel.get("messages", [])
            )
        for user in fixture.get("users", []):
            if user.get("id"):
                state["users"][user["id"]] = copy.deepcopy(user)
        for consenting in fixture.get("dm_users", []):
            user_id = consenting.get("user_id")
            summaries = []
            for conversation in consenting.get("conversations", []):
                channel_type = conversation.get("channel_type")
                summary = {
                    "id": conversation["id"],
                    "is_im": channel_type == "im" or bool(conversation.get("is_im")),
                    "is_mpim": (
                        channel_type == "mpim" or bool(conversation.get("is_mpim"))
                    ),
                    "channel_type": channel_type,
                }
                for key in ("name", "user"):
                    if conversation.get(key):
                        summary[key] = conversation[key]
                summaries.append(summary)
                state["messages"][conversation["id"]] = copy.deepcopy(
                    conversation.get("messages", [])
                )
            if user_id:
                state["direct_messages"][f"{team}::{user_id}"] = summaries
    return state


def _seed_gmail(fixtures: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    state: dict[str, Any] = {
        "directory": {
            "domain": None,
            "users": [],
            "groups": [],
            "group_members": {},
            "org_units": [],
        },
        "mailboxes": {},
    }
    for fixture in fixtures:
        directory = fixture.get("directory")
        if isinstance(directory, Mapping):
            state["directory"] = copy.deepcopy(dict(directory))

        raw_mailboxes = fixture.get("mailboxes")
        if isinstance(raw_mailboxes, Mapping):
            mailbox_items = raw_mailboxes.items()
        elif fixture.get("email"):
            mailbox_items = ((str(fixture["email"]), fixture),)
        else:
            mailbox_items = ()

        for email, raw_mailbox in mailbox_items:
            if not isinstance(raw_mailbox, Mapping):
                continue
            mailbox = copy.deepcopy(dict(raw_mailbox))
            mailbox["current_history_id"] = str(
                mailbox.get("current_history_id") or mailbox.get("history_id") or "1000"
            )
            mailbox.setdefault("messages", [])
            mailbox.setdefault("history_events", [])
            state["mailboxes"][str(email).lower()] = mailbox
    return state


def _seed_github(fixtures: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    state: dict[str, Any] = {"installations": {}, "repositories": {}}
    for fixture in fixtures:
        installation_id = str(fixture.get("installation_id", "12345"))
        names = state["installations"].setdefault(installation_id, [])
        for repo in fixture.get("repos", []):
            full_name = str(repo["full_name"])
            names.append(full_name)
            state["repositories"][full_name] = {
                "events_by_type": copy.deepcopy(repo.get("events_by_type", {}))
            }
    return state


def _seed_discord(fixtures: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    state: dict[str, Any] = {
        "guilds": {},
        "messages": {},
        "active_threads": {},
        "archived_threads": {},
        "gateway_url": None,
        "gateway_session_id": "provider-lab-session",
        "gateway_heartbeat_interval_ms": 1_000,
        "gateway_events": [],
    }
    for fixture in fixtures:
        guild_id = str(fixture["guild_id"])
        state["guilds"][guild_id] = {"channels": []}
        for channel in fixture.get("channels", []):
            channel_record = copy.deepcopy(
                {key: value for key, value in channel.items() if key != "messages"}
            )
            # Discord's REST guild-channel object always declares its numeric
            # channel type.  Compact fixtures may omit it, in which case they
            # represent a regular guild text channel (type 0).
            channel_record.setdefault("type", 0)
            state["guilds"][guild_id]["channels"].append(channel_record)
            state["messages"][channel["id"]] = copy.deepcopy(
                channel.get("messages", [])
            )
        active_threads = [
            copy.deepcopy(thread)
            for thread in fixture.get("active_threads", [])
            if isinstance(thread, Mapping)
        ]
        state["active_threads"][guild_id] = active_threads
        for thread in active_threads:
            thread_id = str(thread.get("id") or "")
            if thread_id:
                state["messages"][thread_id] = copy.deepcopy(thread.get("messages", []))
        for channel_id, archives in (fixture.get("archived_threads", {}) or {}).items():
            if not isinstance(archives, Mapping):
                continue
            state["archived_threads"][str(channel_id)] = {}
            for archive_kind in ("public", "private"):
                threads = [
                    copy.deepcopy(thread)
                    for thread in archives.get(archive_kind, [])
                    if isinstance(thread, Mapping)
                ]
                state["archived_threads"][str(channel_id)][archive_kind] = threads
                for thread in threads:
                    thread_id = str(thread.get("id") or "")
                    if thread_id:
                        state["messages"][thread_id] = copy.deepcopy(
                            thread.get("messages", [])
                        )
    return state


__all__ = [
    "build_lab_adapter_registry",
    "seed_reference_fixtures",
]
