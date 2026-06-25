"""services/ingest/integrations/figma/client.py - outbound Figma REST client.

Verified Figma read surface:

  * GET /v1/teams/{team_id}/projects
  * GET /v1/projects/{project_id}/files
  * GET /v1/files/{file_key}
  * GET /v1/files/{file_key}/versions
  * GET /v1/files/{file_key}/comments

There is no `/v1/files` list endpoint and no `/events` endpoint. The public
`list_events` method derives the event stream by merging file versions and
comments into the handler's existing event shape.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from urllib.parse import quote, urlsplit
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import FigmaApiError
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.figma.client")


_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 50
_MAX_VERSION_PAGE_SIZE = 50


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class FigmaClient:
    """Outbound Figma REST client, one per backfill/poll shard open."""

    def __init__(
        self,
        *,
        base_url: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        secret_ref: str | None = None,
        api_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str | None = None,
        team_id: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._secret_ref = secret_ref
        self._api_token_cache = SecretValueCache(preset=api_token)
        self._team_id = team_id
        self._token_lock = asyncio.Lock()
        self._api_base_url = (api_base_url or base_url).rstrip("/")
        self._owns_client = http_client is None
        self._http: httpx.AsyncClient | None = http_client

    def _httpx(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT_S)
            self._owns_client = True
        return self._http

    async def aclose(self) -> None:
        if self._owns_client and self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _token(self) -> str:
        return await self._api_token_cache.resolve(
            lock=self._token_lock,
            secret_store=self._secret_store,
            secret_ref=self._secret_ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: FigmaApiError(
                "figma client has no api token and cannot resolve one "
                "(missing secret_store / secret_ref / tenant_id)",
                code="figma_api_unauthorized",
            )
        )

    async def _headers(self) -> dict[str, str]:
        token = await self._token()
        # Personal access tokens use X-Figma-Token. OAuth bearer tokens are also
        # accepted by Figma, but the connect wizard stores PAT-style API tokens.
        return {
            "X-Figma-Token": token,
            "Accept": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        url: str | None = None,
    ) -> dict[str, Any]:
        from services.ingest.integrations.figma import metrics

        target = url or f"{self._api_base_url}{path}"
        headers = await self._headers()
        max_attempts = int(os.environ.get("FIGMA_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("FIGMA_RL_MAX_SLEEP_SEC", "30"))
        client = self._httpx()

        attempt = 0
        log_path = path or urlsplit(target).path
        while True:
            attempt += 1
            try:
                response = await client.request(
                    method, target, headers=headers, params=params,
                )
            except httpx.TransportError as exc:
                metrics.record_request("error")
                raise FigmaApiError(
                    "transport error calling figma",
                    code="figma_api_error",
                    context={"error_type": type(exc).__name__, "path": log_path},
                ) from exc

            if response.status_code == 429 and attempt < max_attempts:
                metrics.record_request("rate_limited")
                delay = _parse_retry_after(response.headers.get("Retry-After"))
                await asyncio.sleep(min(max_sleep, delay))
                continue

            if response.status_code // 100 == 2:
                metrics.record_request("ok")
                body = _safe_json(response)
                if not isinstance(body, dict):
                    raise FigmaApiError(
                        "figma response was not a JSON object",
                        code="figma_api_error",
                        context={"path": log_path},
                    )
                return body

            if response.status_code in (401, 403):
                metrics.record_request("unauthorized")
            else:
                metrics.record_request("error")
            raise _api_error_from_response(response, log_path)

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def list_files(self, team_id: str | None = None) -> list[dict[str, Any]]:
        """Enumerate team projects, then project files."""
        tid = team_id or self._team_id
        if not tid:
            raise FigmaApiError(
                "figma list_files requires a team_id",
                code="figma_api_error",
            )
        projects_body = await self._request(
            "GET", f"/v1/teams/{quote(tid, safe='')}/projects",
        )
        projects = _extract_list(projects_body, "projects")
        files: list[dict[str, Any]] = []
        for project in projects:
            project_id = str(project.get("id") or "")
            if not project_id:
                continue
            body = await self._request(
                "GET", f"/v1/projects/{quote(project_id, safe='')}/files",
            )
            for item in _extract_list(body, "files"):
                item.setdefault("project_id", project_id)
                item.setdefault("project_name", project.get("name"))
                item.setdefault("team_id", tid)
                files.append(item)
        return files

    async def get_file(self, file_key: str) -> dict[str, Any]:
        """`GET /v1/files/{key}` - file metadata/tree visible to the token."""
        return await self._request(
            "GET", f"/v1/files/{quote(file_key, safe='')}",
        )

    async def list_events(
        self,
        file_key: str,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        versions = await self._list_versions(file_key)
        comments = await self._list_comments(file_key)
        events = [
            _version_event(file_key, v)
            for v in versions
            if isinstance(v, dict)
        ] + [
            _comment_event(file_key, c)
            for c in comments
            if isinstance(c, dict)
        ]
        if start:
            floor = start[:10]
            events = [e for e in events if _event_date(e) >= floor]
        events.sort(key=_event_sort_key, reverse=True)

        eff_limit = max(1, int(limit or _DEFAULT_PAGE_SIZE))
        page = events[offset:offset + eff_limit]
        total = len(events)
        next_offset = offset + len(page)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total

    async def _list_versions(self, file_key: str) -> list[dict[str, Any]]:
        path = f"/v1/files/{quote(file_key, safe='')}/versions"
        params: dict[str, Any] = {"page_size": _MAX_VERSION_PAGE_SIZE}
        body = await self._request("GET", path, params=params)
        out = _extract_list(body, "versions")
        next_url = _pagination_next(body)
        while next_url:
            body = await self._request("GET", "", url=next_url)
            out.extend(_extract_list(body, "versions"))
            next_url = _pagination_next(body)
        return out

    async def _list_comments(self, file_key: str) -> list[dict[str, Any]]:
        body = await self._request(
            "GET", f"/v1/files/{quote(file_key, safe='')}/comments",
        )
        return _extract_list(body, "comments")


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _extract_list(body: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = body.get(key)
    return [x for x in value if isinstance(x, dict)] if isinstance(value, list) else []


def _pagination_next(body: dict[str, Any]) -> str | None:
    pagination = body.get("pagination")
    if isinstance(pagination, dict):
        value = pagination.get("next_page") or pagination.get("next")
        return value if isinstance(value, str) and value else None
    links = body.get("links")
    if isinstance(links, dict):
        value = links.get("next")
        return value if isinstance(value, str) and value else None
    return None


def _version_event(file_key: str, version: dict[str, Any]) -> dict[str, Any]:
    version_id = str(version.get("id") or version.get("version") or "")
    created = (
        version.get("created_at")
        or version.get("createdAt")
        or version.get("timestamp")
    )
    label = version.get("label") or version.get("description") or version_id
    user = version.get("user")
    actor = None
    if isinstance(user, dict):
        actor = user.get("handle") or user.get("name") or user.get("id")
    elif isinstance(user, str):
        actor = user
    return {
        "id": version_id or f"version:{created or 'unknown'}",
        "event_id": version_id or f"version:{created or 'unknown'}",
        "event_type": "FILE_VERSION_UPDATE",
        "type": "FILE_VERSION_UPDATE",
        "file_key": file_key,
        "fileKey": file_key,
        "version": version_id or created or "none",
        "label": label,
        "description": version.get("description"),
        "user": actor,
        "triggered_by": {"handle": actor} if actor else None,
        "createdAt": created,
        "created_at": created,
        "raw_version": version,
    }


def _comment_event(file_key: str, comment: dict[str, Any]) -> dict[str, Any]:
    comment_id = str(comment.get("id") or comment.get("comment_id") or "")
    created = (
        comment.get("created_at")
        or comment.get("createdAt")
        or comment.get("timestamp")
    )
    user = comment.get("user")
    actor = None
    if isinstance(user, dict):
        actor = user.get("handle") or user.get("name") or user.get("id")
    elif isinstance(user, str):
        actor = user
    return {
        "id": comment_id or f"comment:{created or 'unknown'}",
        "event_id": comment_id or f"comment:{created or 'unknown'}",
        "event_type": "FILE_COMMENT",
        "type": "FILE_COMMENT",
        "file_key": file_key,
        "fileKey": file_key,
        "version": comment.get("updated_at") or created or "none",
        "label": "File comment",
        "message": comment.get("message"),
        "user": actor,
        "triggered_by": {"handle": actor} if actor else None,
        "createdAt": created,
        "created_at": created,
        "raw_comment": comment,
    }


def _event_date(event: dict[str, Any]) -> str:
    value = event.get("createdAt") or event.get("created_at") or ""
    return value[:10] if isinstance(value, str) else ""


def _event_sort_key(event: dict[str, Any]) -> str:
    value = event.get("createdAt") or event.get("created_at")
    return value if isinstance(value, str) else ""


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001
        return None


def _api_error_from_response(
    response: httpx.Response, path: str,
) -> FigmaApiError:
    status = response.status_code
    if status in (401, 403):
        return FigmaApiError(
            f"figma {status}: access token rejected or insufficient scope",
            code="figma_api_unauthorized",
            context={"http_status": status, "path": path},
        )
    if status == 404:
        return FigmaApiError(
            "figma 404: file/resource not found or not visible to the token",
            code="figma_api_not_found",
            context={"http_status": status, "path": path},
        )
    if status == 429:
        return FigmaApiError(
            "figma rate limit (429), retry budget exhausted",
            code="figma_api_rate_limited",
            context={
                "http_status": 429,
                "retry_after": response.headers.get("Retry-After"),
                "path": path,
            },
        )
    return FigmaApiError(
        f"figma returned {status}",
        code="figma_api_error",
        context={"http_status": status, "path": path},
    )


__all__ = ["FigmaClient", "FigmaApiError"]
