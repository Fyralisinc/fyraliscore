"""services/integrations/gmail/client.py — Gmail + Directory HTTP clients.

Thin httpx-based wrappers over Google's REST APIs. We intentionally
avoid `google-api-python-client` to keep the dependency footprint
small and the call sites async-friendly.

Each call:
  1. Asks the DwdTokenMinter for an impersonated bearer token.
  2. Issues the request with `Authorization: Bearer <token>`.
  3. On 401, invalidates the cached token and retries once.
  4. On 429 / 5xx, raises a typed error carrying the suggested
     retry_after (callers propagate to backoff state machines).

Scope strings:
  GMAIL_METADATA_SCOPE  — gmail.metadata (headers only)
  GMAIL_READONLY_SCOPE  — gmail.readonly (headers + body)
  DIRECTORY_READ_SCOPE  — admin.directory.user.readonly + group.readonly + orgunit.readonly
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Iterable

import httpx

from lib.shared.errors import CompanyOSError

from services.integrations.gmail.dwd import DwdTokenMinter


GMAIL_METADATA_SCOPE = "https://www.googleapis.com/auth/gmail.metadata"
GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
DIRECTORY_USER_SCOPE = "https://www.googleapis.com/auth/admin.directory.user.readonly"
DIRECTORY_GROUP_SCOPE = "https://www.googleapis.com/auth/admin.directory.group.readonly"
DIRECTORY_ORGUNIT_SCOPE = "https://www.googleapis.com/auth/admin.directory.orgunit.readonly"

DIRECTORY_READ_SCOPES = (
    DIRECTORY_USER_SCOPE,
    DIRECTORY_GROUP_SCOPE,
    DIRECTORY_ORGUNIT_SCOPE,
)

_GMAIL_BASE = "https://gmail.googleapis.com/gmail/v1"
_DIRECTORY_BASE = "https://admin.googleapis.com/admin/directory/v1"


class GoogleApiError(CompanyOSError):
    default_code = "google_api_error"


class GoogleRateLimited(GoogleApiError):
    default_code = "google_rate_limited"


@dataclass
class PagedResult:
    items: list[dict[str, Any]]
    next_page_token: str | None


class GoogleHttpClient:
    """Authed HTTP client. One instance per process.

    Callers pass the impersonated user_email + scope on each call;
    token minting is delegated to the DwdTokenMinter.
    """

    def __init__(
        self,
        minter: DwdTokenMinter,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._minter = minter
        self._client = http_client
        self._owns_client = http_client is None

    async def __aenter__(self) -> "GoogleHttpClient":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def request(
        self,
        method: str,
        url: str,
        *,
        user_email: str,
        scopes: Iterable[str],
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scopes_t = tuple(scopes)
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        for attempt in (1, 2):
            token = await self._minter.mint(user_email=user_email, scopes=list(scopes_t))
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            resp = await self._client.request(
                method, url, params=params, json=json_body, headers=headers,
            )
            if resp.status_code == 401 and attempt == 1:
                # Token may have been revoked or rotated — drop the cache and retry.
                self._minter.invalidate(user_email=user_email, scopes=list(scopes_t))
                continue
            return self._handle_response(resp)
        raise GoogleApiError("unreachable: retry loop fell through")

    @staticmethod
    def _handle_response(resp: httpx.Response) -> dict[str, Any]:
        if 200 <= resp.status_code < 300:
            if not resp.content:
                return {}
            return resp.json()
        if resp.status_code == 429 or resp.status_code in (500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            try:
                retry_after_s = int(retry_after) if retry_after else None
            except ValueError:
                retry_after_s = None
            raise GoogleRateLimited(
                f"google api rate-limited: status={resp.status_code}",
                status=resp.status_code,
                retry_after_s=retry_after_s,
            )
        if resp.status_code == 403:
            # quotaExceeded behaves like 429 from the caller's POV.
            try:
                payload = resp.json()
            except ValueError:
                payload = {}
            reason = (
                (payload.get("error") or {})
                .get("errors", [{}])[0]
                .get("reason", "")
            )
            if reason in ("quotaExceeded", "userRateLimitExceeded", "rateLimitExceeded"):
                raise GoogleRateLimited(
                    f"google quota: {reason}", status=403, retry_after_s=60,
                )
        # NEVER log resp.request body / headers — they contain bearer tokens.
        raise GoogleApiError(
            f"google api error: status={resp.status_code} body={resp.text[:200]!r}",
            status=resp.status_code,
        )


# =====================================================================
# Gmail API
# =====================================================================


class GmailClient:
    """Operations against gmail.googleapis.com (base URL resolved via
    lib.integrations.endpoints so it can be pointed at a local spammer)."""

    def __init__(self, http: GoogleHttpClient, *, base_url: str | None = None) -> None:
        from lib.integrations.endpoints import endpoint
        self._http = http
        self._base = (base_url or endpoint("gmail_api")).rstrip("/")

    async def watch(
        self,
        *,
        user_email: str,
        scope: str,
        topic_name: str,
        label_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"topicName": topic_name}
        if label_ids:
            body["labelIds"] = label_ids
        return await self._http.request(
            "POST",
            f"{self._base}/users/me/watch",
            user_email=user_email,
            scopes=(scope,),
            json_body=body,
        )

    async def stop(self, *, user_email: str, scope: str) -> None:
        await self._http.request(
            "POST",
            f"{self._base}/users/me/stop",
            user_email=user_email,
            scopes=(scope,),
        )

    async def messages_list(
        self,
        *,
        user_email: str,
        scope: str,
        page_token: str | None = None,
        max_results: int = 100,
        query: str | None = None,
    ) -> dict[str, Any]:
        """List messages in the mailbox. Per Gmail's users.messages.list.

        Used by the M6.3 backfill fetcher to page through all messages
        chronologically (Gmail returns newest-first; backfill is
        page-by-page until nextPageToken is absent).

        Returns the raw API body: `{"messages": [{"id": ..., "threadId": ...}, ...],
        "nextPageToken": ..., "resultSizeEstimate": ...}`. The fetcher
        is responsible for hydrating each id via `get_message`.

        `query` is the Gmail search expression; M6.3 doesn't use it
        (full-mailbox backfill) but it's exposed for future per-source
        filtering needs.
        """
        params: dict[str, Any] = {"maxResults": max_results}
        if page_token:
            params["pageToken"] = page_token
        if query:
            params["q"] = query
        return await self._http.request(
            "GET",
            f"{self._base}/users/me/messages",
            user_email=user_email,
            scopes=(scope,),
            params=params,
        )

    async def history_list(
        self,
        *,
        user_email: str,
        scope: str,
        start_history_id: str,
        page_token: str | None = None,
        history_types: tuple[str, ...] = ("messageAdded",),
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "startHistoryId": start_history_id,
            "historyTypes": list(history_types),
        }
        if page_token:
            params["pageToken"] = page_token
        return await self._http.request(
            "GET",
            f"{self._base}/users/me/history",
            user_email=user_email,
            scopes=(scope,),
            params=params,
        )

    async def get_message(
        self,
        *,
        user_email: str,
        scope: str,
        message_id: str,
    ) -> dict[str, Any]:
        # metadata vs full driven by the install scope.
        format_ = "full" if scope == GMAIL_READONLY_SCOPE else "metadata"
        return await self._http.request(
            "GET",
            f"{self._base}/users/me/messages/{message_id}",
            user_email=user_email,
            scopes=(scope,),
            params={"format": format_},
        )

    async def get_profile(self, *, user_email: str, scope: str) -> dict[str, Any]:
        return await self._http.request(
            "GET",
            f"{self._base}/users/me/profile",
            user_email=user_email,
            scopes=(scope,),
        )


# =====================================================================
# Admin Directory API
# =====================================================================


class DirectoryClient:
    """Operations against admin.googleapis.com/admin/directory."""

    def __init__(
        self, http: GoogleHttpClient, admin_email: str,
        *, base_url: str | None = None,
    ) -> None:
        from lib.integrations.endpoints import endpoint
        self._http = http
        self._admin = admin_email
        self._base = (base_url or endpoint("google_directory")).rstrip("/")

    async def list_users(
        self, *, domain: str, page_token: str | None = None, page_size: int = 200,
    ) -> PagedResult:
        params: dict[str, Any] = {"domain": domain, "maxResults": page_size}
        if page_token:
            params["pageToken"] = page_token
        body = await self._http.request(
            "GET",
            f"{self._base}/users",
            user_email=self._admin,
            scopes=(DIRECTORY_USER_SCOPE,),
            params=params,
        )
        return PagedResult(
            items=body.get("users") or [],
            next_page_token=body.get("nextPageToken"),
        )

    async def list_groups(
        self, *, domain: str, page_token: str | None = None, page_size: int = 200,
    ) -> PagedResult:
        params: dict[str, Any] = {"domain": domain, "maxResults": page_size}
        if page_token:
            params["pageToken"] = page_token
        body = await self._http.request(
            "GET",
            f"{self._base}/groups",
            user_email=self._admin,
            scopes=(DIRECTORY_GROUP_SCOPE,),
            params=params,
        )
        return PagedResult(
            items=body.get("groups") or [],
            next_page_token=body.get("nextPageToken"),
        )

    async def list_group_members(
        self, *, group_key: str, page_token: str | None = None,
    ) -> PagedResult:
        params: dict[str, Any] = {"maxResults": 200}
        if page_token:
            params["pageToken"] = page_token
        body = await self._http.request(
            "GET",
            f"{self._base}/groups/{group_key}/members",
            user_email=self._admin,
            scopes=(DIRECTORY_GROUP_SCOPE,),
            params=params,
        )
        return PagedResult(
            items=body.get("members") or [],
            next_page_token=body.get("nextPageToken"),
        )

    async def list_org_units(self, *, customer_id: str = "my_customer") -> list[dict[str, Any]]:
        body = await self._http.request(
            "GET",
            f"{self._base}/customer/{customer_id}/orgunits",
            user_email=self._admin,
            scopes=(DIRECTORY_ORGUNIT_SCOPE,),
            params={"type": "all"},
        )
        return body.get("organizationUnits") or []

    async def list_users_in_orgunit(
        self,
        *,
        customer_id: str = "my_customer",
        org_unit_path: str,
        page_token: str | None = None,
    ) -> PagedResult:
        # The Directory API filters users by orgUnitPath via the `query` param.
        params: dict[str, Any] = {
            "customer": customer_id,
            "query": f"orgUnitPath={org_unit_path}",
            "maxResults": 200,
        }
        if page_token:
            params["pageToken"] = page_token
        body = await self._http.request(
            "GET",
            f"{self._base}/users",
            user_email=self._admin,
            scopes=(DIRECTORY_USER_SCOPE,),
            params=params,
        )
        return PagedResult(
            items=body.get("users") or [],
            next_page_token=body.get("nextPageToken"),
        )


__all__ = [
    "DIRECTORY_GROUP_SCOPE",
    "DIRECTORY_ORGUNIT_SCOPE",
    "DIRECTORY_READ_SCOPES",
    "DIRECTORY_USER_SCOPE",
    "DirectoryClient",
    "GMAIL_METADATA_SCOPE",
    "GMAIL_READONLY_SCOPE",
    "GmailClient",
    "GoogleApiError",
    "GoogleHttpClient",
    "GoogleRateLimited",
    "PagedResult",
]
