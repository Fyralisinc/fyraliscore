"""services/ingest/integrations/gmail/client.py — Gmail + Directory HTTP clients.

Thin httpx-based wrappers over Google's REST APIs. We intentionally
avoid `google-api-python-client` to keep the dependency footprint
small and the call sites async-friendly.

Each call:
  1. Asks the DwdTokenMinter for an impersonated bearer token.
  2. Issues every HTTP attempt through ProviderTransport with
     `Authorization: Bearer <token>`.
  3. On 401, invalidates the cached token and retries once.
  4. Classifies 429/quota, timeout, transport, and 5xx outcomes for the
     transport's distributed cooldown and bounded retry policy.

Scope strings:
  GMAIL_METADATA_SCOPE  — gmail.metadata (headers only)
  GMAIL_READONLY_SCOPE  — gmail.readonly (headers + body)
  DIRECTORY_READ_SCOPE  — admin.directory.user.readonly + group.readonly + orgunit.readonly
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import httpx

from lib.shared.errors import CompanyOSError
from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RequestPolicy,
    parse_retry_after,
)

from services.ingest.integrations.gmail.dwd import DwdTokenMinter
from services.ingest.integrations.provider_transport import (
    PolicyResolver,
    ProviderExecutor,
    ProviderRequestBinding,
    QuotaResolver,
    explicit_local_transport,
)


GMAIL_METADATA_SCOPE = "https://www.googleapis.com/auth/gmail.metadata"
GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
DIRECTORY_USER_SCOPE = "https://www.googleapis.com/auth/admin.directory.user.readonly"
DIRECTORY_GROUP_SCOPE = "https://www.googleapis.com/auth/admin.directory.group.readonly"
DIRECTORY_ORGUNIT_SCOPE = (
    "https://www.googleapis.com/auth/admin.directory.orgunit.readonly"
)

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


class GmailHistoryExpired(GoogleApiError):
    """The history cursor is outside Gmail's retained history window."""

    default_code = "gmail_history_expired"
    _recoverable = True


class GmailHistoryRecoveryIncomplete(GoogleApiError):
    """A full history recovery did not complete, so its cursor is unchanged."""

    default_code = "gmail_history_recovery_incomplete"
    _recoverable = True


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
        source: str = "gmail",
        tenant_id: str | None = None,
        installation_id: str | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
        quota_dimensions: Mapping[str, str] | None = None,
        require_tenant_installation: bool = True,
    ) -> None:
        self._minter = minter
        self._client = http_client
        self._owns_client = http_client is None
        self._source = source
        self._tenant_id = tenant_id
        self._installation_id = installation_id
        self._quota_dimensions = dict(quota_dimensions or {})
        self._require_tenant_installation = require_tenant_installation
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=http_client is not None,
        )
        self._provider = ProviderRequestBinding(
            source=source,
            tenant_id=tenant_id,
            installation_id=installation_id,
            transport=provider_transport,
            request_policy=request_policy,
            quota_resolver=quota_resolver,
            allow_unlimited_local=local_unlimited,
            require_tenant=True,
            require_installation=require_tenant_installation,
        )

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
        operation_id: str = "api.request",
        source: str | None = None,
        tenant_id: str | None = None,
        installation_id: str | None = None,
    ) -> dict[str, Any]:
        scopes_t = tuple(scopes)
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        for attempt in (1, 2):
            token = await self._minter.mint(
                user_email=user_email,
                scopes=list(scopes_t),
                source=source or self._source,
                tenant_id=tenant_id or self._tenant_id,
                installation_id=installation_id or self._installation_id,
                quota_dimensions=self._quota_dimensions,
                require_tenant_installation=self._require_tenant_installation,
            )
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            resp = await self._request_attempt(
                method,
                url,
                operation_id=operation_id,
                headers=headers,
                params=params,
                json_body=json_body,
                source=source,
                tenant_id=tenant_id,
                installation_id=installation_id,
                user_email=user_email,
            )
            if resp.status_code == 401 and attempt == 1:
                # Token may have been revoked or rotated — drop the cache and retry.
                self._minter.invalidate(user_email=user_email, scopes=list(scopes_t))
                continue
            return self._handle_response(resp)
        raise GoogleApiError("unreachable: retry loop fell through")

    async def request_bytes(
        self,
        method: str,
        url: str,
        *,
        user_email: str,
        scopes: Iterable[str],
        params: dict[str, Any] | None = None,
        operation_id: str = "api.request_bytes",
        source: str | None = None,
        tenant_id: str | None = None,
        installation_id: str | None = None,
    ) -> bytes:
        """Like `request`, but returns the raw response body bytes instead of
        parsed JSON — for endpoints that return non-JSON content (Drive's
        `files.export` / `alt=media`, which return text/plain, text/csv, …).
        Shares the same DWD auth, 401-retry, and typed-error mapping."""
        scopes_t = tuple(scopes)
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        for attempt in (1, 2):
            token = await self._minter.mint(
                user_email=user_email,
                scopes=list(scopes_t),
                source=source or self._source,
                tenant_id=tenant_id or self._tenant_id,
                installation_id=installation_id or self._installation_id,
                quota_dimensions=self._quota_dimensions,
                require_tenant_installation=self._require_tenant_installation,
            )
            headers = {"Authorization": f"Bearer {token}"}
            resp = await self._request_attempt(
                method,
                url,
                operation_id=operation_id,
                headers=headers,
                params=params,
                source=source,
                tenant_id=tenant_id,
                installation_id=installation_id,
                user_email=user_email,
            )
            if resp.status_code == 401 and attempt == 1:
                self._minter.invalidate(user_email=user_email, scopes=list(scopes_t))
                continue
            if 200 <= resp.status_code < 300:
                return resp.content
            self._raise_for_error(resp)
        raise GoogleApiError("unreachable: retry loop fell through")

    async def _request_attempt(
        self,
        method: str,
        url: str,
        *,
        operation_id: str,
        headers: dict[str, str],
        params: dict[str, Any] | None,
        json_body: dict[str, Any] | None = None,
        source: str | None,
        tenant_id: str | None,
        installation_id: str | None,
        user_email: str,
    ) -> httpx.Response:
        if self._client is None:  # pragma: no cover - caller initializes it
            self._client = httpx.AsyncClient(timeout=30.0)

        async def _once() -> httpx.Response:
            assert self._client is not None
            try:
                response = await self._client.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                )
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "Google request timed out",
                    source=source or self._source,
                    operation=operation_id,
                    error_type=type(exc).__name__,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "Google transport error",
                    source=source or self._source,
                    operation=operation_id,
                    error_type=type(exc).__name__,
                ) from exc

            self._raise_transport_outcome(
                response,
                source=source or self._source,
                operation=operation_id,
            )
            return response

        return await self._provider.execute(
            operation_id,
            _once,
            source=source,
            tenant_id=tenant_id,
            installation_id=installation_id,
            quota_dimensions={
                **self._quota_dimensions,
                "user": user_email,
            },
        )

    @staticmethod
    def _raise_transport_outcome(
        resp: httpx.Response,
        *,
        source: str,
        operation: str,
    ) -> None:
        if resp.status_code == 429:
            raise ProviderRateLimited(
                "Google rate limit",
                retry_after_seconds=parse_retry_after(
                    resp.headers.get("Retry-After"),
                ),
                status_code=429,
                header_parser_id="http.retry_after",
                source=source,
                operation=operation,
            )
        if resp.status_code == 403:
            try:
                payload = resp.json()
            except ValueError:
                payload = {}
            reason = (
                (payload.get("error") or {}).get("errors", [{}])[0].get("reason", "")
                if isinstance(payload, dict)
                else ""
            )
            if reason in {
                "quotaExceeded",
                "userRateLimitExceeded",
                "rateLimitExceeded",
            }:
                raise ProviderRateLimited(
                    f"Google quota: {reason}",
                    retry_after_seconds=parse_retry_after(
                        resp.headers.get("Retry-After"),
                    ),
                    status_code=403,
                    header_parser_id="http.retry_after",
                    source=source,
                    operation=operation,
                )
        if resp.status_code in {500, 502, 503, 504}:
            retry_after = parse_retry_after(resp.headers.get("Retry-After"))
            if retry_after is not None:
                raise ProviderRateLimited(
                    "Google service cooldown",
                    retry_after_seconds=retry_after,
                    status_code=resp.status_code,
                    header_parser_id="http.retry_after",
                    source=source,
                    operation=operation,
                )
            raise ProviderTransientError(
                f"Google returned {resp.status_code}",
                source=source,
                operation=operation,
                http_status=resp.status_code,
            )

    @classmethod
    def _handle_response(cls, resp: httpx.Response) -> dict[str, Any]:
        if 200 <= resp.status_code < 300:
            if not resp.content:
                return {}
            return resp.json()
        cls._raise_for_error(resp)
        raise GoogleApiError("unreachable")  # pragma: no cover

    @staticmethod
    def _raise_for_error(resp: httpx.Response) -> None:
        """Map a non-2xx Google response to a typed error. NEVER returns."""
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
                (payload.get("error") or {}).get("errors", [{}])[0].get("reason", "")
            )
            if reason in (
                "quotaExceeded",
                "userRateLimitExceeded",
                "rateLimitExceeded",
            ):
                raise GoogleRateLimited(
                    f"google quota: {reason}",
                    status=403,
                    retry_after_s=60,
                )
        # NEVER log resp.request body / headers — they contain bearer tokens.
        raise GoogleApiError(
            f"google api error: status={resp.status_code} body={resp.text[:200]!r}",
            status=resp.status_code,
        )


def build_google_http_client(
    minter: DwdTokenMinter,
    *,
    tenant_id: str,
    installation_id: str,
    source: str = "gmail",
    http_client: httpx.AsyncClient | None = None,
) -> GoogleHttpClient:
    """Build the production-bound Google client for one exact installation."""

    from services.ingest.integrations.provider_transport_runtime import (
        get_provider_transport_runtime,
    )

    runtime = get_provider_transport_runtime()
    return GoogleHttpClient(
        minter,
        http_client=http_client,
        source=source,
        tenant_id=tenant_id,
        installation_id=installation_id,
        provider_transport=(runtime.transport if runtime is not None else None),
        quota_resolver=(runtime.quota_resolver if runtime is not None else None),
        allow_unlimited_local=runtime is None,
    )


def build_google_onboarding_http_client(
    minter: DwdTokenMinter,
    *,
    tenant_id: str,
    source: str = "gmail",
    quota_dimensions: Mapping[str, str] | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> GoogleHttpClient:
    """Build a tenant-bound client before a durable installation exists.

    Onboarding probes are real provider traffic, but fabricating a row id such
    as ``preflight:<domain>`` would make audit and quota attribution lie.  The
    transport context therefore carries the authenticated tenant and an
    explicit ``installation_id=None``. Provider quota policies for onboarding
    operations must use tenant/app/user/provider dimensions, not installation.
    """

    from services.ingest.integrations.provider_transport_runtime import (
        get_provider_transport_runtime,
    )

    runtime = get_provider_transport_runtime()
    return GoogleHttpClient(
        minter,
        http_client=http_client,
        source=source,
        tenant_id=tenant_id,
        installation_id=None,
        provider_transport=(runtime.transport if runtime is not None else None),
        quota_resolver=(runtime.quota_resolver if runtime is not None else None),
        allow_unlimited_local=runtime is None,
        quota_dimensions=quota_dimensions,
        require_tenant_installation=False,
    )


# =====================================================================
# Gmail API
# =====================================================================


class GmailClient:
    """Operations against gmail.googleapis.com (base URL resolved via
    lib.integrations.endpoints so it can be pointed at Provider Lab)."""

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
            operation_id="watch.create",
        )

    async def stop(self, *, user_email: str, scope: str) -> None:
        await self._http.request(
            "POST",
            f"{self._base}/users/me/stop",
            user_email=user_email,
            scopes=(scope,),
            operation_id="watch.stop",
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
            operation_id="messages.list",
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
        try:
            return await self._http.request(
                "GET",
                f"{self._base}/users/me/history",
                user_email=user_email,
                scopes=(scope,),
                params=params,
                operation_id="history.list",
            )
        except GoogleApiError as exc:
            if exc.context.get("status") == 404:
                raise GmailHistoryExpired(
                    "Gmail startHistoryId is invalid or expired",
                    status=404,
                    start_history_id=start_history_id,
                    user_email=user_email,
                ) from exc
            raise

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
            operation_id="messages.get",
        )

    async def get_profile(self, *, user_email: str, scope: str) -> dict[str, Any]:
        return await self._http.request(
            "GET",
            f"{self._base}/users/me/profile",
            user_email=user_email,
            scopes=(scope,),
            operation_id="profile.get",
        )


# =====================================================================
# Admin Directory API
# =====================================================================


class DirectoryClient:
    """Operations against admin.googleapis.com/admin/directory."""

    def __init__(
        self,
        http: GoogleHttpClient,
        admin_email: str,
        *,
        base_url: str | None = None,
    ) -> None:
        from lib.integrations.endpoints import endpoint

        self._http = http
        self._admin = admin_email
        self._base = (base_url or endpoint("google_directory")).rstrip("/")

    async def list_users(
        self,
        *,
        domain: str,
        page_token: str | None = None,
        page_size: int = 200,
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
            operation_id="directory.users.list",
        )
        return PagedResult(
            items=body.get("users") or [],
            next_page_token=body.get("nextPageToken"),
        )

    async def list_groups(
        self,
        *,
        domain: str,
        page_token: str | None = None,
        page_size: int = 200,
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
            operation_id="directory.groups.list",
        )
        return PagedResult(
            items=body.get("groups") or [],
            next_page_token=body.get("nextPageToken"),
        )

    async def list_group_members(
        self,
        *,
        group_key: str,
        page_token: str | None = None,
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
            operation_id="directory.group_members.list",
        )
        return PagedResult(
            items=body.get("members") or [],
            next_page_token=body.get("nextPageToken"),
        )

    async def list_org_units(
        self, *, customer_id: str = "my_customer"
    ) -> list[dict[str, Any]]:
        body = await self._http.request(
            "GET",
            f"{self._base}/customer/{customer_id}/orgunits",
            user_email=self._admin,
            scopes=(DIRECTORY_ORGUNIT_SCOPE,),
            params={"type": "all"},
            operation_id="directory.org_units.list",
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
            operation_id="directory.users_by_org_unit.list",
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
    "GmailHistoryExpired",
    "GmailHistoryRecoveryIncomplete",
    "GoogleApiError",
    "GoogleHttpClient",
    "GoogleRateLimited",
    "PagedResult",
    "build_google_http_client",
    "build_google_onboarding_http_client",
]
