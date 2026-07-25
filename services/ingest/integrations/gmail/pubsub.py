"""services/ingest/integrations/gmail/pubsub.py — per-tenant Pub/Sub provisioning.

For each Gmail install we provision a tenant-isolated topic +
subscription in Fyralis's GCP project. The Gmail push service account
(`gmail-api-push@system.gserviceaccount.com`) is granted publisher on
that one topic; our webhook is configured as the subscription's push
endpoint, gated by an OIDC token whose audience is our webhook URL.

The topic name encodes tenant_id so a leaked or mis-routed Pub/Sub
message can be traced to a single tenant — and so the subscription
itself is the tenant boundary at the GCP IAM layer.

Production requires GCP IAM privileges (pubsub.admin) granted to the
service account running this module. In a customer-owned-topic follow-up
spec these calls move into the customer's GCP project; the interface
stays the same.

This module never needs a per-mailbox impersonated token; the
provisioning service account itself is enough.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from uuid import UUID

import httpx
import structlog

from lib.shared.errors import CompanyOSError
from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RequestPolicy,
    parse_retry_after,
)

from services.ingest.integrations.gmail.dwd import ServiceAccountKey, DwdTokenMinter
from services.ingest.integrations.provider_transport import (
    PolicyResolver,
    ProviderExecutor,
    ProviderRequestBinding,
    QuotaResolver,
    explicit_local_transport,
)
from services.ingest.integrations.provider_transport_runtime import (
    get_provider_transport_runtime,
)


log = structlog.get_logger("integrations.gmail.pubsub")


# Google's reserved push service account that Gmail uses to publish
# notifications onto the topic we own.
GMAIL_PUSH_SA = "serviceAccount:gmail-api-push@system.gserviceaccount.com"

PUBSUB_SCOPE = "https://www.googleapis.com/auth/pubsub"

_PUBSUB_BASE = "https://pubsub.googleapis.com/v1"


class PubsubProvisioningError(CompanyOSError):
    default_code = "gmail_pubsub_provisioning_error"


@dataclass(frozen=True)
class PubsubResources:
    topic_name: str         # 'projects/{proj}/topics/gmail-{tenant_id}'
    subscription_name: str  # 'projects/{proj}/subscriptions/gmail-{tenant_id}-sub'


def _project_id() -> str:
    pid = os.environ.get("GMAIL_PUBSUB_PROJECT_ID")
    if not pid:
        raise PubsubProvisioningError(
            "GMAIL_PUBSUB_PROJECT_ID is not set — required to provision per-tenant topics",
        )
    return pid


def _push_endpoint() -> str:
    """The full HTTPS URL that the subscription pushes to."""
    url = os.environ.get("GMAIL_PUBSUB_PUSH_ENDPOINT")
    if not url:
        raise PubsubProvisioningError(
            "GMAIL_PUBSUB_PUSH_ENDPOINT is not set — required for push subscription",
        )
    return url


def _push_oidc_audience() -> str:
    """The audience the Pub/Sub OIDC token must carry. Defaults to the
    push endpoint URL, but allows override when the webhook sits behind
    a proxy that rewrites the audience."""
    return os.environ.get("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE") or _push_endpoint()


def _push_oidc_service_account() -> str:
    """The service-account email whose identity Pub/Sub will mint OIDC
    tokens for. The webhook verifies the JWT was signed for this account."""
    sa = os.environ.get("GMAIL_PUBSUB_PUSH_OIDC_SA")
    if not sa:
        raise PubsubProvisioningError(
            "GMAIL_PUBSUB_PUSH_OIDC_SA is not set — required for OIDC-authed push",
        )
    return sa


def resource_names_for_tenant(tenant_id: UUID) -> PubsubResources:
    """Compute the canonical topic + subscription names for a tenant
    (deterministic so reprovisioning is idempotent)."""
    pid = _project_id()
    suffix = str(tenant_id).replace("-", "")
    return PubsubResources(
        topic_name=f"projects/{pid}/topics/gmail-{suffix}",
        subscription_name=f"projects/{pid}/subscriptions/gmail-{suffix}-sub",
    )


class PubsubAdmin:
    """Provision and tear down per-tenant Pub/Sub resources.

    Uses a service account distinct from the Gmail DWD service account.
    Some deployments will use the same identity; the two are decoupled
    in this module so they can diverge.
    """

    def __init__(
        self,
        minter: DwdTokenMinter | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
    ) -> None:
        # Reuse the DWD key for token minting against pubsub.googleapis.com.
        # The pubsub scope is granted to the service account at the
        # Google Cloud IAM level (pubsub.admin role).
        runtime = get_provider_transport_runtime()
        self._owns_minter = minter is None
        self._minter = minter or DwdTokenMinter(
            ServiceAccountKey.from_env(),
            provider_transport=(
                provider_transport
                or (runtime.transport if runtime is not None else None)
            ),
            quota_resolver=(
                quota_resolver
                or (runtime.quota_resolver if runtime is not None else None)
            ),
            allow_unlimited_local=(
                runtime is None
                if allow_unlimited_local is None
                else allow_unlimited_local
            ),
        )
        self._client = http_client
        self._owns_client = http_client is None
        local_unlimited = explicit_local_transport(
            requested=(
                runtime is None
                if allow_unlimited_local is None
                else allow_unlimited_local
            ),
            has_local_injection=http_client is not None,
        )
        self._provider = ProviderRequestBinding(
            source="gmail",
            tenant_id=None,
            installation_id=None,
            transport=(
                provider_transport
                or (runtime.transport if runtime is not None else None)
            ),
            request_policy=request_policy,
            quota_resolver=(
                quota_resolver
                or (runtime.quota_resolver if runtime is not None else None)
            ),
            allow_unlimited_local=(
                local_unlimited if runtime is None else False
            ),
        )

    async def __aenter__(self) -> "PubsubAdmin":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._owns_minter:
            await self._minter.__aexit__(*exc)

    async def _token(
        self,
        *,
        tenant_id: UUID,
        installation_id: UUID | str,
    ) -> str:
        # For service-to-service (non-user) calls we mint a token
        # impersonating the SA itself. Google's DWD endpoint accepts
        # sub == iss for non-user grants.
        return await self._minter.mint(
            user_email=self._minter.service_account_email,
            scopes=[PUBSUB_SCOPE],
            source="gmail",
            tenant_id=str(tenant_id),
            installation_id=str(installation_id),
            quota_dimensions={"project": _project_id()},
        )

    async def provision(
        self,
        tenant_id: UUID,
        *,
        installation_id: UUID | str,
    ) -> PubsubResources:
        """Idempotent: creates topic + subscription if missing, grants
        Gmail's push SA publisher rights, configures push endpoint."""
        resources = resource_names_for_tenant(tenant_id)
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        token = await self._token(
            tenant_id=tenant_id,
            installation_id=installation_id,
        )
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # 1. Create topic (PUT is idempotent in Pub/Sub).
        resp = await self._request(
            "PUT",
            resources.topic_name,
            operation="pubsub.topic.create",
            tenant_id=tenant_id,
            installation_id=installation_id,
            headers=headers,
            json_body={},
        )
        if resp.status_code not in (200, 409):
            raise PubsubProvisioningError(
                f"topic create failed: {resp.status_code} {resp.text[:200]!r}",
            )

        # 2. Grant Gmail push SA publisher.
        await self._set_topic_iam(
            resources.topic_name,
            role="roles/pubsub.publisher",
            member=GMAIL_PUSH_SA,
            token=token,
            tenant_id=tenant_id,
            installation_id=installation_id,
        )

        # 3. Create subscription with push config (PUT idempotent).
        sub_body = {
            "topic": resources.topic_name,
            "ackDeadlineSeconds": 60,
            "messageRetentionDuration": "604800s",  # 7 days
            "pushConfig": {
                "pushEndpoint": _push_endpoint(),
                "oidcToken": {
                    "serviceAccountEmail": _push_oidc_service_account(),
                    "audience": _push_oidc_audience(),
                },
            },
        }
        resp = await self._request(
            "PUT",
            resources.subscription_name,
            operation="pubsub.subscription.create",
            tenant_id=tenant_id,
            installation_id=installation_id,
            headers=headers,
            json_body=sub_body,
        )
        if resp.status_code not in (200, 409):
            raise PubsubProvisioningError(
                f"subscription create failed: {resp.status_code} {resp.text[:200]!r}",
            )
        return resources

    async def teardown(
        self,
        tenant_id: UUID,
        *,
        installation_id: UUID | str,
    ) -> None:
        """Delete the per-tenant subscription + topic. Idempotent
        — 404s on missing resources are tolerated."""
        resources = resource_names_for_tenant(tenant_id)
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        token = await self._token(
            tenant_id=tenant_id,
            installation_id=installation_id,
        )
        headers = {"Authorization": f"Bearer {token}"}
        for path, operation in (
            (
                resources.subscription_name,
                "pubsub.subscription.delete",
            ),
            (resources.topic_name, "pubsub.topic.delete"),
        ):
            resp = await self._request(
                "DELETE",
                path,
                operation=operation,
                tenant_id=tenant_id,
                installation_id=installation_id,
                headers=headers,
            )
            if resp.status_code not in (200, 404):
                raise PubsubProvisioningError(
                    f"delete {path} failed: {resp.status_code} {resp.text[:200]!r}",
                )

    async def _set_topic_iam(
        self,
        topic_name: str,
        *,
        role: str,
        member: str,
        token: str,
        tenant_id: UUID,
        installation_id: UUID | str,
    ) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        # getIamPolicy → modify → setIamPolicy
        resp = await self._request(
            "POST",
            f"{topic_name}:getIamPolicy",
            operation="pubsub.iam.get",
            tenant_id=tenant_id,
            installation_id=installation_id,
            headers=headers,
            json_body={},
        )
        if resp.status_code != 200:
            raise PubsubProvisioningError(
                f"getIamPolicy failed: {resp.status_code} {resp.text[:200]!r}",
            )
        policy = resp.json()
        bindings = policy.get("bindings") or []
        for b in bindings:
            if b.get("role") == role:
                members = set(b.get("members") or [])
                if member in members:
                    return  # already granted
                members.add(member)
                b["members"] = sorted(members)
                break
        else:
            bindings.append({"role": role, "members": [member]})
        policy["bindings"] = bindings
        resp = await self._request(
            "POST",
            f"{topic_name}:setIamPolicy",
            operation="pubsub.iam.set",
            tenant_id=tenant_id,
            installation_id=installation_id,
            headers=headers,
            json_body={"policy": policy},
        )
        if resp.status_code != 200:
            raise PubsubProvisioningError(
                f"setIamPolicy failed: {resp.status_code} {resp.text[:200]!r}",
            )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        tenant_id: UUID,
        installation_id: UUID | str,
        headers: dict[str, str],
        json_body: dict[str, object] | None = None,
    ) -> httpx.Response:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)

        async def _once() -> httpx.Response:
            assert self._client is not None
            try:
                response = await self._client.request(
                    method,
                    f"{_PUBSUB_BASE}/{path}",
                    headers=headers,
                    json=json_body,
                )
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "Google Pub/Sub admin request timed out",
                    source="gmail",
                    operation=operation,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "Google Pub/Sub admin transport error",
                    source="gmail",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            self._raise_transport_outcome(response, operation=operation)
            return response

        return await self._provider.execute(
            operation,
            _once,
            tenant_id=str(tenant_id),
            installation_id=str(installation_id),
            quota_dimensions={"project": _project_id()},
        )

    @staticmethod
    def _raise_transport_outcome(
        response: httpx.Response,
        *,
        operation: str,
    ) -> None:
        if response.status_code == 429:
            raise ProviderRateLimited(
                "Google Pub/Sub admin rate limit",
                retry_after_seconds=parse_retry_after(
                    response.headers.get("Retry-After"),
                ),
                status_code=429,
                header_parser_id="http.retry_after",
                source="gmail",
                operation=operation,
            )
        if response.status_code == 403:
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            reason = (
                (payload.get("error") or {})
                .get("errors", [{}])[0]
                .get("reason", "")
                if isinstance(payload, dict)
                else ""
            )
            if reason in {
                "quotaExceeded",
                "userRateLimitExceeded",
                "rateLimitExceeded",
            }:
                raise ProviderRateLimited(
                    f"Google Pub/Sub quota: {reason}",
                    retry_after_seconds=parse_retry_after(
                        response.headers.get("Retry-After"),
                    ),
                    status_code=403,
                    header_parser_id="http.retry_after",
                    source="gmail",
                    operation=operation,
                )
        if response.status_code in {500, 502, 503, 504}:
            retry_after = parse_retry_after(
                response.headers.get("Retry-After"),
            )
            if retry_after is not None:
                raise ProviderRateLimited(
                    "Google Pub/Sub service cooldown",
                    retry_after_seconds=retry_after,
                    status_code=response.status_code,
                    header_parser_id="http.retry_after",
                    source="gmail",
                    operation=operation,
                )
            raise ProviderTransientError(
                f"Google Pub/Sub returned {response.status_code}",
                source="gmail",
                operation=operation,
                http_status=response.status_code,
            )


__all__ = [
    "GMAIL_PUSH_SA",
    "PUBSUB_SCOPE",
    "PubsubAdmin",
    "PubsubProvisioningError",
    "PubsubResources",
    "resource_names_for_tenant",
]
