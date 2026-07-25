"""Small binding seam between provider clients and ``ProviderTransport``.

Provider clients deliberately do not know how Redis quota state is built.
Their constructors receive an executor plus operation policy/quota resolvers,
and this module turns those bindings into the exact request context charged
for each upstream attempt.

The no-op transport is available only when a caller explicitly opts into
local/test mode.  Production therefore cannot silently send unmetered provider
requests while local Provider-Lab and unit tests remain ergonomic.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Protocol, TypeVar

from lib.shared.env import is_prod
from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderTransport,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
)


T = TypeVar("T")
PolicyResolver = Callable[[str], RequestPolicy]
QuotaResolver = Callable[
    [str, str, str | None, str | None, Mapping[str, str]],
    Sequence[QuotaRequirement],
]


class ProviderExecutor(Protocol):
    async def execute(
        self,
        request_context: RequestContext,
        policy: RequestPolicy,
        call: Callable[[], Awaitable[T]],
    ) -> T: ...


class ProviderRequestBinding:
    """Build and execute one exact provider operation context."""

    def __init__(
        self,
        *,
        source: str,
        tenant_id: str | None,
        installation_id: str | None,
        transport: ProviderExecutor | None,
        request_policy: RequestPolicy | PolicyResolver | None,
        quota_resolver: QuotaResolver | None,
        allow_unlimited_local: bool,
        require_tenant_installation: bool = True,
        require_tenant: bool | None = None,
        require_installation: bool | None = None,
    ) -> None:
        if is_prod() and allow_unlimited_local:
            raise RuntimeError(
                "allow_unlimited_local is forbidden in production",
            )
        if is_prod() and transport is None:
            raise RuntimeError(
                f"{source} requires ProviderTransport in production",
            )
        if is_prod() and quota_resolver is None:
            raise RuntimeError(
                f"{source} requires a declared quota resolver in production",
            )
        self._source = source
        self._tenant_id = tenant_id
        self._installation_id = installation_id
        self._transport: ProviderExecutor = transport or ProviderTransport()
        self._policy = request_policy
        self._quota = quota_resolver
        self._allow_unlimited_local = allow_unlimited_local
        self._require_tenant = (
            require_tenant_installation
            if require_tenant is None
            else require_tenant
        )
        self._require_installation = (
            require_tenant_installation
            if require_installation is None
            else require_installation
        )

    async def execute(
        self,
        operation: str,
        call: Callable[[], Awaitable[T]],
        *,
        source: str | None = None,
        tenant_id: str | None = None,
        installation_id: str | None = None,
        concurrency_key: str | None = None,
        idempotency_key: str | None = None,
        quota_dimensions: Mapping[str, str] | None = None,
    ) -> T:
        resolved_source = source or self._source
        resolved_tenant = tenant_id or self._tenant_id
        resolved_installation = installation_id or self._installation_id
        missing_binding = (
            (self._require_tenant and resolved_tenant is None)
            or (
                self._require_installation
                and resolved_installation is None
            )
        )
        if is_prod() and missing_binding:
            raise ProviderPermanentError(
                "provider request is missing exact tenant/installation binding",
                source=resolved_source,
                operation=operation,
                tenant_id=resolved_tenant,
                installation_id=resolved_installation,
                tenant_required=self._require_tenant,
                installation_required=self._require_installation,
            )
        requirements = (
            tuple(
                self._quota(
                    resolved_source,
                    operation,
                    resolved_tenant,
                    resolved_installation,
                    quota_dimensions or {},
                )
            )
            if self._quota is not None
            else ()
        )
        if not requirements and not self._allow_unlimited_local and self._quota is None:
            raise ProviderPermanentError(
                "provider operation has no quota policy",
                source=resolved_source,
                operation=operation,
            )
        context = RequestContext(
            source=resolved_source,
            operation=operation,
            tenant_id=resolved_tenant,
            installation_id=resolved_installation,
            quota_requirements=requirements,
            concurrency_key=concurrency_key,
            idempotency_key=idempotency_key,
        )
        if self._policy is None:
            from services.ingest.source_contract.catalog import (
                effective_request_policy,
            )

            policy = effective_request_policy(
                resolved_source,
                operation,
            )
        else:
            policy = (
                self._policy(operation)
                if callable(self._policy)
                else self._policy
            )
        return await self._transport.execute(context, policy, call)


def explicit_local_transport(
    *,
    requested: bool | None,
    has_local_injection: bool,
) -> bool:
    """Resolve the opt-in without ever weakening production.

    Supplying an HTTP client or provider base URL is considered an explicit
    local/test injection outside production.  A caller can also pass the flag
    directly (Provider Lab does this).
    """

    if requested is not None:
        return requested
    return not is_prod() and has_local_injection


def tenant_preinstall_transport_kwargs(
    tenant_id: Any,
) -> dict[str, Any]:
    """Bind an onboarding probe before its installation row exists.

    The tenant is already authenticated by the connect route, while the exact
    installation row is created only after the credential probe succeeds.
    Production still requires the distributed runtime; local tests explicitly
    opt into the no-op transport.
    """

    from services.ingest.integrations.provider_transport_runtime import (
        get_provider_transport_runtime,
    )

    runtime = get_provider_transport_runtime()
    return {
        "tenant_id": tenant_id,
        "provider_transport": (
            runtime.transport if runtime is not None else None
        ),
        "quota_resolver": (
            runtime.quota_resolver if runtime is not None else None
        ),
        "allow_unlimited_local": runtime is None,
        "require_tenant_installation": False,
    }


__all__ = [
    "PolicyResolver",
    "ProviderExecutor",
    "ProviderRequestBinding",
    "QuotaResolver",
    "explicit_local_transport",
    "tenant_preinstall_transport_kwargs",
]
