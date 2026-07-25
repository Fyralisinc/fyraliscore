"""Runtime resolution for declarative source callable bindings.

Production bindings come from :class:`SourceDefinition` as validated
``module.path:callable`` references. Tests may replace individual roles only
inside an explicit context manager; overrides are task-local via ``ContextVar``
and restore correctly across nested scopes.
"""
from __future__ import annotations

import importlib
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from functools import lru_cache
from types import MappingProxyType
from typing import Any, Literal

from services.ingest.source_contract.catalog import (
    NORMALIZER_BINDING_CATALOG,
    PROVIDER_DEFINITIONS,
    SOURCE_DEFINITIONS,
    dedicated_ingress_definition,
    normalizer_binding_for_channel,
    source_definition,
    webhook_ingress_definition,
)


HistoryBindingRole = Literal["planner", "fetcher", "reconciler"]
HistoryCallable = Callable[..., Any]
NormalizerCallable = Callable[..., Any]
IdempotencyBuilderCallable = Callable[..., str | None]
InstallationLoaderCallable = Callable[..., Any]
PlannerClientBuilderCallable = Callable[..., Any]
OnboardingFailureCallable = Callable[..., Any]
WebhookVerifierCallable = Callable[..., Any]
WebhookTenantExtractorCallable = Callable[..., str | None]
DedicatedIngressCallable = Callable[..., Any]

_CALLABLE_REF_RE = re.compile(
    r"^(?P<module>[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*):"
    r"(?P<qualname>[A-Za-z_][A-Za-z0-9_]*"
    r"(?:\.[A-Za-z_][A-Za-z0-9_]*)*)$"
)
_BINDING_ATTRIBUTE: Mapping[HistoryBindingRole, str] = MappingProxyType(
    {
        "planner": "planner_binding",
        "fetcher": "fetcher_binding",
        "reconciler": "reconciler_binding",
    }
)


class BindingResolutionError(RuntimeError):
    """A declared callable reference could not be safely resolved."""


class HistoryNotSupportedError(BindingResolutionError):
    """The source explicitly has no historical ingestion contract."""


class NormalizationChannelNotFoundError(BindingResolutionError):
    """A channel has no normalizer binding in the source contract."""


class InstallationBindingNotFoundError(BindingResolutionError):
    """A source has no historical installation binding."""


_OverrideKey = tuple[str, HistoryBindingRole]
_EMPTY_OVERRIDES: Mapping[_OverrideKey, HistoryCallable] = MappingProxyType({})
_OVERRIDES: ContextVar[Mapping[_OverrideKey, HistoryCallable]] = ContextVar(
    "source_contract_history_overrides",
    default=_EMPTY_OVERRIDES,
)


def split_callable_reference(reference: str) -> tuple[str, str]:
    """Validate and split one ``module.path:qualname`` reference."""

    if not isinstance(reference, str):
        raise BindingResolutionError("callable reference must be a string")
    match = _CALLABLE_REF_RE.fullmatch(reference)
    if match is None:
        raise BindingResolutionError(
            "invalid callable reference "
            f"{reference!r}; expected 'module.path:callable'"
        )
    return match.group("module"), match.group("qualname")


@lru_cache(maxsize=None)
def resolve_callable_reference(reference: str) -> HistoryCallable:
    """Import and resolve a validated callable reference once per process."""

    module_name, qualname = split_callable_reference(reference)
    try:
        value: Any = importlib.import_module(module_name)
    except Exception as exc:  # noqa: BLE001 - preserve import diagnostics
        raise BindingResolutionError(
            f"could not import binding module {module_name!r} for {reference!r}"
        ) from exc
    for attribute in qualname.split("."):
        try:
            value = getattr(value, attribute)
        except AttributeError as exc:
            raise BindingResolutionError(
                f"binding {reference!r} has no attribute {attribute!r}"
            ) from exc
    if not callable(value):
        raise BindingResolutionError(
            f"binding {reference!r} resolved to non-callable "
            f"{type(value).__name__}"
        )
    return value


def resolve_history_binding(
    source_name: str,
    role: HistoryBindingRole,
) -> HistoryCallable:
    """Resolve one source's planner, fetcher, or reconciler."""

    if role not in _BINDING_ATTRIBUTE:
        raise BindingResolutionError(f"unknown history binding role {role!r}")
    source = source_definition(source_name)
    if source.history is None:
        raise HistoryNotSupportedError(
            f"source {source.source_id!r} has no historical ingestion contract"
        )
    override = _OVERRIDES.get().get((source.source_id, role))
    if override is not None:
        return override
    reference = getattr(source, _BINDING_ATTRIBUTE[role])
    if reference is None:
        raise BindingResolutionError(
            f"source {source.source_id!r} has history={source.history!r} "
            f"but no {role} binding"
        )
    return resolve_callable_reference(reference)


def resolve_planner(source_name: str) -> HistoryCallable:
    return resolve_history_binding(source_name, "planner")


def resolve_fetcher(source_name: str) -> HistoryCallable:
    return resolve_history_binding(source_name, "fetcher")


def resolve_reconciler(source_name: str) -> HistoryCallable:
    return resolve_history_binding(source_name, "reconciler")


def resolve_handler(channel: str) -> NormalizerCallable:
    """Resolve one channel's immutable contract-declared normalizer."""

    try:
        reference = normalizer_binding_for_channel(channel)
    except (KeyError, TypeError) as exc:
        raise NormalizationChannelNotFoundError(
            f"channel {channel!r} has no declared normalizer binding"
        ) from exc
    return resolve_callable_reference(reference)


def resolve_idempotency_builders(
    source_name: str,
) -> tuple[IdempotencyBuilderCallable, ...]:
    """Resolve every external-ID builder owned by one source contract."""

    source = source_definition(source_name)
    return tuple(
        resolve_callable_reference(reference)
        for reference in source.idempotency_builder_bindings
    )


def resolve_installation_loader(
    source_name: str,
) -> InstallationLoaderCallable:
    """Resolve the source's exact, tenant-scoped installation loader."""

    source = source_definition(source_name)
    adapter = source.installation_adapter
    if adapter is None or adapter.loader_binding is None:
        raise InstallationBindingNotFoundError(
            f"source {source.source_id!r} has no historical installation adapter"
        )
    return resolve_callable_reference(adapter.loader_binding)


def resolve_planner_client_builder(
    source_name: str,
) -> PlannerClientBuilderCallable | None:
    """Resolve the optional provider client needed during shard planning."""

    source = source_definition(source_name)
    adapter = source.installation_adapter
    if adapter is None or adapter.loader_binding is None:
        raise InstallationBindingNotFoundError(
            f"source {source.source_id!r} has no historical installation adapter"
        )
    reference = adapter.planner_client_builder_binding
    return resolve_callable_reference(reference) if reference is not None else None


def resolve_onboarding_failure_handler(
    source_name: str,
) -> OnboardingFailureCallable | None:
    """Resolve the optional source-specific onboarding failure side effect."""

    source = source_definition(source_name)
    adapter = source.installation_adapter
    if adapter is None or adapter.loader_binding is None:
        raise InstallationBindingNotFoundError(
            f"source {source.source_id!r} has no historical installation adapter"
        )
    reference = adapter.onboarding_failure_binding
    return resolve_callable_reference(reference) if reference is not None else None


def resolve_webhook_verifier(route_id: str) -> WebhookVerifierCallable:
    """Resolve the verifier callable declared for a webhook route."""

    ingress = webhook_ingress_definition(route_id)
    return resolve_callable_reference(ingress.verifier_binding)


def resolve_webhook_tenant_extractor(
    route_id: str,
) -> WebhookTenantExtractorCallable:
    """Resolve the tenant-binding extractor declared for a webhook route."""

    ingress = webhook_ingress_definition(route_id)
    return resolve_callable_reference(ingress.tenant_extractor_binding)


def resolve_dedicated_ingress_dispatcher(
    ingress_id: str,
) -> DedicatedIngressCallable:
    """Resolve a provider-specific ingress dispatcher."""

    ingress = dedicated_ingress_definition(ingress_id)
    return resolve_callable_reference(ingress.dispatcher_binding)


def resolve_dedicated_ingress_router_factory(
    ingress_id: str,
) -> DedicatedIngressCallable:
    """Resolve the router factory that mounts a dedicated ingress."""

    ingress = dedicated_ingress_definition(ingress_id)
    return resolve_callable_reference(ingress.router_factory_binding)


@lru_cache(maxsize=1)
def validate_runtime_bindings() -> None:
    """Resolve every callable declared by the canonical source catalog.

    The catalog itself remains dependency-light and performs structural
    validation at import time. Runtime entry points call this cached guard
    before accepting work so a missing module, renamed function, or
    non-callable binding fails startup instead of failing one source later.
    """

    references = set(NORMALIZER_BINDING_CATALOG.values())
    for provider in PROVIDER_DEFINITIONS:
        for ingress in provider.oauth_ingresses:
            references.update(
                (
                    ingress.install_handler_binding,
                    ingress.callback_handler_binding,
                )
            )
        for ingress in provider.webhook_ingresses:
            references.update(
                reference
                for reference in (
                    ingress.verifier_binding,
                    ingress.tenant_extractor_binding,
                    ingress.verification_handshake_binding,
                    ingress.verification_handshake_handler_binding,
                    ingress.dedicated_handler_binding,
                )
                if reference is not None
            )
        for ingress in provider.dedicated_ingresses:
            references.update(ingress.verification_bindings)
            references.update(
                (
                    ingress.tenant_resolver_binding,
                    ingress.dispatcher_binding,
                    ingress.router_factory_binding,
                )
            )
    for source in SOURCE_DEFINITIONS:
        references.update(source.idempotency_builder_bindings)
        references.update(
            reference
            for reference in (
                source.planner_binding,
                source.fetcher_binding,
                source.reconciler_binding,
                source.connect_router_binding,
            )
            if reference is not None
        )
        adapter = source.installation_adapter
        if adapter is not None:
            references.update(
                reference
                for reference in (
                    adapter.loader_binding,
                    adapter.planner_client_builder_binding,
                    adapter.onboarding_failure_binding,
                )
                if reference is not None
            )

    for reference in sorted(references):
        resolve_callable_reference(reference)


def _collect_overrides(
    role: HistoryBindingRole,
    values: Mapping[str, HistoryCallable] | None,
) -> dict[_OverrideKey, HistoryCallable]:
    collected: dict[_OverrideKey, HistoryCallable] = {}
    for source_name, value in (values or {}).items():
        source = source_definition(source_name)
        if source.history is None:
            raise HistoryNotSupportedError(
                f"source {source.source_id!r} has no historical ingestion contract"
            )
        if not callable(value):
            raise TypeError(
                f"{role} override for {source.source_id!r} must be callable"
            )
        collected[(source.source_id, role)] = value
    return collected


@contextmanager
def override_history_bindings(
    *,
    planners: Mapping[str, HistoryCallable] | None = None,
    fetchers: Mapping[str, HistoryCallable] | None = None,
    reconcilers: Mapping[str, HistoryCallable] | None = None,
) -> Iterator[None]:
    """Temporarily override source bindings in the current context.

    Overrides are inherited by child asyncio tasks through ``ContextVar`` but
    never mutate the production catalog or leak after the context exits.
    Nested scopes restore the exact previous mapping.
    """

    updates: dict[_OverrideKey, HistoryCallable] = {}
    updates.update(_collect_overrides("planner", planners))
    updates.update(_collect_overrides("fetcher", fetchers))
    updates.update(_collect_overrides("reconciler", reconcilers))
    if not updates:
        raise ValueError("at least one history binding override is required")

    merged = dict(_OVERRIDES.get())
    merged.update(updates)
    token = _OVERRIDES.set(MappingProxyType(merged))
    try:
        yield
    finally:
        _OVERRIDES.reset(token)


__all__ = [
    "BindingResolutionError",
    "DedicatedIngressCallable",
    "HistoryBindingRole",
    "HistoryCallable",
    "HistoryNotSupportedError",
    "IdempotencyBuilderCallable",
    "InstallationBindingNotFoundError",
    "InstallationLoaderCallable",
    "NormalizationChannelNotFoundError",
    "NormalizerCallable",
    "OnboardingFailureCallable",
    "PlannerClientBuilderCallable",
    "override_history_bindings",
    "resolve_callable_reference",
    "resolve_dedicated_ingress_dispatcher",
    "resolve_dedicated_ingress_router_factory",
    "resolve_fetcher",
    "resolve_history_binding",
    "resolve_handler",
    "resolve_idempotency_builders",
    "resolve_installation_loader",
    "resolve_onboarding_failure_handler",
    "resolve_planner",
    "resolve_planner_client_builder",
    "resolve_reconciler",
    "resolve_webhook_tenant_extractor",
    "resolve_webhook_verifier",
    "split_callable_reference",
    "validate_runtime_bindings",
]
