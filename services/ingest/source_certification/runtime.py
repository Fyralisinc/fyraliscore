"""Lazy resolution for source-owned certification-kit callables."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, cast

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_SPECS,
    source_certification_spec,
)
from services.ingest.source_certification.models import (
    CertificationBindingRole,
    CertificationCallableBinding,
    SourceCertificationSpec,
)
from services.ingest.source_contract.catalog import source_definition
from services.ingest.source_contract.runtime import (
    BindingResolutionError,
    resolve_callable_reference,
)


_SOURCE_ATTRIBUTE = "__fyralis_certification_source_id__"
_ROLE_ATTRIBUTE = "__fyralis_certification_binding_role__"
_CallableT = TypeVar("_CallableT", bound=Callable[..., Any])


class CertificationBindingResolutionError(BindingResolutionError):
    """A certification-kit binding is absent, invalid, or mis-owned."""


class CertificationHistoryUnsupportedError(
    CertificationBindingResolutionError,
):
    """The source explicitly has no historical certification surface."""


def certification_callable(
    *,
    source_id: str,
    role: CertificationBindingRole,
) -> Callable[[_CallableT], _CallableT]:
    """Mark a callable with its immutable source and certification role."""

    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("certification callable source_id must be non-empty")
    if role not in {"fixture_factory", "installation_seeder"}:
        raise ValueError(f"unknown certification callable role {role!r}")

    def _decorate(value: _CallableT) -> _CallableT:
        existing_source = getattr(value, _SOURCE_ATTRIBUTE, source_id)
        existing_role = getattr(value, _ROLE_ATTRIBUTE, role)
        if existing_source != source_id or existing_role != role:
            raise ValueError(
                f"callable {value!r} is already owned by "
                f"{existing_source!r}/{existing_role!r}"
            )
        setattr(value, _SOURCE_ATTRIBUTE, source_id)
        setattr(value, _ROLE_ATTRIBUTE, role)
        return value

    return _decorate


def _binding_for(
    spec: SourceCertificationSpec,
    role: CertificationBindingRole,
) -> CertificationCallableBinding:
    source = source_definition(spec.source_id)
    if source.history is None:
        raise CertificationHistoryUnsupportedError(
            f"source {source.source_id!r} explicitly does not support history"
        )
    binding = (
        spec.fixture_factory_binding
        if role == "fixture_factory"
        else spec.installation_seeder_binding
    )
    if binding is None:
        raise CertificationBindingResolutionError(
            f"source {source.source_id!r} supports history but has no "
            f"{role} certification binding"
        )
    if binding.source_id != source.source_id or binding.role != role:
        raise CertificationBindingResolutionError(
            f"source {source.source_id!r} has mismatched {role} binding "
            f"{binding.source_id!r}/{binding.role!r}"
        )
    return binding


def _resolve(
    source_name: str,
    role: CertificationBindingRole,
) -> Callable[..., Any]:
    spec = source_certification_spec(source_name)
    binding = _binding_for(spec, role)
    value = resolve_callable_reference(binding.reference)
    callable_source = getattr(value, _SOURCE_ATTRIBUTE, None)
    callable_role = getattr(value, _ROLE_ATTRIBUTE, None)
    if callable_source != spec.source_id or callable_role != role:
        raise CertificationBindingResolutionError(
            f"binding {binding.reference!r} resolved to a callable owned by "
            f"{callable_source!r}/{callable_role!r}, expected "
            f"{spec.source_id!r}/{role!r}"
        )
    return value


def resolve_fixture_factory(source_name: str) -> Callable[..., dict[str, Any]]:
    """Resolve one history source's deterministic Provider Lab fixture."""

    return cast(Callable[..., dict[str, Any]], _resolve(source_name, "fixture_factory"))


def resolve_installation_seeder(source_name: str) -> Callable[..., Any]:
    """Resolve one history source's tenant/install/onboarding row seeder."""

    return _resolve(source_name, "installation_seeder")


def validate_certification_bindings() -> tuple[str, ...]:
    """Resolve and ownership-check every canonical certification binding."""

    resolved: list[str] = []
    for spec in SOURCE_CERTIFICATION_SPECS:
        source = source_definition(spec.source_id)
        if source.history is None:
            if (
                spec.fixture_factory_binding is not None
                or spec.installation_seeder_binding is not None
            ):
                raise CertificationBindingResolutionError(
                    f"history-unsupported source {source.source_id!r} "
                    "declares certification bindings"
                )
            continue
        resolve_fixture_factory(source.source_id)
        resolve_installation_seeder(source.source_id)
        resolved.append(source.source_id)
    return tuple(resolved)


__all__ = [
    "CertificationBindingResolutionError",
    "CertificationHistoryUnsupportedError",
    "certification_callable",
    "resolve_fixture_factory",
    "resolve_installation_seeder",
    "validate_certification_bindings",
]
