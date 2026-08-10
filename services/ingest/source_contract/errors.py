"""Typed error taxonomy at the connector boundary."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, ClassVar


class ConnectorError(Exception):
    """Base class for source-contract failures.

    ``details`` must contain redacted diagnostic metadata only. It is frozen so
    callers cannot mutate an error after it has entered logs or diagnostics.
    """

    code: ClassVar[str] = "connector_error"
    retryable: ClassVar[bool] = False

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = MappingProxyType(dict(details or {}))

    def __str__(self) -> str:
        return self.message


class InvalidConfigurationError(ConnectorError):
    code = "invalid_configuration"


class AuthenticationRejectedError(ConnectorError):
    code = "authentication_rejected"


class PermissionDeniedError(ConnectorError):
    code = "permission_denied"


class ResourceNotFoundError(ConnectorError):
    code = "resource_not_found"


class RateLimitedError(ConnectorError):
    code = "rate_limited"
    retryable = True


class TransientSourceError(ConnectorError):
    code = "transient_source_failure"
    retryable = True


class SourceUnavailableError(ConnectorError):
    code = "source_unavailable"
    retryable = True


class PayloadRejectedError(ConnectorError):
    code = "payload_rejected"


class StateIncompatibleError(ConnectorError):
    code = "state_incompatible"


class ContractViolationError(ConnectorError):
    code = "contract_violation"


class OperationCancelledError(ConnectorError):
    code = "cancelled"


class ManifestValidationError(ContractViolationError):
    code = "manifest_validation_failed"


class RegistryBuildError(ContractViolationError):
    code = "registry_build_failed"


class DuplicateConnectorError(RegistryBuildError):
    code = "duplicate_connector"


class ContractIncompatibleError(RegistryBuildError):
    code = "contract_incompatible"


class CapabilityMismatchError(RegistryBuildError):
    code = "capability_mismatch"


class ConnectorNotFoundError(ConnectorError):
    code = "connector_not_found"


class BindingError(ContractViolationError):
    code = "connector_binding_failed"


class CapabilityUnavailableError(ConnectorError):
    code = "capability_unavailable"


__all__ = [
    "AuthenticationRejectedError",
    "BindingError",
    "CapabilityMismatchError",
    "CapabilityUnavailableError",
    "ConnectorError",
    "ConnectorNotFoundError",
    "ContractIncompatibleError",
    "ContractViolationError",
    "DuplicateConnectorError",
    "InvalidConfigurationError",
    "ManifestValidationError",
    "OperationCancelledError",
    "PayloadRejectedError",
    "PermissionDeniedError",
    "RateLimitedError",
    "RegistryBuildError",
    "ResourceNotFoundError",
    "SourceUnavailableError",
    "StateIncompatibleError",
    "TransientSourceError",
]
