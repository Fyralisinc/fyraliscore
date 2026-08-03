"""Translation from connector failures to runtime-owned actions."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum

from services.ingest.source_contract.errors import (
    AuthenticationRejectedError,
    BindingError,
    CapabilityUnavailableError,
    ConnectorError,
    ContractViolationError,
    InvalidConfigurationError,
    OperationCancelledError,
    PayloadRejectedError,
    PermissionDeniedError,
    RateLimitedError,
    ResourceNotFoundError,
    SourceUnavailableError,
    TransientSourceError,
)


class RuntimeFailureAction(StrEnum):
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"
    FAIL_INSTALLATION = "fail_installation"
    FAIL_CLOSED = "fail_closed"
    CANCEL = "cancel"


@dataclass(frozen=True)
class TranslatedFailure:
    code: str
    retryable: bool
    action: RuntimeFailureAction
    category: str


class RuntimeConnectorFailure(Exception):
    """Stable runtime exception that preserves the original as ``__cause__``."""

    def __init__(self, message: str, translated: TranslatedFailure) -> None:
        super().__init__(message)
        self.translated = translated

    @property
    def retryable(self) -> bool:
        return self.translated.retryable

    @property
    def recoverable(self) -> bool:
        """Compatibility alias consumed by current worker retry boundaries."""

        return self.translated.retryable


def classify_failure(exc: BaseException) -> TranslatedFailure:
    if isinstance(exc, OperationCancelledError | asyncio.CancelledError):
        return TranslatedFailure(
            "cancelled", False, RuntimeFailureAction.CANCEL, "cancellation"
        )
    if isinstance(exc, asyncio.TimeoutError):
        return TranslatedFailure(
            "connector_deadline_exceeded",
            True,
            RuntimeFailureAction.RETRY,
            "timeout",
        )
    if isinstance(exc, RateLimitedError):
        return TranslatedFailure(
            exc.code, True, RuntimeFailureAction.RETRY, "rate_limit"
        )
    if isinstance(exc, TransientSourceError | SourceUnavailableError):
        return TranslatedFailure(
            exc.code, True, RuntimeFailureAction.RETRY, "source_transient"
        )
    if isinstance(exc, PayloadRejectedError):
        return TranslatedFailure(
            exc.code, False, RuntimeFailureAction.DEAD_LETTER, "payload"
        )
    if isinstance(
        exc,
        AuthenticationRejectedError
        | PermissionDeniedError
        | InvalidConfigurationError
        | ResourceNotFoundError,
    ):
        return TranslatedFailure(
            exc.code,
            False,
            RuntimeFailureAction.FAIL_INSTALLATION,
            "installation",
        )
    if isinstance(exc, BindingError | CapabilityUnavailableError | ContractViolationError):
        return TranslatedFailure(
            exc.code, False, RuntimeFailureAction.FAIL_CLOSED, "contract"
        )
    if isinstance(exc, ConnectorError):
        return TranslatedFailure(
            exc.code,
            exc.retryable,
            RuntimeFailureAction.RETRY
            if exc.retryable
            else RuntimeFailureAction.FAIL_CLOSED,
            "connector",
        )
    if bool(getattr(exc, "recoverable", False)):
        return TranslatedFailure(
            str(getattr(exc, "code", "legacy_recoverable_failure")),
            True,
            RuntimeFailureAction.RETRY,
            "legacy_recoverable",
        )
    return TranslatedFailure(
        "connector_unexpected_failure",
        False,
        RuntimeFailureAction.FAIL_CLOSED,
        "unexpected",
    )


def translate_failure(exc: BaseException) -> RuntimeConnectorFailure:
    translated = classify_failure(exc)
    return RuntimeConnectorFailure(
        f"connector execution failed ({translated.code})", translated
    )

__all__ = [
    "RuntimeConnectorFailure",
    "RuntimeFailureAction",
    "TranslatedFailure",
    "classify_failure",
    "translate_failure",
]
