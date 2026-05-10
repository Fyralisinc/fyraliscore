"""
lib/shared/errors.py — shared error types with structured context.

Every error carries a `context: dict[str, Any]` dictionary. The
context is what downstream log emitters and retry machinery read.
Carrying context structurally (not only in the message) is how we
build uniform observability across services.
"""
from __future__ import annotations

from typing import Any


class CompanyOSError(Exception):
    """Root of every domain-level exception. Never raised directly."""

    default_code: str = "company_os_error"

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = dict(context)

    @property
    def code(self) -> str:
        return getattr(self, "_code", self.default_code)

    def to_dict(self) -> dict[str, Any]:
        """
        Serialisable form used by structured loggers, HTTP error
        responses, and the Think failure ledger.
        """
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.message!r}, context={self.context!r})"


# ---------------------------------------------------------------------
# Validation & invariants
# ---------------------------------------------------------------------

class ValidationError(CompanyOSError):
    """A payload failed schema or field validation. 4xx-class."""
    default_code = "validation_error"


class InvariantViolation(CompanyOSError):
    """
    A domain invariant (C1-C10, G1-G4, per spec §3) was violated.
    Raised at INSERT/transition time by services/acts/invariants.py
    and by the Think validator.
    """
    default_code = "invariant_violation"

    def __init__(
        self,
        invariant: str,
        message: str,
        **context: Any,
    ) -> None:
        super().__init__(message, invariant=invariant, **context)
        self.invariant = invariant


# ---------------------------------------------------------------------
# Schema / storage
# ---------------------------------------------------------------------

class SchemaDriftError(CompanyOSError):
    """
    Live database diverges from SCHEMA-LOCK.md. Raised by
    scripts/check_schema_drift.py when run in fail-fast mode from
    inside a service (e.g. at startup).
    """
    default_code = "schema_drift"


# ---------------------------------------------------------------------
# Trust / calibration / falsifier
# ---------------------------------------------------------------------

class TrustTierError(CompanyOSError):
    """
    An operation required a minimum trust tier that the present
    signal did not satisfy. E.g. Commitment transition to
    `doneverified` with a non-authoritative resolved_by_event.
    """
    default_code = "trust_tier_error"

    def __init__(
        self,
        required: str,
        actual: str,
        message: str | None = None,
        **context: Any,
    ) -> None:
        super().__init__(
            message or f"required trust tier {required}; got {actual}",
            required=required,
            actual=actual,
            **context,
        )
        self.required = required
        self.actual = actual


class FalsifierInadequateError(CompanyOSError):
    """
    A Model with confidence > 0.7 was proposed without an adequate
    falsifier per spec §10 is_adequate_falsifier. See S2.1.
    """
    default_code = "falsifier_inadequate"

    def __init__(
        self,
        reason: str,
        falsifier: Any | None = None,
        **context: Any,
    ) -> None:
        super().__init__(reason, falsifier=falsifier, **context)
        self.reason = reason
        self.falsifier = falsifier


class MalformedFalsifierError(CompanyOSError):
    """
    A falsifier payload is structurally invalid — it has the right
    `kind` but at least one field cannot be parsed (e.g.
    `within_window` does not match either the ISO-8601 duration or
    human-readable grammar; `evaluate_at` is not a parseable
    timestamp; `check` does not match the prediction-deadline
    grammar).

    Distinct from `FalsifierInadequateError`, which signals a falsifier
    that is well-formed but too vague (pattern < 20 chars, missing
    `within_window`, etc.). Inadequate is a content-quality judgment;
    malformed is a parser failure.

    Surfacing this as a separate class lets the validator log a
    distinct `failure_reason='malformed_falsifier'` for observability,
    and lets call sites that want to repair the input (e.g. retry the
    LLM with a remediation hint) branch on the type rather than
    string-matching the message.
    """
    default_code = "falsifier_malformed"

    def __init__(
        self,
        reason: str,
        falsifier: Any | None = None,
        field: str | None = None,
        value: Any | None = None,
        **context: Any,
    ) -> None:
        super().__init__(
            reason, falsifier=falsifier, field=field, value=value, **context,
        )
        self.reason = reason
        self.falsifier = falsifier
        self.field = field
        self.value = value


class CalibrationMissingError(CompanyOSError):
    """
    A confidence adjustment was attempted but no calibration offset
    exists for the (actor, proposition_kind) pair and no cold-start
    default is configured. Typically raised during Think.validate.
    """
    default_code = "calibration_missing"

    def __init__(
        self,
        actor_id: Any,
        proposition_kind: str,
        **context: Any,
    ) -> None:
        super().__init__(
            f"no calibration offset for actor={actor_id} "
            f"proposition_kind={proposition_kind}",
            actor_id=str(actor_id),
            proposition_kind=proposition_kind,
            **context,
        )
        self.actor_id = actor_id
        self.proposition_kind = proposition_kind


__all__ = [
    "CompanyOSError",
    "ValidationError",
    "InvariantViolation",
    "SchemaDriftError",
    "TrustTierError",
    "FalsifierInadequateError",
    "MalformedFalsifierError",
    "CalibrationMissingError",
]
