"""Tests for lib/shared/errors.py."""
from __future__ import annotations

import json

from hypothesis import given, strategies as st

from lib.shared.errors import (
    CalibrationMissingError,
    CompanyOSError,
    FalsifierInadequateError,
    InvariantViolation,
    SchemaDriftError,
    TrustTierError,
    ValidationError,
)


def test_all_errors_subclass_root():
    for cls in (
        ValidationError,
        InvariantViolation,
        SchemaDriftError,
        TrustTierError,
        FalsifierInadequateError,
        CalibrationMissingError,
    ):
        e = cls.__name__
        assert issubclass(cls, CompanyOSError), f"{e} is not a CompanyOSError"


def test_company_os_error_context_default_empty():
    e = CompanyOSError("boom")
    assert e.message == "boom"
    assert e.context == {}
    assert e.code == "company_os_error"


def test_company_os_error_context_captured():
    e = CompanyOSError("boom", tenant_id="t1", model_id="m-99")
    assert e.context == {"tenant_id": "t1", "model_id": "m-99"}


def test_to_dict_is_json_serialisable():
    e = CompanyOSError("boom", key="value", number=42)
    payload = e.to_dict()
    assert payload == {
        "code": "company_os_error",
        "message": "boom",
        "context": {"key": "value", "number": 42},
    }
    # must be json-serialisable
    assert json.loads(json.dumps(payload))["code"] == "company_os_error"


def test_validation_error_code():
    e = ValidationError("bad field", field="confidence")
    assert e.code == "validation_error"
    assert e.context["field"] == "confidence"


def test_invariant_violation_carries_name():
    e = InvariantViolation("C1", "owner required", commitment_id="c-1")
    assert e.invariant == "C1"
    assert e.context["invariant"] == "C1"
    assert e.context["commitment_id"] == "c-1"
    assert e.code == "invariant_violation"


def test_schema_drift_error_code():
    e = SchemaDriftError("column missing", table="models", column="confirmed_count")
    assert e.code == "schema_drift"
    assert e.context["table"] == "models"


def test_trust_tier_error_captures_required_actual():
    e = TrustTierError(required="authoritative", actual="inferential")
    assert e.required == "authoritative"
    assert e.actual == "inferential"
    assert "required" in str(e.message)
    assert e.code == "trust_tier_error"


def test_trust_tier_error_accepts_custom_message():
    e = TrustTierError(
        required="authoritative",
        actual="unvetted",
        message="doneverified requires authoritative evidence",
        commitment_id="c-42",
    )
    assert e.message == "doneverified requires authoritative evidence"
    assert e.context["commitment_id"] == "c-42"


def test_falsifier_inadequate_error_carries_reason_and_body():
    body = {"kind": "observation_pattern", "pattern": "short"}
    e = FalsifierInadequateError("pattern too vague", falsifier=body)
    assert e.reason == "pattern too vague"
    assert e.falsifier == body
    assert e.code == "falsifier_inadequate"
    assert e.context["falsifier"] == body


def test_calibration_missing_error_formats_message():
    e = CalibrationMissingError(actor_id="a-1", proposition_kind="prediction")
    assert "a-1" in e.message
    assert "prediction" in e.message
    assert e.code == "calibration_missing"
    assert e.context["proposition_kind"] == "prediction"


def test_raising_and_catching_as_base():
    try:
        raise ValidationError("x", field="f")
    except CompanyOSError as e:
        assert isinstance(e, ValidationError)
        assert e.context["field"] == "f"


def test_repr_contains_type_and_message():
    e = ValidationError("bad", field="confidence")
    r = repr(e)
    assert "ValidationError" in r
    assert "bad" in r
    assert "confidence" in r


def test_error_code_is_per_class_not_per_instance():
    a = ValidationError("a")
    b = ValidationError("b")
    assert a.code == b.code == "validation_error"


def test_error_context_isolated_per_instance():
    a = ValidationError("a", field="x")
    b = ValidationError("b", field="y")
    assert a.context["field"] == "x"
    assert b.context["field"] == "y"
    # Mutating one does not affect the other.
    a.context["new"] = 1
    assert "new" not in b.context


@given(st.text(min_size=1), st.dictionaries(st.text(min_size=1, max_size=20), st.integers()))
def test_property_roundtrip_json(message: str, ctx: dict):
    e = CompanyOSError(message, **ctx)
    payload = e.to_dict()
    round_tripped = json.loads(json.dumps(payload, default=str))
    assert round_tripped["message"] == message


def test_invariant_violation_default_code():
    e = InvariantViolation("G2", "goal cycle")
    assert e.code == "invariant_violation"


def test_errors_chain_properly():
    """CompanyOSError supports `raise X from Y` chaining."""
    try:
        try:
            raise ValueError("underlying")
        except ValueError as underlying:
            raise ValidationError("surface error", underlying=str(underlying)) from underlying
    except ValidationError as e:
        assert e.__cause__ is not None
        assert isinstance(e.__cause__, ValueError)
        assert e.context["underlying"] == "underlying"
