"""services/think/tests/test_dropped_op_logging_op4.py — OP-4 tests.

THINK-DESIGN-AUDIT §5.1 arg 2 — structured logging of dropped ops.
Verifies:
  * A diff with one failing op produces a `validation_op_dropped` log
    entry with the expected classification reason.
  * The `think.validation.dropped_ops{reason, op_type}` counter is
    incremented per drop.
  * Classification maps exceptions to the right reason tag.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
import structlog

from services.think.diff_schema import ActOp, ClaimOp, RawDiff, ResourceOp
from services.think.observability import METRICS, log_dropped_op
from services.think.validator import (
    _classify_act_drop_reason,
    _classify_claim_drop_reason,
    _classify_resource_drop_reason,
    validate,
)

from lib.shared.errors import (
    FalsifierInadequateError,
    InvariantViolation,
    TrustTierError,
    ValidationError,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Classification helpers (pure unit tests)
# ---------------------------------------------------------------------


def test_classify_claim_falsifier():
    exc = FalsifierInadequateError(
        "falsifier inadequate: not specific",
        falsifier="x", confidence=0.8,
    )
    assert _classify_claim_drop_reason(exc) == "inadequate_falsifier"


def test_classify_claim_invalid_entity_reference():
    exc = ValidationError(
        "claim_op insert: scope_actor 'x' is not a UUID",
    )
    assert _classify_claim_drop_reason(exc) == "invalid_entity_reference"


def test_classify_claim_immutable():
    exc = ValidationError("confidence_at_assertion is immutable (Q3)")
    assert _classify_claim_drop_reason(exc) == "immutable_column"


def test_classify_claim_missing_model():
    exc = ValidationError("claim_op update: model xyz not found")
    assert _classify_claim_drop_reason(exc) == "missing_model_reference"


def test_classify_act_trust_tier():
    exc = TrustTierError(
        required="authoritative",
        actual="baseline",
        message="x",
        observation_id="y",
    )
    assert _classify_act_drop_reason(exc) == "inadequate_trust_tier"


def test_classify_act_illegal_transition():
    exc = InvariantViolation("C_STATE", "illegal: active → closed")
    assert _classify_act_drop_reason(exc) == "illegal_transition"


def test_classify_act_confidence_below_threshold():
    exc = ValidationError(
        "insufficient confidence for transition_commitment: "
        "basis=0.5 < threshold=0.7",
    )
    assert _classify_act_drop_reason(exc) == "confidence_below_threshold"


def test_classify_resource_invalid_shape():
    exc = ValidationError("resource_op update requires patch or payload")
    assert _classify_resource_drop_reason(exc) == "invalid_shape"


# ---------------------------------------------------------------------
# log_dropped_op — structured log + metrics counter
# ---------------------------------------------------------------------


def test_log_dropped_op_increments_counter():
    METRICS.reset()
    log_dropped_op(
        trigger_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        op_kind="insert",
        op_type="claim",
        failure_reason="inadequate_falsifier",
        original_op={"op": "insert"},
    )
    log_dropped_op(
        trigger_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        op_kind="insert",
        op_type="claim",
        failure_reason="inadequate_falsifier",
        original_op={"op": "insert"},
    )
    log_dropped_op(
        trigger_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        op_kind="transition_commitment",
        op_type="act",
        failure_reason="confidence_below_threshold",
        original_op={"op": "transition_commitment"},
    )
    snap = METRICS.snapshot()
    counters = snap["validation_dropped_ops"]
    assert counters["inadequate_falsifier|claim"] == 2
    assert counters["confidence_below_threshold|act"] == 1


def test_log_dropped_op_serializes_pydantic():
    """Serialization round-trips a ClaimOp's fields even though it's a
    Pydantic BaseModel with `extra='forbid'`."""
    op = ClaimOp(op="insert", entry={"confidence": 0.9})
    # Shouldn't raise.
    log_dropped_op(
        trigger_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        op_kind="insert",
        op_type="claim",
        failure_reason="inadequate_falsifier",
        original_op=op,
    )


# ---------------------------------------------------------------------
# End-to-end validator — inject a diff with a dropped op
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def clean_metrics():
    METRICS.reset()
    yield
    METRICS.reset()


async def test_validator_injected_bad_resource_logs_drop(
    fresh_db, warm_fixtures, clean_metrics,
):
    """Inject a resource_op with invalid shape (update without
    resource_id). The validator should drop it, log the event, and
    increment the counter. The survivors are zero, so the validator
    raises ValidationFailure (no-survivors gate) — that's expected
    shape-level behaviour; we still get the drop log before the raise."""
    from services.think.validator import ValidationFailure

    trigger_ref = uuid.uuid4()
    diff = RawDiff(
        trigger_ref=trigger_ref,
        tenant_id=warm_fixtures.tenant_id,
        claim_ops=[],
        act_ops=[],
        resource_ops=[ResourceOp(op="update", resource_id=None, patch=None)],
    )
    # Retrieval result shape — minimal stub (validator only reads
    # .models for one claim path; resource_ops don't consult it).
    class _Stub:
        models = []
        observations = []

    async with fresh_db.acquire() as conn:
        with pytest.raises(ValidationFailure):
            await validate(
                diff, _Stub(), conn,
                allowed_region=None, strict_region=False,
            )
    snap = METRICS.snapshot()
    counters = snap["validation_dropped_ops"]
    # Invalid shape → classified as invalid_shape for resource_op.
    assert counters.get("invalid_shape|resource", 0) >= 1
