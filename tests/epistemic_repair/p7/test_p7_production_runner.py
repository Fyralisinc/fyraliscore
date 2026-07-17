from __future__ import annotations

from uuid import uuid4

import pytest

from lib.evaluation.epistemic_repair.p7_production_runner import P7_ARMS, _run_id
from lib.shared.errors import InvariantViolation


def test_p7_arm_set_is_preregistered_and_complete() -> None:
    assert P7_ARMS == (
        "adaptive",
        "frozen",
        "observation_only",
        "memory_hidden",
        "corrupted",
    )


def test_run_id_requires_successful_durable_production_run() -> None:
    run_id = uuid4()
    assert _run_id({"run": {"id": str(run_id), "status": "success"}}) == run_id
    with pytest.raises(InvariantViolation, match="successful durable Think run"):
        _run_id({"run": {"id": str(run_id), "status": "failed"}})
    with pytest.raises(InvariantViolation, match="successful durable Think run"):
        _run_id({"run": None})
