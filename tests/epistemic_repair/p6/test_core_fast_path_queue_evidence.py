from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.evaluation.epistemic_repair.core_fast_path_queue_evidence import (
    proven_batch_observation_ids,
)


class _Connection:
    def __init__(self, run, parent, members) -> None:
        self.run = run
        self.parent = parent
        self.members = members

    async def fetchrow(self, query, *_args):
        return self.run if "FROM think_runs" in query else self.parent

    async def fetch(self, _query, *_args):
        return self.members


def _fixture():
    parent_id = uuid4()
    member_ids = [uuid4(), uuid4()]
    observation_ids = [uuid4(), uuid4()]
    completed = datetime.now(timezone.utc)
    label = "p6-batch-1"
    run = {
        "trigger_id": parent_id,
        "status": "success",
        "trigger_kind": "T1:event_batch",
    }
    parent = {
        "id": parent_id,
        "payload": {
            "batch_member_trigger_ids": [str(value) for value in member_ids],
            "member_trigger_ids": [str(value) for value in member_ids],
        },
        "completed_at": completed,
        "trigger_kind": "T1",
        "trigger_subkind": "event_batch",
    }
    members = [
        {
            "id": member_id,
            "observation_id": observation_id,
            "payload": {"mega_probe": {"run_id": label}},
            "completed_at": completed,
            "batch_parent_id": parent_id,
        }
        for member_id, observation_id in zip(
            member_ids, observation_ids, strict=True,
        )
    ]
    return run, parent, members, set(observation_ids), label


@pytest.mark.asyncio
async def test_proves_only_exact_completed_declared_batch_membership() -> None:
    run, parent, members, expected, label = _fixture()

    result = await proven_batch_observation_ids(
        _Connection(run, parent, members),
        tenant_id=uuid4(),
        run_id=uuid4(),
        expected_observation_ids=expected,
        batch_label=label,
    )

    assert result == tuple(sorted(expected, key=str))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    (
        "failed_run",
        "open_parent",
        "open_member",
        "extra_member",
        "missing_expected",
        "wrong_parent_manifest",
        "wrong_label",
    ),
)
async def test_fails_closed_on_any_queue_membership_mismatch(damage: str) -> None:
    run, parent, members, expected, label = _fixture()
    if damage == "failed_run":
        run["status"] = "failed"
    elif damage == "open_parent":
        parent["completed_at"] = None
    elif damage == "open_member":
        members[0]["completed_at"] = None
    elif damage == "extra_member":
        extra_id, extra_observation = uuid4(), uuid4()
        members.append({
            "id": extra_id,
            "observation_id": extra_observation,
            "payload": {"mega_probe": {"run_id": label}},
            "completed_at": datetime.now(timezone.utc),
            "batch_parent_id": parent["id"],
        })
        parent["payload"]["batch_member_trigger_ids"].append(str(extra_id))
        parent["payload"]["member_trigger_ids"].append(str(extra_id))
    elif damage == "missing_expected":
        expected.pop()
    elif damage == "wrong_parent_manifest":
        parent["payload"]["batch_member_trigger_ids"].pop()
    elif damage == "wrong_label":
        members[0]["payload"]["mega_probe"]["run_id"] = "another-batch"

    assert await proven_batch_observation_ids(
        _Connection(run, parent, members),
        tenant_id=uuid4(),
        run_id=uuid4(),
        expected_observation_ids=expected,
        batch_label=label,
    ) == ()
