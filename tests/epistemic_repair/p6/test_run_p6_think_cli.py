from __future__ import annotations

from argparse import Namespace
import json
from types import SimpleNamespace

import pytest

from scripts import run_epistemic_repair_p6_think as run_cli


def _manifest() -> dict:
    return {
        "manifest_ref": "founder-manifest:sample:v1",
        "authority_ref": "founder-authority:sample:v1",
        "asserted_by_ref": "actor:founder-1",
        "provenance_refs": ["founder-interview:sample:v1"],
        "effective_at": "2025-07-10T08:00:00Z",
        "entries": [{
            "canonical_ref": {
                "type": "workstream", "id": "workstream:sample", "version": 1,
            },
            "canonical_name": "Sample initiative",
            "aliases": ["Sample work"],
        }],
    }


def test_load_founder_manifest_builds_caller_owned_preparer(tmp_path) -> None:
    path = tmp_path / "founder.json"
    path.write_text(json.dumps(_manifest()), encoding="utf-8")

    preparer = run_cli._load_founder_manifest(path)

    assert preparer.manifest_ref == "founder-manifest:sample:v1"
    assert preparer.authority_ref == "founder-authority:sample:v1"
    assert preparer.asserted_by_ref == "actor:founder-1"
    assert preparer.provenance_refs == ("founder-interview:sample:v1",)
    assert preparer.entries[0].canonical_name == "Sample initiative"
    assert preparer.entries[0].aliases == ("Sample work",)
    assert preparer.effective_at.isoformat() == "2025-07-10T08:00:00+00:00"


@pytest.mark.asyncio
@pytest.mark.parametrize("with_manifest", [False, True])
async def test_run_forwards_manifest_preparer_only_when_requested(
    monkeypatch: pytest.MonkeyPatch, tmp_path, with_manifest: bool,
) -> None:
    manifest_path = tmp_path / "founder.json"
    manifest_path.write_text(json.dumps(_manifest()), encoding="utf-8")
    captured = {}

    async def fake_run(**kwargs):
        captured.update(kwargs)
        return {"complete": True, "completed_batches": 1, "terminal_reason": None}

    monkeypatch.setattr(run_cli, "run_p6_production_think", fake_run)
    status = await run_cli._run(Namespace(
        database_url="postgresql://unused",
        output=tmp_path / "output.json",
        batch_timeout=10.0,
        attempt_timeout=5.0,
        total_timeout=20.0,
        max_batches=1,
        founder_manifest=manifest_path if with_manifest else None,
    ))

    assert status == 0
    preparer = captured["prepare_persisted_batch"]
    if with_manifest:
        assert preparer is not None
        assert preparer.manifest_ref == "founder-manifest:sample:v1"
        written = json.loads((tmp_path / "output.json").read_text())
        evidence = written["founder_identity_bootstrap"]
        assert evidence["manifest_ref"] == "founder-manifest:sample:v1"
        assert evidence["canonical_entry_count"] == 1
        assert evidence["alias_count"] is None
        assert evidence["applied_before_enqueue"] is False
        assert evidence["semantic_truth_unchanged"] is None
        assert evidence["no_behavioral_models_seeded"] is True
        assert len(evidence["manifest_file_sha256"]) == 64
    else:
        assert preparer is None
        assert not (tmp_path / "output.json").exists()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("entries", [], "entries must be a non-empty array"),
        ("provenance_refs", [], "provenance_refs must be a non-empty array"),
        ("effective_at", "not-a-time", "effective_at must be ISO-8601"),
        ("effective_at", "2025-07-10T08:00:00", "must include a timezone"),
        (
            "effective_at",
            "2025-07-10T09:00:01Z",
            "must not be later than the first selected P6 observation",
        ),
    ],
)
def test_manifest_shape_errors_fail_before_execution(
    tmp_path, field: str, value: object, message: str,
) -> None:
    payload = _manifest()
    payload[field] = value
    path = tmp_path / "founder.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        run_cli._load_founder_manifest(path)


@pytest.mark.asyncio
async def test_future_effective_at_fails_before_production_construction(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    payload = _manifest()
    payload["effective_at"] = "2025-07-10T09:00:01Z"
    path = tmp_path / "founder.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    production_called = False

    async def forbidden_run(**_kwargs):
        nonlocal production_called
        production_called = True
        raise AssertionError("production runner must follow manifest preflight")

    monkeypatch.setattr(run_cli, "run_p6_production_think", forbidden_run)
    with pytest.raises(ValueError, match="first selected P6 observation"):
        await run_cli._run(Namespace(
            database_url="postgresql://unused",
            output=tmp_path / "output.json",
            batch_timeout=10.0,
            attempt_timeout=5.0,
            total_timeout=20.0,
            max_batches=1,
            founder_manifest=path,
        ))

    assert production_called is False


@pytest.mark.asyncio
async def test_checkpoint_consumes_database_backed_bootstrap_receipt(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    path = tmp_path / "founder.json"
    path.write_text(json.dumps(_manifest()), encoding="utf-8")
    counts = {
        "models": 0,
        "accepted_relation_instances": 0,
        "active_model_edges": 0,
        "observations": 25,
        "resources": 0,
    }
    receipt = {
        "manifest_ref": "founder-manifest:sample:v1",
        "alias_count": 2,
        "applied_before_enqueue": True,
        "semantic_truth_unchanged": True,
        "counts_before": counts,
        "counts_after": counts,
        "semantic_deltas": {key: 0 for key in counts},
    }
    preparer = SimpleNamespace(
        receipt=receipt,
        manifest_ref="founder-manifest:sample:v1",
        entries=(object(),),
    )

    async def fake_run(**_kwargs):
        return {"complete": True, "completed_batches": 1, "terminal_reason": None}

    monkeypatch.setattr(run_cli, "_load_founder_manifest", lambda *_args, **_kwargs: preparer)
    monkeypatch.setattr(run_cli, "run_p6_production_think", fake_run)
    await run_cli._run(Namespace(
        database_url="postgresql://unused",
        output=tmp_path / "output.json",
        batch_timeout=10.0,
        attempt_timeout=5.0,
        total_timeout=20.0,
        max_batches=1,
        founder_manifest=path,
    ))

    evidence = json.loads((tmp_path / "output.json").read_text())[
        "founder_identity_bootstrap"
    ]
    for key, value in receipt.items():
        assert evidence[key] == value
    assert evidence["no_behavioral_models_seeded"] is True
