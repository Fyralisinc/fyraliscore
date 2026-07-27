"""Focused gates for the opt-in writer attribution seam."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from services.ingest.ingestion.event_replica_attribution import (
    EVENT_ATTRIBUTION_METADATA_KEY,
    EventAttributionStamp,
    MissingWriterReplicaId,
)
from services.ingest.ingestion.normalizer.models import NormalizedEnvelope
from services.ingest.ingestion.writers import observation_writer as writer


pytestmark = pytest.mark.asyncio
_NOW = dt.datetime(2026, 7, 27, 12, 0, tzinfo=dt.timezone.utc)
_TENANT_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
_INSTALLATION_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


def _envelope(*, stamped: bool = True) -> NormalizedEnvelope:
    metadata: dict[str, Any] = {}
    if stamped:
        metadata[EVENT_ATTRIBUTION_METADATA_KEY] = EventAttributionStamp(
            trial_namespace="pipeline:github:trial-7",
            installation_id=_INSTALLATION_ID,
            event_id="github:7:41:0:1:1",
            operation_id="issues.list",
        ).to_ingress_metadata()
    return NormalizedEnvelope(
        source="github",
        ingress_kind="backfill",
        tenant_id=_TENANT_ID,
        raw_s3_key="test/github/raw.json",
        content_hash="a" * 64,
        raw_ingested_at=_NOW,
        source_channel="github:issue",
        content_text="issue",
        content={"action": "opened"},
        occurred_at=_NOW,
        trust_tier="attested_agent",
        external_id="issue-1",
        normalized_at=_NOW,
        ingress_metadata=metadata,
    )


@pytest.mark.parametrize("deduped", [False, True])
async def test_full_mode_records_after_normal_and_deduped_success(
    monkeypatch: pytest.MonkeyPatch,
    deduped: bool,
) -> None:
    calls: list[tuple[str, Any]] = []

    async def _ingest(**_kwargs: Any) -> Any:
        calls.append(("ingest", None))
        return SimpleNamespace(deduped=deduped)

    async def _record(_pool: Any, *, attribution: Any) -> None:
        calls.append(("record", attribution))

    monkeypatch.setattr(writer, "ingest_from_draft", _ingest)
    monkeypatch.setattr(writer, "_record_full_mode_attribution", _record)

    result = await writer._full_mode_write(
        _envelope(),
        pool=object(),  # type: ignore[arg-type]
        actor_repo=None,
        alias_repo=None,
        embedder=None,
        embedding_producer=None,
        summarization_producer=None,
        replica_id="writer-a",
    )

    assert result.deduped is deduped
    assert [name for name, _value in calls] == ["ingest", "record"]
    attribution = calls[1][1]
    assert attribution.tenant_id == _TENANT_ID
    assert attribution.source == "github"
    assert attribution.installation_id == _INSTALLATION_ID
    assert attribution.replica_id == "writer-a"


async def test_full_mode_is_noop_without_stamp_and_requires_replica_with_stamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingests = 0
    records = 0

    async def _ingest(**_kwargs: Any) -> Any:
        nonlocal ingests
        ingests += 1
        return SimpleNamespace(deduped=False)

    async def _record(_pool: Any, *, attribution: Any) -> None:
        nonlocal records
        records += 1

    monkeypatch.setattr(writer, "ingest_from_draft", _ingest)
    monkeypatch.setattr(writer, "_record_full_mode_attribution", _record)
    kwargs = {
        "pool": object(),
        "actor_repo": None,
        "alias_repo": None,
        "embedder": None,
        "embedding_producer": None,
        "summarization_producer": None,
    }

    await writer._full_mode_write(  # type: ignore[arg-type]
        _envelope(stamped=False),
        replica_id=None,
        **kwargs,
    )
    assert (ingests, records) == (1, 0)

    with pytest.raises(MissingWriterReplicaId):
        await writer._full_mode_write(  # type: ignore[arg-type]
            _envelope(),
            replica_id=None,
            **kwargs,
        )
    assert (ingests, records) == (1, 0)


async def test_self_heal_retry_keeps_explicit_replica_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def _ensure(*_args: Any, **_kwargs: Any) -> list[str]:
        return []

    async def _write(_env: Any, **kwargs: Any) -> Any:
        captured.update(kwargs)
        return SimpleNamespace(deduped=False)

    monkeypatch.setattr(writer, "ensure_partitions", _ensure)
    monkeypatch.setattr(writer, "_full_mode_write", _write)
    config = writer.WriterConfig(
        pool=object(),  # type: ignore[arg-type]
        replica_id="writer-self-heal",
    )
    env = _envelope().model_copy(
        update={"occurred_at": dt.datetime.now(tz=dt.timezone.utc)}
    )

    status = await writer._attempt_partition_self_heal(
        env,
        config=config,
        embedding_producer=None,
        summarization_producer=None,
    )

    assert status == writer._HEAL_INSERTED
    assert captured["replica_id"] == "writer-self-heal"


async def test_attribution_failure_escapes_after_observation_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _AttributionFailure(RuntimeError):
        pass

    async def _ingest(**_kwargs: Any) -> Any:
        return SimpleNamespace(deduped=False)

    async def _record(_pool: Any, *, attribution: Any) -> None:
        raise _AttributionFailure(attribution.event_id)

    monkeypatch.setattr(writer, "ingest_from_draft", _ingest)
    monkeypatch.setattr(writer, "_record_full_mode_attribution", _record)

    with pytest.raises(_AttributionFailure):
        await writer._full_mode_write(
            _envelope(),
            pool=object(),  # type: ignore[arg-type]
            actor_repo=None,
            alias_repo=None,
            embedder=None,
            embedding_producer=None,
            summarization_producer=None,
            replica_id="writer-a",
        )


async def test_writer_replica_id_comes_only_from_explicit_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WRITER_REPLICA_ID", raising=False)
    assert writer._writer_replica_id_from_env() is None
    monkeypatch.setenv("WRITER_REPLICA_ID", "writer-env-2")
    assert writer._writer_replica_id_from_env() == "writer-env-2"
