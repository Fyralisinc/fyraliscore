"""Shared fixtures for baseline smoke tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

import pytest

from lsob_contracts import (
    AtRiskReport,
    Belief,
    BeliefQuery,
    DiffOp,
    EntityRef,
    Signal,
    SUTConfig,
    SystemUnderTest,
    Trigger,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES_DIR = REPO_ROOT / "fixtures"


@pytest.fixture(scope="session")
def mini_corpus_a() -> dict:
    with (FIXTURES_DIR / "mini_corpus_a.json").open("r", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture()
def ten_signals(mini_corpus_a: dict) -> list[Signal]:
    raw_signals = mini_corpus_a["signals"][:10]
    return [Signal.model_validate(s) for s in raw_signals]


@pytest.fixture()
def commitment_query() -> BeliefQuery:
    return BeliefQuery(
        query_id="q-commitment-C-ingest",
        entity_ref=EntityRef(kind="commitment", id="C-ingest"),
        timestamp=datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
        proposition_kind="status",
        k=3,
    )


@pytest.fixture()
def sut_config() -> SUTConfig:
    return SUTConfig(sut_name="test", tenant_id="t-test", params={})


@pytest.fixture()
def handcrafted_trigger() -> Trigger:
    return Trigger(
        trigger_id="trg-1",
        kind="commitment_at_risk",
        payload={
            "entity_ref": "C-ingest",
            "note": "Alice reports slipping to end of next week",
        },
        timestamp=datetime(2026, 1, 17, 0, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture()
def smoke_runner(
    ten_signals: list[Signal],
    commitment_query: BeliefQuery,
    handcrafted_trigger: Trigger,
    sut_config: SUTConfig,
) -> Callable[[Callable[[], SystemUnderTest]], Awaitable[None]]:
    """Return an async callable that exercises a SUT end-to-end.

    Runs: startup -> ingest 10 signals -> query_beliefs_at ->
    query_at_risk_at -> produce_diff_for_trigger -> shutdown. Asserts
    returned types are valid Pydantic.
    """

    async def _run(factory: Callable[[], SystemUnderTest]) -> None:
        sut = factory()
        await sut.startup(sut_config)
        try:
            for sig in ten_signals:
                await sut.ingest_signal(sig)

            beliefs = await sut.query_beliefs_at(commitment_query)
            assert isinstance(beliefs, list)
            for b in beliefs:
                Belief.model_validate(b.model_dump(mode="json"))

            at_risk = await sut.query_at_risk_at(commitment_query.timestamp)
            assert isinstance(at_risk, AtRiskReport)
            AtRiskReport.model_validate(at_risk.model_dump(mode="json"))

            diff = await sut.produce_diff_for_trigger(handcrafted_trigger)
            assert isinstance(diff, DiffOp)
            DiffOp.model_validate(diff.model_dump(mode="json"))
            assert diff.trigger_id == handcrafted_trigger.trigger_id
        finally:
            await sut.shutdown()

    return _run
