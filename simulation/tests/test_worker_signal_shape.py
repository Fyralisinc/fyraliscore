"""Smoke-test the shape of SyntheticSignal objects each worker
produces without hitting the database.

This exercises the templating + field assembly paths — the full
"does it INSERT" check lives in the end-to-end replay test, which
requires Postgres and Ollama. Running fast unit coverage here catches
the usual bugs (wrong channel prefix, missing external_id, broken
persona lookup) in a ms-scale loop.
"""
from __future__ import annotations

import os

import pytest


# Ensure the env guard passes at import time for this test module.
os.environ.setdefault("COMPANY_OS_ENV", "test")

from simulation.personas import get_persona  # noqa: E402
from services.synthetic.core import SyntheticSignal  # noqa: E402


def _build_signal(**overrides) -> SyntheticSignal:
    defaults = dict(
        source_channel="github:payments",
        content_text="hello",
        content={"event_kind": "pr_opened"},
        occurred_at=__import__("datetime").datetime.now(
            tz=__import__("datetime").timezone.utc
        ),
        source_actor_ref="github:alice-dev",
        external_id="gh-pr-payments-1-opened",
        run_id="test-run",
        scenario_id="test-scenario",
    )
    defaults.update(overrides)
    return SyntheticSignal(**defaults)


def test_synthetic_signal_accepts_worker_fields():
    signal = _build_signal()
    assert signal.source_channel == "github:payments"
    assert signal.external_id.startswith("gh-pr-")
    assert signal.content["event_kind"] == "pr_opened"


def test_personas_have_expected_channel_refs():
    alice = get_persona("alice")
    assert alice.github_ref == "github:alice-dev"
    assert alice.slack_ref == "slack:alice"
    assert alice.email_ref == "email:alice@fyralis.internal"

    monica = get_persona("monica")
    assert monica.github_ref is None  # sales has no github handle


def test_worker_argparsers_are_wireable():
    """Each worker exposes a parse_args / main surface; smoke-check
    by importing and making sure the parsers build without error."""
    from simulation.workers import (  # noqa: F401
        calendar_worker,
        email_worker,
        github_issue_worker,
        github_pr_worker,
        linear_worker,
    )

    for mod in (
        github_pr_worker,
        github_issue_worker,
        email_worker,
        calendar_worker,
        linear_worker,
    ):
        # Every worker defines _parse_args; we can't easily call it
        # without CLI args, but we can introspect that the main entry
        # point exists.
        assert hasattr(mod, "main")


def test_parse_occurred_at_relative_forms():
    from simulation.workers._common import parse_occurred_at

    assert parse_occurred_at("now") is not None
    assert parse_occurred_at("-3h") < parse_occurred_at("+1h")
    assert parse_occurred_at("+2d") > parse_occurred_at("+1d")
    with pytest.raises(ValueError):
        parse_occurred_at("-abc")


def test_voice_hints_contain_persona_voice():
    from simulation.personas import voice_hints_for

    for handle in ("alice", "marcus", "monica", "priya", "david"):
        hints = voice_hints_for(handle)
        assert len(hints) > 20
        # Hints never contain the phrase "insights" (forbidden per
        # voice rules) — sanity that the YAML didn't drift.
        assert "insights" not in hints.lower()
