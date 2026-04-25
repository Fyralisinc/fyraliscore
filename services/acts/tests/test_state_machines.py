"""Pure unit tests for state_machines.py — no DB, no pool, no fixtures."""
from __future__ import annotations

import pytest

from services.acts.state_machines import (
    COMMITMENT_TERMINAL,
    DECISION_TERMINAL,
    GOAL_TERMINAL,
    can_transition,
    is_terminal,
    legal_targets,
)


# ---- Goal ----------------------------------------------------------

GOAL_LEGAL = [
    ("active", "paused"),
    ("active", "achieved"),
    ("active", "abandoned"),
    ("paused", "active"),
    ("paused", "achieved"),
    ("paused", "abandoned"),
]

GOAL_ILLEGAL = [
    ("active", "drafted"),
    ("achieved", "active"),
    ("achieved", "paused"),
    ("abandoned", "active"),
    ("paused", "paused"),   # no-op
    ("active", "active"),   # no-op
]


@pytest.mark.parametrize("cur,nxt", GOAL_LEGAL)
def test_goal_legal_transitions(cur: str, nxt: str) -> None:
    ok, reason = can_transition(cur, nxt, "goal")
    assert ok, f"expected {cur}->{nxt} legal, got {reason}"


@pytest.mark.parametrize("cur,nxt", GOAL_ILLEGAL)
def test_goal_illegal_transitions(cur: str, nxt: str) -> None:
    ok, reason = can_transition(cur, nxt, "goal")
    assert not ok
    assert reason


# ---- Commitment ----------------------------------------------------

COMMITMENT_LEGAL = [
    ("proposed", "active"),
    ("proposed", "closed"),
    ("active", "blocked"),
    ("active", "paused"),
    ("active", "doneunverified"),
    ("active", "closed"),
    ("blocked", "active"),
    ("blocked", "paused"),
    ("blocked", "closed"),
    ("paused", "active"),
    ("paused", "closed"),
    ("doneunverified", "doneverified"),
    ("doneunverified", "active"),
    ("doneunverified", "closed"),
]

COMMITMENT_ILLEGAL = [
    ("proposed", "blocked"),         # must go through active first
    ("proposed", "paused"),
    ("proposed", "doneunverified"),
    ("proposed", "doneverified"),
    ("active", "proposed"),          # no backflow to proposed
    ("blocked", "doneunverified"),   # only via active
    ("blocked", "doneverified"),
    ("paused", "blocked"),
    ("doneverified", "active"),      # terminal
    ("doneverified", "closed"),      # terminal
    ("closed", "active"),            # terminal
    ("closed", "doneverified"),      # terminal
    ("proposed", "proposed"),        # no-op
]


@pytest.mark.parametrize("cur,nxt", COMMITMENT_LEGAL)
def test_commitment_legal_transitions(cur: str, nxt: str) -> None:
    ok, reason = can_transition(cur, nxt, "commitment")
    assert ok, f"expected {cur}->{nxt} legal, got {reason}"


@pytest.mark.parametrize("cur,nxt", COMMITMENT_ILLEGAL)
def test_commitment_illegal_transitions(cur: str, nxt: str) -> None:
    ok, reason = can_transition(cur, nxt, "commitment")
    assert not ok
    assert reason


# ---- Decision ------------------------------------------------------

DECISION_LEGAL = [
    ("drafted", "active"),
    ("active", "revisited"),
    ("active", "archived"),
    ("revisited", "active"),
    ("revisited", "archived"),
]

DECISION_ILLEGAL = [
    ("drafted", "archived"),
    ("drafted", "revisited"),
    ("archived", "active"),   # terminal
    ("archived", "revisited"),
    ("active", "drafted"),
    ("revisited", "drafted"),
    ("drafted", "drafted"),
]


@pytest.mark.parametrize("cur,nxt", DECISION_LEGAL)
def test_decision_legal_transitions(cur: str, nxt: str) -> None:
    ok, reason = can_transition(cur, nxt, "decision")
    assert ok, f"expected {cur}->{nxt} legal, got {reason}"


@pytest.mark.parametrize("cur,nxt", DECISION_ILLEGAL)
def test_decision_illegal_transitions(cur: str, nxt: str) -> None:
    ok, reason = can_transition(cur, nxt, "decision")
    assert not ok
    assert reason


def test_terminal_sets_match_spec() -> None:
    assert GOAL_TERMINAL == {"achieved", "abandoned"}
    assert COMMITMENT_TERMINAL == {"doneverified", "closed"}
    assert DECISION_TERMINAL == {"archived"}


def test_legal_targets_returns_empty_for_terminals() -> None:
    assert legal_targets("doneverified", "commitment") == set()
    assert legal_targets("closed", "commitment") == set()
    assert legal_targets("achieved", "goal") == set()
    assert legal_targets("archived", "decision") == set()


def test_is_terminal_lookup() -> None:
    assert is_terminal("doneverified", "commitment")
    assert not is_terminal("active", "commitment")
    assert is_terminal("achieved", "goal")


def test_can_transition_unknown_kind() -> None:
    ok, reason = can_transition("active", "paused", "not_a_kind")  # type: ignore[arg-type]
    assert not ok
    assert "unknown act kind" in reason


def test_can_transition_unknown_state() -> None:
    ok, reason = can_transition("flibber", "paused", "goal")
    assert not ok
    assert "unknown goal state" in reason
