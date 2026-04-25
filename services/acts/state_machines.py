"""
services/acts/state_machines.py — pure, declarative state-machine
definitions for Goals, Commitments, and Decisions.

See ARCHITECTURE-FINAL.md §3.1 / §3.2 / §3.3 for the authoritative
transition diagrams. These tables are the *only* source of truth for
what transitions are legal — callers in goals.py / commitments.py /
decisions.py import `can_transition` and delegate.

No DB access. No side effects. No dependency on lib.shared. Pure sets.
"""
from __future__ import annotations

from typing import Literal

from lib.shared.types import CommitmentState, DecisionState, GoalState


ActKind = Literal["goal", "commitment", "decision"]


# ---------------------------------------------------------------------
# Goal state machine (ARCHITECTURE-FINAL.md §3.1)
# ---------------------------------------------------------------------
# active ↔ paused
# active → achieved     (terminal)
# active → abandoned    (terminal)
# paused → achieved / abandoned
# ---------------------------------------------------------------------

GOAL_TRANSITIONS: dict[GoalState, set[GoalState]] = {
    "active": {"paused", "achieved", "abandoned"},
    "paused": {"active", "achieved", "abandoned"},
    "achieved": set(),    # terminal
    "abandoned": set(),   # terminal
}

GOAL_TERMINAL: set[GoalState] = {"achieved", "abandoned"}


# ---------------------------------------------------------------------
# Commitment state machine (ARCHITECTURE-FINAL.md §3.2)
# ---------------------------------------------------------------------
# proposed      → active / closed
# active        → blocked / paused / doneunverified / closed
# blocked       → active / paused / closed
# paused        → active / closed
# doneunverified → doneverified / active / closed
# Terminal: doneverified, closed
# ---------------------------------------------------------------------

COMMITMENT_TRANSITIONS: dict[CommitmentState, set[CommitmentState]] = {
    "proposed": {"active", "closed"},
    "active": {"blocked", "paused", "doneunverified", "closed"},
    "blocked": {"active", "paused", "closed"},
    "paused": {"active", "closed"},
    "doneunverified": {"doneverified", "active", "closed"},
    "doneverified": set(),   # terminal
    "closed": set(),         # terminal
}

COMMITMENT_TERMINAL: set[CommitmentState] = {"doneverified", "closed"}


# ---------------------------------------------------------------------
# Decision state machine (ARCHITECTURE-FINAL.md §3.3)
# ---------------------------------------------------------------------
# drafted   → active
# active    → revisited / archived
# revisited → active / archived
# archived  (terminal)
# ---------------------------------------------------------------------

DECISION_TRANSITIONS: dict[DecisionState, set[DecisionState]] = {
    "drafted": {"active"},
    "active": {"revisited", "archived"},
    "revisited": {"active", "archived"},
    "archived": set(),     # terminal
}

DECISION_TERMINAL: set[DecisionState] = {"archived"}


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

_TRANSITIONS: dict[ActKind, dict[str, set[str]]] = {
    "goal": GOAL_TRANSITIONS,       # type: ignore[dict-item]
    "commitment": COMMITMENT_TRANSITIONS,  # type: ignore[dict-item]
    "decision": DECISION_TRANSITIONS,  # type: ignore[dict-item]
}

_TERMINAL: dict[ActKind, set[str]] = {
    "goal": GOAL_TERMINAL,           # type: ignore[dict-item]
    "commitment": COMMITMENT_TERMINAL,  # type: ignore[dict-item]
    "decision": DECISION_TERMINAL,   # type: ignore[dict-item]
}


def can_transition(
    current_state: str,
    new_state: str,
    kind: ActKind,
) -> tuple[bool, str]:
    """
    Pure check: is (current_state -> new_state) a legal move for `kind`?

    Returns (True, "") when legal, (False, reason) otherwise.
    """
    table = _TRANSITIONS.get(kind)
    if table is None:
        return False, f"unknown act kind: {kind!r}"

    if current_state not in table:
        return False, f"unknown {kind} state: {current_state!r}"

    # C8 / terminal: no outgoing transitions from terminal states
    if current_state in _TERMINAL[kind]:
        return (
            False,
            f"cannot transition out of terminal {kind} state {current_state!r}",
        )

    if new_state == current_state:
        # idempotent self-transition is not a transition — reject so
        # callers can't accidentally update last_state_change_at.
        return False, f"no-op transition {current_state!r}→{new_state!r}"

    allowed = table[current_state]
    if new_state in allowed:
        return True, ""

    return (
        False,
        f"illegal {kind} transition: {current_state!r}→{new_state!r} "
        f"(allowed: {sorted(allowed)})",
    )


def is_terminal(state: str, kind: ActKind) -> bool:
    """Return True if `state` is a terminal state for `kind`."""
    return state in _TERMINAL.get(kind, set())


def legal_targets(current_state: str, kind: ActKind) -> set[str]:
    """Return the set of legal target states from `current_state`."""
    return set(_TRANSITIONS.get(kind, {}).get(current_state, set()))


__all__ = [
    "ActKind",
    "GOAL_TRANSITIONS",
    "GOAL_TERMINAL",
    "COMMITMENT_TRANSITIONS",
    "COMMITMENT_TERMINAL",
    "DECISION_TRANSITIONS",
    "DECISION_TERMINAL",
    "can_transition",
    "is_terminal",
    "legal_targets",
]
