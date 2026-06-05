"""Unit tests for services.reasoning.sage.cue_extractor.

These tests exercise the deterministic v1 of the Structured Cue
Extractor (SAGE Phase 2, doc §7.2). No DB and no LLM — the
`alias_loader` constructor argument lets tests inject a static alias
mapping.
"""
from __future__ import annotations

import asyncio


from services.reasoning.sage.cue_extractor import CueExtractor, StructuredCues


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _extractor(aliases: dict[str, str] | None = None) -> CueExtractor:
    """Build an extractor with an injected alias mapping (no DB)."""
    return CueExtractor(
        pool=None,
        tenant_id="tenant_test",
        alias_loader=(lambda: dict(aliases or {})),
    )


# ---------------------------------------------------------------------
# 1. Doc §7.2 example: extract "Acme" and "SSO"
# ---------------------------------------------------------------------


def test_extracts_acme_and_sso_from_doc_example():
    """Doc §7.2 example: signal mentions Acme + SSO via aliases."""
    extractor = _extractor(
        {
            "single sign-on": "SSO",
            "enterprise login": "SSO",
            "acme corp": "Acme",
        }
    )
    cues = _run(
        extractor.extract(
            signal={"summary": "Acme Corp launch depends on single sign-on rollout."},
            question="Is SSO on Acme's critical path?",
            hypotheses=[],
        )
    )
    assert isinstance(cues, StructuredCues)
    assert "Acme" in cues.explicit_entities
    assert "SSO" in cues.explicit_entities
    # Alias matches surface both the configured aliases.
    assert "single sign-on" in cues.aliases
    assert "acme corp" in cues.aliases


# ---------------------------------------------------------------------
# 2. Relation clues: depends_on + critical_path
# ---------------------------------------------------------------------


def test_detects_depends_on_and_critical_path_clues():
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={"summary": "Launch depends on SSO; SSO is on the critical path."},
            question=None,
            hypotheses=[],
        )
    )
    assert "depends_on" in cues.relationship_clues
    assert "critical_path" in cues.relationship_clues


# ---------------------------------------------------------------------
# 3. Time constraint: "in the last 30 days" -> recent_window_days=30
# ---------------------------------------------------------------------


def test_time_constraint_last_30_days():
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={"summary": "Three deals slipped in the last 30 days."},
            question=None,
            hypotheses=[],
        )
    )
    assert cues.time_constraints.get("recent_window_days") == 30
    assert "phrase" in cues.time_constraints


# ---------------------------------------------------------------------
# 4. Expected synthesis decision type for blocked-launch signal
# ---------------------------------------------------------------------


def test_blocked_launch_yields_update_commitment_risk():
    extractor = _extractor({"acme": "Acme"})
    cues = _run(
        extractor.extract(
            signal={"summary": "Acme launch is blocked by SSO."},
            question=None,
            hypotheses=[],
        )
    )
    assert "update_commitment_risk" in cues.expected_synthesis_decision_type
    # The relation clue for "blocks" should also fire.
    assert "blocks" in cues.relationship_clues


# ---------------------------------------------------------------------
# 5. Non-actionable signal yields (mostly) empty cues
# ---------------------------------------------------------------------


def test_team_retro_went_well_is_empty_of_actionable_cues():
    """A non-actionable, positive signal should produce no relation
    clues, no decision triggers, and no time/source/access constraints.
    Capitalized-noun fallback may still surface "Team" — but the
    actionable buckets must be empty.
    """
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={"summary": "Team retro went well."},
            question=None,
            hypotheses=[],
        )
    )
    assert cues.relationship_clues == ()
    assert cues.expected_synthesis_decision_type == ()
    assert cues.time_constraints == {}
    assert cues.source_constraints == {}
    assert cues.access_constraints == {}


# ---------------------------------------------------------------------
# 6. Source + access constraints
# ---------------------------------------------------------------------


def test_source_and_access_constraints():
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={
                "summary": (
                    "Confidential note shared in Slack and Linear about HR rollout."
                )
            },
            question=None,
            hypotheses=[],
        )
    )
    channels = cues.source_constraints.get("channels", ())
    assert "slack" in channels
    assert "linear" in channels
    markers = cues.access_constraints.get("sensitivity_markers", ())
    assert "confidential" in markers
    assert "hr" in markers


# ---------------------------------------------------------------------
# 7. Pattern / recurring question -> create_emerging_pattern_model
# ---------------------------------------------------------------------


def test_recurring_pattern_decision():
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={"summary": "Recurring deal slippage across the Sales team."},
            question="Is this a pattern across customers?",
            hypotheses=[],
        )
    )
    assert "create_emerging_pattern_model" in cues.expected_synthesis_decision_type
    # The relation clue for "recurring" should also fire.
    assert "recurring" in cues.relationship_clues


# ---------------------------------------------------------------------
# 8. Ownership question -> create_ownership_relation
# ---------------------------------------------------------------------


def test_ownership_decision():
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={"summary": "Who owns the onboarding flow?"},
            question="Who is the owner of onboarding?",
            hypotheses=[],
        )
    )
    assert "create_ownership_relation" in cues.expected_synthesis_decision_type
    assert "owns" in cues.relationship_clues


# ---------------------------------------------------------------------
# 9. Dependency / critical-path question -> create_dependency_relation
# ---------------------------------------------------------------------


def test_dependency_decision():
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={"summary": "SSO is on the critical path of Acme onboarding."},
            question="What does Acme onboarding depend on?",
            hypotheses=[],
        )
    )
    assert "create_dependency_relation" in cues.expected_synthesis_decision_type
    assert "depends_on" in cues.relationship_clues
    assert "critical_path" in cues.relationship_clues


# ---------------------------------------------------------------------
# 10. "Since YYYY-MM-DD" produces ISO since constraint
# ---------------------------------------------------------------------


def test_since_date_time_constraint():
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={"summary": "Customer engagement has dropped since 2026-01-15."},
            question=None,
            hypotheses=[],
        )
    )
    assert cues.time_constraints.get("since") == "2026-01-15"


# ---------------------------------------------------------------------
# 11. Alias cache: loader called once across multiple extract() calls
# ---------------------------------------------------------------------


def test_alias_loader_cached_across_calls():
    calls: list[int] = []

    def loader():
        calls.append(1)
        return {"acme": "Acme"}

    extractor = CueExtractor(pool=None, tenant_id="t", alias_loader=loader)
    _run(extractor.extract(signal={"summary": "Acme launch."}, question=None, hypotheses=[]))
    _run(extractor.extract(signal={"summary": "Acme retro."}, question=None, hypotheses=[]))
    assert len(calls) == 1


# ---------------------------------------------------------------------
# 12. Builtin system dictionary catches K8s / OAuth without alias
# ---------------------------------------------------------------------


def test_builtin_system_dictionary_fallback():
    extractor = _extractor()
    cues = _run(
        extractor.extract(
            signal={"summary": "K8s upgrade unblocked the OAuth migration."},
            question=None,
            hypotheses=[],
        )
    )
    assert "K8S" in cues.system_mentions or "k8s" in (s.lower() for s in cues.system_mentions)
    assert any(s.upper() == "OAUTH" for s in cues.system_mentions)
    # "enables/unblocks" relation should fire.
    assert "enables" in cues.relationship_clues


# ---------------------------------------------------------------------
# 13. Async alias loader is awaited correctly
# ---------------------------------------------------------------------


def test_async_alias_loader_supported():
    async def loader():
        return {"acme": "Acme"}

    extractor = CueExtractor(pool=None, tenant_id="t", alias_loader=loader)
    cues = _run(
        extractor.extract(
            signal={"summary": "Acme is blocked."}, question=None, hypotheses=[]
        )
    )
    assert "Acme" in cues.explicit_entities
    assert "update_commitment_risk" in cues.expected_synthesis_decision_type
