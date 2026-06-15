from __future__ import annotations

from services.platform.execution import inquiry, language_signals


def test_language_signal_helpers_keep_legacy_inquiry_identity() -> None:
    assert (
        inquiry._has_act_affecting_language
        is language_signals.has_act_affecting_language
    )
    assert (
        inquiry._has_broad_signal_language is language_signals.has_broad_signal_language
    )
    assert inquiry._has_commitment_language is language_signals.has_commitment_language
    assert inquiry._has_constraint_language is language_signals.has_constraint_language
    assert inquiry._has_dependency_language is language_signals.has_dependency_language
    assert (
        inquiry._has_revenue_impact_language
        is language_signals.has_revenue_impact_language
    )
    assert inquiry._has_risk_language is language_signals.has_risk_language
    assert inquiry._mentions_recurrence is language_signals.mentions_recurrence
    assert (
        inquiry._scrub_negated_signal_language
        is language_signals.scrub_negated_signal_language
    )
    assert (
        inquiry._signal_has_material_update_intent
        is language_signals.signal_has_material_update_intent
    )


def test_material_update_intent_ignores_explicit_no_update_chatter() -> None:
    lower = (
        "workspace chatter: lunch notes and travel plans. no blocker, no owner "
        "change, no decision, no customer risk, and no commitment update."
    )

    assert not language_signals.signal_has_material_update_intent(lower)
    assert not language_signals.has_broad_signal_language(lower)


def test_language_predicates_detect_positive_material_and_broad_signals() -> None:
    lower = (
        "renewal escalation is blocked by a policy exception across all accounts, "
        "and the launch deadline slipped again."
    )

    assert language_signals.has_broad_signal_language(lower)
    assert language_signals.has_risk_language(lower)
    assert language_signals.has_dependency_language(lower)
    assert language_signals.has_constraint_language(lower)
    assert language_signals.has_revenue_impact_language(lower)
    assert language_signals.has_commitment_language(lower)
    assert language_signals.has_act_affecting_language(lower)
    assert language_signals.mentions_recurrence(lower)
    assert language_signals.signal_has_material_update_intent(lower)
