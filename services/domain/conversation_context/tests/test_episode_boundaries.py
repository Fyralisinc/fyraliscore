from datetime import datetime, timezone

from services.domain.conversation_context.episode_boundaries import (
    ConversationBoundaryObservation,
    project_conversation_episode_boundaries,
)


NOW = datetime(2026, 7, 18, tzinfo=timezone.utc)


def _row(identifier: str, text: str, container: str):
    return ConversationBoundaryObservation(identifier, NOW, text, container)


def test_explicit_topic_drift_splits_one_thread_and_joins_cross_source_evidence() -> None:
    groups = project_conversation_episode_boundaries((
        _row("slack-old", "Harbor episode 17 is blocked.", "slack-thread:A"),
        _row("slack-drift", "Harbor episode 18 is now the priority.", "slack-thread:A"),
        _row("jira-new", "Harbor episode 18 has a Jira update.", "jira:ISSUE-9"),
    ))
    assert {frozenset(group) for group in groups} == {
        frozenset({"slack-old"}),
        frozenset({"slack-drift", "jira-new"}),
    }


def test_deictic_message_does_not_create_an_unsupported_cross_source_merge() -> None:
    groups = project_conversation_episode_boundaries((
        _row("root", "The renewal is blocked.", "slack-thread:A"),
        _row("reply", "It still is.", "slack-thread:A"),
        _row("noise", "It shipped.", "slack-thread:B"),
    ))
    assert {frozenset(group) for group in groups} == {
        frozenset({"root", "reply"}), frozenset({"noise"}),
    }


def test_persisted_entity_ref_joins_cross_source_episode() -> None:
    groups = project_conversation_episode_boundaries((
        ConversationBoundaryObservation(
            "slack", NOW, "The owner is missing.", "slack:thread-a",
            ("workstream:delta-handoff",),
        ),
        ConversationBoundaryObservation(
            "email", NOW, "The incident rate moved.", "email:thread-b",
            ("workstream:delta-handoff",),
        ),
        ConversationBoundaryObservation(
            "jira", NOW, "The checklist is incomplete.", "jira:issue-c",
            ("workstream:delta-handoff",),
        ),
    ))
    assert groups == (("email", "jira", "slack"),)


def test_same_surface_without_same_persisted_identity_does_not_merge() -> None:
    groups = project_conversation_episode_boundaries((
        ConversationBoundaryObservation(
            "business", NOW, "Beacon is delayed.", "slack:business",
            ("workstream:beacon-migration",),
        ),
        ConversationBoundaryObservation(
            "office", NOW, "Beacon ticket moved.", "jira:workplace",
            ("work_item:beacon-office-ticket",),
        ),
    ))
    assert {frozenset(group) for group in groups} == {
        frozenset({"business"}), frozenset({"office"}),
    }
