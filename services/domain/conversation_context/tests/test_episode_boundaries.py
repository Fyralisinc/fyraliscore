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
