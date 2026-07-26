"""Source-owned fixture callables used by certification and Provider Lab."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from services.ingest.source_certification.runtime import certification_callable
from services.ingest.synthetic.fixtures.ashby_generator import make_ashby
from services.ingest.synthetic.fixtures.aws_generator import make_aws
from services.ingest.synthetic.fixtures.brex_generator import make_brex
from services.ingest.synthetic.fixtures.carta_generator import make_carta
from services.ingest.synthetic.fixtures.deel_generator import make_deel
from services.ingest.synthetic.fixtures.discord_generator import make_discord_guild
from services.ingest.synthetic.fixtures.facebook_pages_generator import (
    make_facebook_pages,
)
from services.ingest.synthetic.fixtures.figma_generator import make_figma
from services.ingest.synthetic.fixtures.fireflies_generator import make_fireflies
from services.ingest.synthetic.fixtures.github_generator import make_github_repos
from services.ingest.synthetic.fixtures.gmail_generator import make_gmail_mailbox
from services.ingest.synthetic.fixtures.google_calendar_generator import (
    make_google_calendar,
)
from services.ingest.synthetic.fixtures.google_drive_generator import (
    make_google_drive,
)
from services.ingest.synthetic.fixtures.grafana_generator import make_grafana
from services.ingest.synthetic.fixtures.gusto_generator import make_gusto
from services.ingest.synthetic.fixtures.hibob_generator import make_hibob
from services.ingest.synthetic.fixtures.jira_generator import make_jira
from services.ingest.synthetic.fixtures.linkedin_generator import make_linkedin
from services.ingest.synthetic.fixtures.mercury_generator import make_mercury
from services.ingest.synthetic.fixtures.miro_generator import make_miro
from services.ingest.synthetic.fixtures.notion_generator import make_notion
from services.ingest.synthetic.fixtures.quickbooks_generator import make_quickbooks
from services.ingest.synthetic.fixtures.ramp_generator import make_ramp
from services.ingest.synthetic.fixtures.signal_generator import make_signal
from services.ingest.synthetic.fixtures.slack_generator import make_slack_workspace
from services.ingest.synthetic.fixtures.telegram_generator import make_telegram


FixtureGenerator = Callable[..., dict[str, Any]]
FixtureFactory = Callable[..., dict[str, Any]]
FixtureCountOracle = Callable[[Mapping[str, Any]], int]


class CertificationFixtureCountError(ValueError):
    """A fixture cannot yield one exact, positive Observation count."""


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CertificationFixtureCountError(
            f"{path} must be a mapping, got {type(value).__name__}"
        )
    return value


def _list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise CertificationFixtureCountError(
            f"{path} must be a list, got {type(value).__name__}"
        )
    return value


def _field(container: Mapping[str, Any], key: str, path: str) -> Any:
    if key not in container:
        raise CertificationFixtureCountError(f"{path}.{key} is required")
    return container[key]


def _mapping_field(
    container: Mapping[str, Any],
    key: str,
    path: str = "fixture",
) -> Mapping[str, Any]:
    return _mapping(_field(container, key, path), f"{path}.{key}")


def _list_field(
    container: Mapping[str, Any],
    key: str,
    path: str = "fixture",
) -> list[Any]:
    return _list(_field(container, key, path), f"{path}.{key}")


def _count_mapping_lists(
    fixture: Mapping[str, Any],
    key: str,
) -> int:
    groups = _mapping_field(fixture, key)
    return sum(
        len(_list(rows, f"fixture.{key}[{group_id!r}]"))
        for group_id, rows in groups.items()
    )


def _count_entity_rows(fixture: Mapping[str, Any]) -> int:
    return _count_mapping_lists(fixture, "entities")


def _count_ordered_children(
    fixture: Mapping[str, Any],
    *,
    order_key: str,
    objects_key: str,
    children_key: str,
    snapshot_per_object: bool = False,
) -> int:
    order = _list_field(fixture, order_key)
    objects = _mapping_field(fixture, objects_key)
    count = 0
    for object_id in order:
        object_key = (
            object_id
            if object_id in objects
            else str(object_id)
            if str(object_id) in objects
            else None
        )
        if object_key is None:
            raise CertificationFixtureCountError(
                f"fixture.{objects_key}[{object_id!r}] is required by "
                f"fixture.{order_key}"
            )
        obj = _mapping(
            objects[object_key],
            f"fixture.{objects_key}[{object_id!r}]",
        )
        count += int(snapshot_per_object)
        count += len(
            _list_field(
                obj,
                children_key,
                f"fixture.{objects_key}[{object_id!r}]",
            )
        )
    return count


def _count_slack(fixture: Mapping[str, Any]) -> int:
    return sum(
        len(
            _list_field(
                _mapping(channel, f"fixture.channels[{index}]"),
                "messages",
                f"fixture.channels[{index}]",
            )
        )
        for index, channel in enumerate(_list_field(fixture, "channels"))
    )


def _count_github(fixture: Mapping[str, Any]) -> int:
    count = 0
    for repo_index, raw_repo in enumerate(_list_field(fixture, "repos")):
        path = f"fixture.repos[{repo_index}]"
        repo = _mapping(raw_repo, path)
        event_groups = _mapping_field(repo, "events_by_type", path)
        for event_type, raw_events in event_groups.items():
            events = _list(raw_events, f"{path}.events_by_type[{event_type!r}]")
            if event_type == "issues":
                count += sum(
                    1
                    for event_index, event in enumerate(events)
                    if "pull_request"
                    not in _mapping(
                        event,
                        f"{path}.events_by_type['issues'][{event_index}]",
                    )
                )
            else:
                count += len(events)
    return count


def _count_discord(fixture: Mapping[str, Any]) -> int:
    count = 0
    for channel_index, raw_channel in enumerate(_list_field(fixture, "channels")):
        path = f"fixture.channels[{channel_index}]"
        channel = _mapping(raw_channel, path)
        if channel.get("type", 0) != 0:
            continue
        count += len(_list_field(channel, "messages", path))
    return count


def _count_notion(fixture: Mapping[str, Any]) -> int:
    page_count = len(_list_field(fixture, "loose_pages"))
    for database_index, raw_database in enumerate(_list_field(fixture, "databases")):
        path = f"fixture.databases[{database_index}]"
        page_count += len(_list_field(_mapping(raw_database, path), "rows", path))
    return (
        page_count
        + _count_mapping_lists(fixture, "blocks_by_page")
        + _count_mapping_lists(fixture, "comments_by_page")
    )


def _count_google_drive(fixture: Mapping[str, Any]) -> int:
    count = 0
    for target_index, raw_target in enumerate(_list_field(fixture, "targets")):
        path = f"fixture.targets[{target_index}]"
        target = _mapping(raw_target, path)
        comments = _mapping_field(target, "comments", path)
        revisions = _mapping_field(target, "revisions", path)
        for file_index, raw_file in enumerate(_list_field(target, "files", path)):
            file_path = f"{path}.files[{file_index}]"
            file = _mapping(raw_file, file_path)
            file_id = _field(file, "id", file_path)
            if not isinstance(file_id, str) or not file_id:
                raise CertificationFixtureCountError(
                    f"{file_path}.id must be a non-empty string"
                )
            count += 1
            if file.get("trashed"):
                continue
            count += len(
                _list(
                    _field(comments, file_id, f"{path}.comments"),
                    f"{path}.comments[{file_id!r}]",
                )
            )
            count += len(
                _list(
                    _field(revisions, file_id, f"{path}.revisions"),
                    f"{path}.revisions[{file_id!r}]",
                )
            )
    return count


def _count_jira(fixture: Mapping[str, Any]) -> int:
    count = 0
    for project_index, raw_project in enumerate(_list_field(fixture, "projects")):
        project_path = f"fixture.projects[{project_index}]"
        project = _mapping(raw_project, project_path)
        for issue_index, raw_issue in enumerate(
            _list_field(project, "issues", project_path)
        ):
            issue_path = f"{project_path}.issues[{issue_index}]"
            issue = _mapping(raw_issue, issue_path)
            histories: list[Any] = []
            if "changelog" in issue:
                histories = _list_field(
                    _mapping_field(issue, "changelog", issue_path),
                    "histories",
                    f"{issue_path}.changelog",
                )
            fields = _mapping_field(issue, "fields", issue_path)
            comments: list[Any] = []
            if "comment" in fields:
                comments = _list_field(
                    _mapping_field(
                        fields,
                        "comment",
                        f"{issue_path}.fields",
                    ),
                    "comments",
                    f"{issue_path}.fields.comment",
                )
            count += 1 + len(histories) + len(comments)
    return count


def _count_figma(fixture: Mapping[str, Any]) -> int:
    # The planner emits two shards per file: its event stream and one durable
    # design snapshot. Both normalize to one Observation per fetched record.
    return _count_ordered_children(
        fixture,
        order_key="file_order",
        objects_key="files",
        children_key="events",
        snapshot_per_object=True,
    )


def _count_facebook_pages(fixture: Mapping[str, Any]) -> int:
    pages = _mapping_field(fixture, "pages")
    page = next(
        (
            _mapping(value, f"fixture.pages[{key!r}]")
            for key, value in pages.items()
            if isinstance(value, Mapping)
        ),
        None,
    )
    if page is None:
        raise CertificationFixtureCountError(
            "fixture.pages must contain the installation's page"
        )
    page_id = _field(page, "id", "fixture.pages")
    if not isinstance(page_id, str) or not page_id:
        raise CertificationFixtureCountError(
            "fixture.pages page id must be a non-empty string"
        )
    conversations_by_page = _mapping_field(fixture, "conversations")
    messages_by_conversation = _mapping_field(fixture, "messages")
    conversations = _list(
        _field(conversations_by_page, page_id, "fixture.conversations"),
        f"fixture.conversations[{page_id!r}]",
    )
    count = 0
    for index, raw_conversation in enumerate(conversations):
        path = f"fixture.conversations[{page_id!r}][{index}]"
        conversation = _mapping(raw_conversation, path)
        conversation_id = _field(conversation, "id", path)
        if not isinstance(conversation_id, str) or not conversation_id:
            raise CertificationFixtureCountError(
                f"{path}.id must be a non-empty string"
            )
        count += len(
            _list(
                _field(
                    messages_by_conversation,
                    conversation_id,
                    "fixture.messages",
                ),
                f"fixture.messages[{conversation_id!r}]",
            )
        )
    return count


def _bind_fixture(
    source_id: str,
    generator: FixtureGenerator,
    *,
    count_observations: FixtureCountOracle,
    identity_parameter: str | None = None,
    required_defaults: Mapping[str, str] | None = None,
) -> tuple[FixtureFactory, FixtureCountOracle]:
    @certification_callable(source_id=source_id, role="fixture_factory")
    def _factory(
        *,
        fixture_params: Mapping[str, Any],
        installation_id: str,
    ) -> dict[str, Any]:
        params = dict(fixture_params)
        if identity_parameter is not None:
            params[identity_parameter] = installation_id
        for key, value in (required_defaults or {}).items():
            params.setdefault(key, value.format(installation_id=installation_id))
        fixture = generator(**params)
        if not isinstance(fixture, dict):
            raise TypeError(
                f"{source_id} certification fixture must be a dict, "
                f"got {type(fixture).__name__}"
            )
        return fixture

    _factory.__name__ = f"build_{source_id}_fixture"
    _factory.__qualname__ = _factory.__name__

    @certification_callable(
        source_id=source_id,
        role="fixture_count_oracle",
    )
    def _oracle(fixture: Mapping[str, Any]) -> int:
        try:
            count = count_observations(_mapping(fixture, "fixture"))
        except CertificationFixtureCountError as exc:
            raise CertificationFixtureCountError(
                f"{source_id} fixture has no exact Observation count: {exc}"
            ) from exc
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise CertificationFixtureCountError(
                f"{source_id} fixture exact Observation count must be a "
                f"positive integer, got {count!r}"
            )
        return count

    _oracle.__name__ = f"count_{source_id}_fixture_observations"
    _oracle.__qualname__ = _oracle.__name__
    return _factory, _oracle


build_slack_fixture, count_slack_fixture_observations = _bind_fixture(
    "slack",
    make_slack_workspace,
    count_observations=_count_slack,
    identity_parameter="team_id",
)
build_github_fixture, count_github_fixture_observations = _bind_fixture(
    "github",
    make_github_repos,
    count_observations=_count_github,
    identity_parameter="installation_id",
    required_defaults={"org_or_user": "{installation_id}"},
)
build_discord_fixture, count_discord_fixture_observations = _bind_fixture(
    "discord",
    make_discord_guild,
    count_observations=_count_discord,
    identity_parameter="guild_id",
)
build_gmail_fixture, count_gmail_fixture_observations = _bind_fixture(
    "gmail",
    make_gmail_mailbox,
    count_observations=lambda fixture: len(_list_field(fixture, "messages")),
    required_defaults={"email": "{installation_id}@provider-lab.test"},
)
build_notion_fixture, count_notion_fixture_observations = _bind_fixture(
    "notion",
    make_notion,
    count_observations=_count_notion,
    identity_parameter="workspace_id",
)
(
    build_google_calendar_fixture,
    count_google_calendar_fixture_observations,
) = _bind_fixture(
    "google_calendar",
    make_google_calendar,
    count_observations=lambda fixture: _count_mapping_lists(fixture, "events"),
)
(
    build_google_drive_fixture,
    count_google_drive_fixture_observations,
) = _bind_fixture(
    "google_drive",
    make_google_drive,
    count_observations=_count_google_drive,
)
build_jira_fixture, count_jira_fixture_observations = _bind_fixture(
    "jira",
    make_jira,
    count_observations=_count_jira,
)
build_mercury_fixture, count_mercury_fixture_observations = _bind_fixture(
    "mercury",
    make_mercury,
    count_observations=lambda fixture: _count_ordered_children(
        fixture,
        order_key="account_order",
        objects_key="accounts",
        children_key="transactions",
        snapshot_per_object=True,
    ),
)
(
    build_quickbooks_fixture,
    count_quickbooks_fixture_observations,
) = _bind_fixture(
    "quickbooks",
    make_quickbooks,
    count_observations=_count_entity_rows,
)
build_grafana_fixture, count_grafana_fixture_observations = _bind_fixture(
    "grafana",
    make_grafana,
    count_observations=lambda fixture: len(_list_field(fixture, "annotations")),
)
build_telegram_fixture, count_telegram_fixture_observations = _bind_fixture(
    "telegram",
    make_telegram,
    count_observations=lambda fixture: _count_ordered_children(
        fixture,
        order_key="dialog_order",
        objects_key="dialogs",
        children_key="messages",
    ),
)
build_brex_fixture, count_brex_fixture_observations = _bind_fixture(
    "brex",
    make_brex,
    count_observations=lambda fixture: _count_ordered_children(
        fixture,
        order_key="account_order",
        objects_key="accounts",
        children_key="transactions",
        snapshot_per_object=True,
    ),
)
build_ramp_fixture, count_ramp_fixture_observations = _bind_fixture(
    "ramp",
    make_ramp,
    count_observations=_count_entity_rows,
)
build_gusto_fixture, count_gusto_fixture_observations = _bind_fixture(
    "gusto",
    make_gusto,
    count_observations=_count_entity_rows,
)
build_deel_fixture, count_deel_fixture_observations = _bind_fixture(
    "deel",
    make_deel,
    count_observations=lambda fixture: _count_ordered_children(
        fixture,
        order_key="contract_order",
        objects_key="contracts",
        children_key="payments",
        snapshot_per_object=True,
    ),
)
(
    build_fireflies_fixture,
    count_fireflies_fixture_observations,
) = _bind_fixture(
    "fireflies",
    make_fireflies,
    count_observations=lambda fixture: len(_list_field(fixture, "transcripts")),
    required_defaults={"workspace_id": "{installation_id}"},
)
build_signal_fixture, count_signal_fixture_observations = _bind_fixture(
    "signal",
    make_signal,
    count_observations=lambda fixture: _count_ordered_children(
        fixture,
        order_key="thread_order",
        objects_key="threads",
        children_key="messages",
    ),
)
build_aws_fixture, count_aws_fixture_observations = _bind_fixture(
    "aws",
    make_aws,
    count_observations=lambda fixture: len(_list_field(fixture, "events")),
    required_defaults={"account_id": "000000000000"},
)
build_miro_fixture, count_miro_fixture_observations = _bind_fixture(
    "miro",
    make_miro,
    count_observations=lambda fixture: _count_ordered_children(
        fixture,
        order_key="board_order",
        objects_key="boards",
        children_key="items",
    ),
    required_defaults={"org_id": "{installation_id}"},
)
build_figma_fixture, count_figma_fixture_observations = _bind_fixture(
    "figma",
    make_figma,
    count_observations=_count_figma,
    required_defaults={"team_id": "{installation_id}"},
)
build_carta_fixture, count_carta_fixture_observations = _bind_fixture(
    "carta",
    make_carta,
    count_observations=_count_entity_rows,
)
build_hibob_fixture, count_hibob_fixture_observations = _bind_fixture(
    "hibob",
    make_hibob,
    count_observations=_count_entity_rows,
)
build_ashby_fixture, count_ashby_fixture_observations = _bind_fixture(
    "ashby",
    make_ashby,
    count_observations=_count_entity_rows,
)
(
    build_linkedin_fixture,
    count_linkedin_fixture_observations,
) = _bind_fixture(
    "linkedin",
    make_linkedin,
    count_observations=_count_entity_rows,
)
(
    build_facebook_pages_fixture,
    count_facebook_pages_fixture_observations,
) = _bind_fixture(
    "facebook_pages",
    make_facebook_pages,
    count_observations=_count_facebook_pages,
    identity_parameter="page_id",
)


__all__ = [
    "CertificationFixtureCountError",
    "build_ashby_fixture",
    "build_aws_fixture",
    "build_brex_fixture",
    "build_carta_fixture",
    "build_deel_fixture",
    "build_discord_fixture",
    "build_facebook_pages_fixture",
    "build_figma_fixture",
    "build_fireflies_fixture",
    "build_github_fixture",
    "build_gmail_fixture",
    "build_google_calendar_fixture",
    "build_google_drive_fixture",
    "build_grafana_fixture",
    "build_gusto_fixture",
    "build_hibob_fixture",
    "build_jira_fixture",
    "build_linkedin_fixture",
    "build_mercury_fixture",
    "build_miro_fixture",
    "build_notion_fixture",
    "build_quickbooks_fixture",
    "build_ramp_fixture",
    "build_signal_fixture",
    "build_slack_fixture",
    "build_telegram_fixture",
    "count_ashby_fixture_observations",
    "count_aws_fixture_observations",
    "count_brex_fixture_observations",
    "count_carta_fixture_observations",
    "count_deel_fixture_observations",
    "count_discord_fixture_observations",
    "count_facebook_pages_fixture_observations",
    "count_figma_fixture_observations",
    "count_fireflies_fixture_observations",
    "count_github_fixture_observations",
    "count_gmail_fixture_observations",
    "count_google_calendar_fixture_observations",
    "count_google_drive_fixture_observations",
    "count_grafana_fixture_observations",
    "count_gusto_fixture_observations",
    "count_hibob_fixture_observations",
    "count_jira_fixture_observations",
    "count_linkedin_fixture_observations",
    "count_mercury_fixture_observations",
    "count_miro_fixture_observations",
    "count_notion_fixture_observations",
    "count_quickbooks_fixture_observations",
    "count_ramp_fixture_observations",
    "count_signal_fixture_observations",
    "count_slack_fixture_observations",
    "count_telegram_fixture_observations",
]
