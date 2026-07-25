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


def _bind_fixture(
    source_id: str,
    generator: FixtureGenerator,
    *,
    identity_parameter: str | None = None,
    required_defaults: Mapping[str, str] | None = None,
) -> FixtureFactory:
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
    return _factory


build_slack_fixture = _bind_fixture(
    "slack",
    make_slack_workspace,
    identity_parameter="team_id",
)
build_github_fixture = _bind_fixture(
    "github",
    make_github_repos,
    identity_parameter="installation_id",
    required_defaults={"org_or_user": "{installation_id}"},
)
build_discord_fixture = _bind_fixture(
    "discord",
    make_discord_guild,
    identity_parameter="guild_id",
)
build_gmail_fixture = _bind_fixture(
    "gmail",
    make_gmail_mailbox,
    required_defaults={"email": "{installation_id}@provider-lab.test"},
)
build_notion_fixture = _bind_fixture(
    "notion",
    make_notion,
    identity_parameter="workspace_id",
)
build_google_calendar_fixture = _bind_fixture(
    "google_calendar",
    make_google_calendar,
)
build_google_drive_fixture = _bind_fixture("google_drive", make_google_drive)
build_jira_fixture = _bind_fixture("jira", make_jira)
build_mercury_fixture = _bind_fixture("mercury", make_mercury)
build_quickbooks_fixture = _bind_fixture("quickbooks", make_quickbooks)
build_grafana_fixture = _bind_fixture("grafana", make_grafana)
build_telegram_fixture = _bind_fixture("telegram", make_telegram)
build_brex_fixture = _bind_fixture("brex", make_brex)
build_ramp_fixture = _bind_fixture("ramp", make_ramp)
build_gusto_fixture = _bind_fixture("gusto", make_gusto)
build_deel_fixture = _bind_fixture("deel", make_deel)
build_fireflies_fixture = _bind_fixture(
    "fireflies",
    make_fireflies,
    required_defaults={"workspace_id": "{installation_id}"},
)
build_signal_fixture = _bind_fixture("signal", make_signal)
build_aws_fixture = _bind_fixture(
    "aws",
    make_aws,
    required_defaults={"account_id": "000000000000"},
)
build_miro_fixture = _bind_fixture(
    "miro",
    make_miro,
    required_defaults={"org_id": "{installation_id}"},
)
build_figma_fixture = _bind_fixture(
    "figma",
    make_figma,
    required_defaults={"team_id": "{installation_id}"},
)
build_carta_fixture = _bind_fixture("carta", make_carta)
build_hibob_fixture = _bind_fixture("hibob", make_hibob)
build_ashby_fixture = _bind_fixture("ashby", make_ashby)
build_linkedin_fixture = _bind_fixture("linkedin", make_linkedin)
build_facebook_pages_fixture = _bind_fixture(
    "facebook_pages",
    make_facebook_pages,
    identity_parameter="page_id",
)


__all__ = [
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
]
