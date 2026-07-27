"""Source-owned behavior bindings for the deterministic validation runner.

This module deliberately contains no source registry.  Every exported callable
is referenced by the canonical source catalog.  Shared orchestration resolves
the selected source's binding and invokes it without selecting provider
behavior itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from lib.shared.ids import uuid7
from services.ingest.ingestion.feature_flags.client import KAFKA_PATH_ENABLED
from services.ingest.synthetic.fixtures import (
    make_discord_guild,
    make_figma,
    make_gmail_mailbox,
    make_github_repos,
    make_signal,
    make_slack_workspace,
    make_telegram,
)
from services.ingest.synthetic.live_generators import (
    AwsPollGenerator,
    CartaPollGenerator,
    DiscordGatewayGenerator,
    FacebookPagesWebhookGenerator,
    GithubWebhookGenerator,
    GmailPubSubGenerator,
    GooglePushGenerator,
    GuildBinding,
    HmacWebhookGenerator,
    LinkedinPollGenerator,
    MiroPollGenerator,
    NotionWebhookGenerator,
    SignalGatewayGenerator,
    SlackWebhookGenerator,
    TelegramGatewayGenerator,
    WhatsAppWebhookGenerator,
)
from services.ingest.synthetic.mock_clients import (
    MockDiscordClient,
    MockGithubClient,
    MockGmailClient,
    MockSlackClient,
)


@dataclass(frozen=True)
class LiveGeneratorBuildContext:
    """Dependency bundle passed to one catalog-selected generator builder."""

    source: str
    targets: tuple[Any, ...]
    stack: Any
    pool: asyncpg.Pool
    shared_app: Any
    gmail_app: Any
    google_app: Any
    secrets: Any
    cutover: Any
    discord_dispatch_deps: Any


def _target(
    tenant_id: UUID,
    source: str,
    slug: str,
    **values: Any,
) -> Any:
    # Imported lazily to avoid a module cycle: composition owns the stable
    # public LiveTarget type while it resolves these catalog callables.
    from services.ingest.synthetic.validation_runs.composition import LiveTarget

    return LiveTarget(
        tenant_id=tenant_id,
        source=source,
        slug=slug,
        **values,
    )


def build_gmail_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(tenant_id, source, slug, email=fixture["email"])


def build_slack_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id,
        source,
        slug,
        team_id=f"x3-{slug}-slack",
        channel_id=f"C_LIVE_{slug}",
    )


def build_discord_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id,
        source,
        slug,
        guild_id=f"x3-{slug}-discord",
        channel_id=f"chan_live_{slug}",
    )


def build_github_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id,
        source,
        slug,
        installation_id=f"x3-{slug}-github",
        repo_full_name=f"{fixture.get('org_or_user', slug)}/live-{slug}",
    )


def build_jira_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = f"{slug}.atlassian.net"
    return _target(
        tenant_id, source, slug, jira_site=value,
        provider_installation_id=value,
    )


def build_mercury_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = f"live-org-{slug}"
    return _target(
        tenant_id, source, slug, mercury_org=value,
        mercury_account=f"live-acct-{slug}", provider_installation_id=value,
    )


def build_quickbooks_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = f"live-realm-{slug}"
    return _target(
        tenant_id, source, slug, qbo_realm=value, qbo_entity="Invoice",
        provider_installation_id=value,
    )


def build_grafana_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = f"{slug}.grafana.net"
    return _target(
        tenant_id, source, slug, grafana_instance=value,
        provider_installation_id=value,
    )


def build_brex_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = f"live-brex-org-{slug}"
    return _target(
        tenant_id, source, slug, brex_org=value,
        brex_account=f"live-brex-acct-{slug}", provider_installation_id=value,
    )


def build_ramp_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = f"live-ramp-biz-{slug}"
    return _target(
        tenant_id, source, slug, ramp_business=value,
        provider_installation_id=value,
    )


def build_gusto_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = f"live-gusto-co-{slug}"
    return _target(
        tenant_id, source, slug, gusto_company=value,
        provider_installation_id=value,
    )


def build_deel_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = f"live-deel-org-{slug}"
    return _target(
        tenant_id, source, slug, deel_org=value,
        provider_installation_id=value,
    )


def build_google_calendar_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id, source, slug, gcal_calendar_id=f"live-{slug}",
        gcal_channel_id=f"chan-gcal-{slug}",
        gcal_watch_token=f"tok-gcal-{slug}",
    )


def build_google_drive_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id, source, slug, gdrive_drive_id=f"live-{slug}",
        gdrive_kind="my_drive", gdrive_channel_id=f"chan-gdrive-{slug}",
        gdrive_watch_token=f"tok-gdrive-{slug}",
    )


def build_notion_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id, source, slug, notion_workspace_id=f"x3-{slug}-notion",
    )


def build_telegram_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    data = (
        fixture
        if "dialog_order" in fixture and "dialogs" in fixture
        else make_telegram(**fixture)
    )
    dialog_id = data["dialog_order"][0]
    dialog = data["dialogs"][str(dialog_id)]
    return _target(
        tenant_id, source, slug, telegram_dialog_id=dialog_id,
        telegram_dialog_kind=dialog["dialog_kind"],
        telegram_dialog_title=dialog["title"],
    )


def build_fireflies_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = fixture["workspace_id"]
    return _target(
        tenant_id, source, slug, fireflies_workspace=value,
        provider_installation_id=value,
    )


def build_miro_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id, source, slug, miro_org=fixture["org_id"],
        miro_board=f"miro-board-{slug}",
    )


def build_figma_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    data = (
        fixture
        if "file_order" in fixture and "files" in fixture
        else make_figma(**fixture)
    )
    webhook_id = f"figwh-{slug}"
    return _target(
        tenant_id, source, slug, figma_webhook_id=webhook_id,
        figma_team=fixture["team_id"], figma_file=data["file_order"][0],
        provider_installation_id=webhook_id,
    )


def build_signal_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    data = (
        fixture
        if "thread_order" in fixture and "threads" in fixture
        else make_signal(**fixture)
    )
    thread_id = data["thread_order"][0]
    thread = data["threads"][str(thread_id)]
    return _target(
        tenant_id, source, slug, signal_thread_id=thread_id,
        signal_thread_kind=thread["thread_kind"],
        signal_thread_title=thread["title"],
    )


def build_aws_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id, source, slug, aws_account_id=fixture.get("account_id"),
        aws_region=fixture.get("region", "us-east-1"),
    )


def build_carta_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id, source, slug, carta_firm=fixture.get("firm_id"),
        carta_entity_type="optionGrant",
    )


def build_hibob_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = fixture["company_id"]
    return _target(
        tenant_id, source, slug, hibob_company=value,
        provider_installation_id=value,
    )


def build_ashby_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    value = fixture["org_id"]
    return _target(
        tenant_id, source, slug, ashby_org=value,
        provider_installation_id=value,
    )


def build_linkedin_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id, source, slug, linkedin_org=fixture["organization_urn"],
        linkedin_entity_type="post",
    )


def build_whatsapp_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    phone_number_id = fixture.get("phone_number_id")
    if not isinstance(phone_number_id, str) or not phone_number_id:
        raise ValueError(
            "whatsapp live target requires an explicit phone_number_id",
        )
    return _target(
        tenant_id, source, slug, whatsapp_phone_number_id=phone_number_id,
    )


def build_facebook_pages_live_target(
    tenant_id: UUID, source: str, slug: str, fixture: dict[str, Any],
) -> Any:
    return _target(
        tenant_id, source, slug,
        facebook_page_id=fixture.get("page_id") or f"x3-{slug}-facebook_pages",
    )


async def _enter(context: LiveGeneratorBuildContext, generator: Any) -> Any:
    return await context.stack.enter_async_context(generator)


async def build_gmail_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    mailboxes = {
        target.email: MockGmailClient(
            fixture=make_gmail_mailbox(
                email=target.email,
                messages=0,
                starting_history_id=1000,
            ),
        )
        for target in context.targets
    }
    tenant_ids = {
        target.email.lower(): target.tenant_id
        for target in context.targets
        if target.email is not None
    }
    return await _enter(
        context,
        GmailPubSubGenerator(
            app=context.gmail_app,
            pool=context.pool,
            mailboxes=mailboxes,
            tenant_ids_by_email=tenant_ids,
            s3_raw_client=context.cutover.s3_raw_client,
            kafka_producer=context.cutover.kafka_producer,
            tenant_flags=context.cutover.tenant_flags,
        ),
    )


async def build_slack_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    mock = MockSlackClient(
        fixture=make_slack_workspace(
            team_id="LIVE_SHARED",
            channels=1,
            messages_per_channel=0,
        ),
    )
    return await _enter(
        context,
        SlackWebhookGenerator(
            app=context.shared_app,
            mock_client=mock,
            signing_secret=context.secrets.slack,
        ),
    )


async def build_github_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    mock = MockGithubClient(
        fixture=make_github_repos(
            org_or_user="live",
            repos=1,
            events_per_repo=0,
            installation_id="live-shared",
        ),
    )
    return await _enter(
        context,
        GithubWebhookGenerator(
            app=context.shared_app,
            mock_client=mock,
            signing_secret=context.secrets.github,
        ),
    )


async def build_discord_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    guilds: dict[str | None, GuildBinding] = {}
    for target in context.targets:
        fixture = make_discord_guild(
            guild_id=target.guild_id,
            channels=1,
            messages_per_channel=0,
        )
        fixture["channels"][0]["id"] = target.channel_id
        guilds[target.guild_id] = GuildBinding(
            guild_id=target.guild_id,
            mock_client=MockDiscordClient(fixture=fixture),
        )
    return await _enter(
        context,
        DiscordGatewayGenerator(
            dispatch_deps=context.discord_dispatch_deps,
            guild_bindings=guilds,
        ),
    )


async def build_hmac_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _enter(
        context,
        HmacWebhookGenerator(
            app=context.shared_app,
            provider=context.source,
            signing_secret=getattr(context.secrets, context.source),
        ),
    )


async def build_google_push_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _enter(
        context,
        GooglePushGenerator(app=context.google_app, pool=context.pool),
    )


async def build_notion_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _enter(
        context,
        NotionWebhookGenerator(
            app=context.shared_app,
            kafka_producer=context.cutover.kafka_producer,
            s3_raw_client=context.cutover.s3_raw_client,
            verification_token=context.secrets.notion,
        ),
    )


async def _build_direct_generator(
    context: LiveGeneratorBuildContext,
    generator: type[Any],
) -> Any:
    return await _enter(
        context,
        generator(
            pool=context.pool,
            kafka_producer=context.cutover.kafka_producer,
            s3_raw_client=context.cutover.s3_raw_client,
            tenant_flags=context.cutover.tenant_flags,
        ),
    )


async def build_telegram_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _build_direct_generator(context, TelegramGatewayGenerator)


async def build_signal_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _build_direct_generator(context, SignalGatewayGenerator)


async def build_aws_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _build_direct_generator(context, AwsPollGenerator)


async def build_miro_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _build_direct_generator(context, MiroPollGenerator)


async def build_carta_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _build_direct_generator(context, CartaPollGenerator)


async def build_linkedin_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _build_direct_generator(context, LinkedinPollGenerator)


async def build_whatsapp_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _enter(
        context,
        WhatsAppWebhookGenerator(
            app=context.shared_app,
            pool=context.pool,
            app_secret=context.secrets.whatsapp,
            kafka_producer=context.cutover.kafka_producer,
            s3_raw_client=context.cutover.s3_raw_client,
            tenant_flags=context.cutover.tenant_flags,
        ),
    )


async def build_facebook_pages_live_generator(
    context: LiveGeneratorBuildContext,
) -> Any:
    return await _enter(
        context,
        FacebookPagesWebhookGenerator(
            app=context.shared_app,
            pool=context.pool,
            app_secret=context.secrets.facebook_pages,
            kafka_producer=context.cutover.kafka_producer,
            s3_raw_client=context.cutover.s3_raw_client,
            tenant_flags=context.cutover.tenant_flags,
        ),
    )


build_jira_live_generator = build_hmac_live_generator
build_mercury_live_generator = build_hmac_live_generator
build_quickbooks_live_generator = build_hmac_live_generator
build_grafana_live_generator = build_hmac_live_generator
build_brex_live_generator = build_hmac_live_generator
build_ramp_live_generator = build_hmac_live_generator
build_gusto_live_generator = build_hmac_live_generator
build_deel_live_generator = build_hmac_live_generator
build_fireflies_live_generator = build_hmac_live_generator
build_figma_live_generator = build_hmac_live_generator
build_hibob_live_generator = build_hmac_live_generator
build_ashby_live_generator = build_hmac_live_generator
build_google_calendar_live_generator = build_google_push_live_generator
build_google_drive_live_generator = build_google_push_live_generator


def _generator(drivers: Any, source: str) -> Any:
    return drivers.generator_for(source)


async def dispatch_gmail_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_push(
        mailbox_email=target.email,
        new_messages=1,
    )


async def dispatch_slack_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_message(
        team_id=target.team_id,
        channel_id=target.channel_id,
        content=f"live-{target.slug}-{event_index}",
    )


async def dispatch_github_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_issue_event(
        installation_id=target.installation_id,
        repo_full_name=target.repo_full_name,
        issue_title=f"live-{target.slug}-{event_index}",
    )


async def dispatch_discord_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_message_create(
        guild_id=target.guild_id,
        channel_id=target.channel_id,
        content=f"live-{target.slug}-{event_index}",
    )


async def dispatch_hmac_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_event(
        target=target,
        content=f"live-{target.slug}-{event_index}",
    )


async def dispatch_google_push_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_push(target=target)


async def dispatch_notion_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_event(target=target)


async def dispatch_message_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_message(
        target=target,
        content=f"live-{target.slug}-{event_index}",
    )


async def dispatch_poll_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_event(
        target=target,
        content=f"live-{target.slug}-{event_index}",
    )


async def dispatch_whatsapp_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_message(
        target=target,
        content=f"live-{target.slug}-{event_index}",
    )


async def dispatch_facebook_pages_live_event(
    drivers: Any, target: Any, event_index: int,
) -> Any:
    return await _generator(drivers, target.source).simulate_message(
        target=target,
        content=f"live-{target.slug}-{event_index}",
    )


dispatch_jira_live_event = dispatch_hmac_live_event
dispatch_mercury_live_event = dispatch_hmac_live_event
dispatch_quickbooks_live_event = dispatch_hmac_live_event
dispatch_grafana_live_event = dispatch_hmac_live_event
dispatch_brex_live_event = dispatch_hmac_live_event
dispatch_ramp_live_event = dispatch_hmac_live_event
dispatch_gusto_live_event = dispatch_hmac_live_event
dispatch_deel_live_event = dispatch_hmac_live_event
dispatch_fireflies_live_event = dispatch_hmac_live_event
dispatch_figma_live_event = dispatch_hmac_live_event
dispatch_hibob_live_event = dispatch_hmac_live_event
dispatch_ashby_live_event = dispatch_hmac_live_event
dispatch_google_calendar_live_event = dispatch_google_push_live_event
dispatch_google_drive_live_event = dispatch_google_push_live_event
dispatch_telegram_live_event = dispatch_message_live_event
dispatch_signal_live_event = dispatch_message_live_event
dispatch_aws_live_event = dispatch_poll_live_event
dispatch_miro_live_event = dispatch_poll_live_event
dispatch_carta_live_event = dispatch_poll_live_event
dispatch_linkedin_live_event = dispatch_poll_live_event


async def dispatch_slack_twin(
    drivers: Any, target: Any, twin: Any,
) -> str:
    channel, _, timestamp = twin.external_id.partition(":")
    await _generator(drivers, target.source).simulate_message(
        team_id=target.team_id,
        channel_id=channel,
        content="twin",
        ts=timestamp,
    )
    return twin.external_id


async def dispatch_github_twin(
    drivers: Any, target: Any, twin: Any,
) -> str:
    node_id, _, action = twin.external_id.rpartition(":")
    await _generator(drivers, target.source).simulate_issue_event(
        installation_id=target.installation_id,
        repo_full_name=target.repo_full_name,
        node_id=node_id,
        action=action or "opened",
        occurred_at_iso=twin.occurred_at.isoformat(),
    )
    return twin.external_id


async def dispatch_gmail_twin(
    drivers: Any, target: Any, twin: Any,
) -> str:
    parts = twin.external_id.split(":", 2)
    message_id = parts[2] if len(parts) == 3 else parts[-1]
    internal_date = str(int(twin.occurred_at.timestamp() * 1000))
    await _generator(drivers, target.source).simulate_push(
        mailbox_email=target.email,
        new_messages=1,
        message_id=message_id,
        internal_date=internal_date,
    )
    return twin.external_id


async def probe_hmac_signature(drivers: Any, target: Any) -> int:
    result = await _generator(drivers, target.source).simulate_event(
        target=target,
        content="tampered",
        tamper_signature=True,
    )
    return int(result.http_status)


async def probe_slack_signature(drivers: Any, target: Any) -> int:
    result = await _generator(drivers, target.source).simulate_message(
        team_id=target.team_id,
        channel_id=target.channel_id,
        content="tampered",
        tamper_signature=True,
    )
    return int(result.http_status)


async def probe_github_signature(drivers: Any, target: Any) -> int:
    result = await _generator(drivers, target.source).simulate_issue_event(
        installation_id=target.installation_id,
        repo_full_name=target.repo_full_name,
        issue_title="tampered",
        tamper_signature=True,
    )
    return int(result.http_status)


async def probe_notion_signature(drivers: Any, target: Any) -> int:
    result = await _generator(drivers, target.source).simulate_event(
        target=target,
        tamper_signature=True,
    )
    return int(result.http_status)


async def probe_whatsapp_signature(drivers: Any, target: Any) -> int:
    result = await _generator(drivers, target.source).simulate_message(
        target=target,
        content="tampered",
        tamper_signature=True,
    )
    return int(result.http_status)


async def probe_facebook_pages_signature(
    drivers: Any, target: Any,
) -> int:
    result = await _generator(drivers, target.source).simulate_message(
        target=target,
        content="tampered",
        tamper_signature=True,
    )
    return int(result.http_status)


probe_jira_signature = probe_hmac_signature
probe_mercury_signature = probe_hmac_signature
probe_quickbooks_signature = probe_hmac_signature
probe_grafana_signature = probe_hmac_signature
probe_brex_signature = probe_hmac_signature
probe_ramp_signature = probe_hmac_signature
probe_gusto_signature = probe_hmac_signature
probe_deel_signature = probe_hmac_signature
probe_fireflies_signature = probe_hmac_signature
probe_figma_signature = probe_hmac_signature
probe_hibob_signature = probe_hmac_signature
probe_ashby_signature = probe_hmac_signature


async def probe_slack_replay(drivers: Any, target: Any) -> None:
    channel = f"{target.channel_id}_rp"
    await _generator(drivers, target.source).simulate_message(
        team_id=target.team_id,
        channel_id=channel,
        content="replay-unique",
    )
    await _generator(drivers, target.source).simulate_message(
        team_id=target.team_id,
        channel_id=channel,
        content="replay-unique",
        replay=True,
    )


async def probe_github_replay(drivers: Any, target: Any) -> None:
    repo = f"{target.repo_full_name}-rp"
    await _generator(drivers, target.source).simulate_issue_event(
        installation_id=target.installation_id,
        repo_full_name=repo,
        issue_title="replay-unique",
    )
    await _generator(drivers, target.source).simulate_issue_event(
        installation_id=target.installation_id,
        repo_full_name=repo,
        replay=True,
    )


async def probe_gmail_replay(drivers: Any, target: Any) -> None:
    await _generator(drivers, target.source).simulate_push(
        mailbox_email=target.email,
        new_messages=1,
    )
    await _generator(drivers, target.source).simulate_push(
        mailbox_email=target.email,
        new_messages=0,
        replay=True,
    )


@dataclass(frozen=True)
class LiveInstallContext:
    conn: asyncpg.Connection
    secrets: Any
    secret_store: Any


async def _ensure_secret(
    context: LiveInstallContext,
    *,
    current_ref: Any,
    plaintext: str,
    tenant_id: UUID,
    label: str,
) -> str:
    from lib.shared.errors import SecretNotFoundError

    if current_ref:
        try:
            await context.secret_store.rotate(
                str(current_ref),
                plaintext,
                tenant_id=tenant_id,
            )
            return str(current_ref)
        except (SecretNotFoundError, ValueError):
            pass
    return await context.secret_store.put(
        plaintext,
        label=label,
        tenant_id=tenant_id,
    )


async def seed_noop_live_install(
    context: LiveInstallContext, target: Any,
) -> None:
    return None


async def seed_provider_live_install(
    context: LiveInstallContext, target: Any,
) -> None:
    installation_id = target.provider_installation_id
    if not installation_id:
        raise RuntimeError(
            f"{target.source} validation target has no provider installation id"
        )
    existing = await context.conn.fetch(
        """
        SELECT id, tenant_id
          FROM provider_installations
         WHERE provider = $1
           AND installation_id = $2
        """,
        target.source,
        installation_id,
    )
    if len(existing) > 1:
        raise RuntimeError(
            "live certification found ambiguous provider binding "
            f"provider={target.source} installation_id={installation_id}; "
            f"found {len(existing)}",
        )
    if existing and existing[0]["tenant_id"] != target.tenant_id:
        raise RuntimeError(
            "live certification refuses to rebind provider installation "
            f"across tenants: provider={target.source} "
            f"installation_id={installation_id} "
            f"existing_tenant={existing[0]['tenant_id']} "
            f"requested_tenant={target.tenant_id}",
        )
    if existing:
        await context.conn.execute(
            """
            UPDATE provider_installations
               SET enabled = TRUE
             WHERE id = $1
               AND tenant_id = $2
            """,
            existing[0]["id"],
            target.tenant_id,
        )
        return
    await context.conn.execute(
        """
        INSERT INTO provider_installations
          (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, $3, $4, NULL, TRUE)
        """,
        uuid7(),
        target.tenant_id,
        target.source,
        installation_id,
    )


async def seed_google_calendar_live_install(
    context: LiveInstallContext, target: Any,
) -> None:
    rows = await context.conn.fetch(
        "SELECT id FROM google_calendar_installations "
        "WHERE tenant_id = $1 AND workspace_domain = $2 "
        "AND disabled_at IS NULL",
        target.tenant_id,
        f"x3-{target.slug}.example",
    )
    if len(rows) != 1:
        raise RuntimeError(
            "google_calendar live certification requires exactly one active "
            f"installation for tenant={target.tenant_id} "
            f"workspace_domain=x3-{target.slug}.example; found {len(rows)}"
        )
    await context.conn.execute(
        """
        INSERT INTO google_calendar_calendars (
            id, tenant_id, google_calendar_installation_id,
            calendar_id, owner_email, sync_token, state,
            watch_channel_id, watch_token, watch_state
        ) VALUES ($1, $2, $3, $4, $5, 'sync-warm', 'active',
                  $6, $7, 'active')
        ON CONFLICT (google_calendar_installation_id, calendar_id)
            DO UPDATE SET sync_token = 'sync-warm',
                watch_channel_id = EXCLUDED.watch_channel_id,
                watch_token = EXCLUDED.watch_token,
                watch_state = 'active'
        """,
        uuid7(),
        target.tenant_id,
        rows[0]["id"],
        target.gcal_calendar_id,
        target.gcal_calendar_id,
        target.gcal_channel_id,
        target.gcal_watch_token,
    )


async def seed_google_drive_live_install(
    context: LiveInstallContext, target: Any,
) -> None:
    rows = await context.conn.fetch(
        "SELECT id FROM google_drive_installations "
        "WHERE tenant_id = $1 AND workspace_domain = $2 "
        "AND disabled_at IS NULL",
        target.tenant_id,
        f"x3-{target.slug}.example",
    )
    if len(rows) != 1:
        raise RuntimeError(
            "google_drive live certification requires exactly one active "
            f"installation for tenant={target.tenant_id} "
            f"workspace_domain=x3-{target.slug}.example; found {len(rows)}"
        )
    await context.conn.execute(
        """
        INSERT INTO google_drive_targets (
            id, tenant_id, google_drive_installation_id,
            drive_kind, drive_id, owner_email, start_page_token,
            state, watch_channel_id, watch_token, watch_state
        ) VALUES ($1, $2, $3, $4, $5, $6, 'live-start', 'active',
                  $7, $8, 'active')
        ON CONFLICT (google_drive_installation_id, drive_kind,
                     drive_id, owner_email)
            DO UPDATE SET start_page_token = 'live-start',
                watch_channel_id = EXCLUDED.watch_channel_id,
                watch_token = EXCLUDED.watch_token,
                watch_state = 'active'
        """,
        uuid7(),
        target.tenant_id,
        rows[0]["id"],
        target.gdrive_kind,
        target.gdrive_drive_id,
        target.gdrive_drive_id,
        target.gdrive_channel_id,
        target.gdrive_watch_token,
    )


async def seed_whatsapp_live_install(
    context: LiveInstallContext, target: Any,
) -> None:
    rows = await context.conn.fetch(
        """
        SELECT id, app_secret_ref
          FROM whatsapp_installations
         WHERE tenant_id = $1
           AND phone_number_id = $2
           AND enabled = true
        """,
        target.tenant_id,
        target.whatsapp_phone_number_id,
    )
    if len(rows) != 1:
        raise RuntimeError(
            "whatsapp live certification requires exactly one enabled "
            f"installation for tenant={target.tenant_id} "
            f"phone_number_id={target.whatsapp_phone_number_id}; "
            f"found {len(rows)}",
        )
    app_secret_ref = await _ensure_secret(
        context,
        current_ref=rows[0]["app_secret_ref"],
        plaintext=context.secrets.whatsapp,
        tenant_id=target.tenant_id,
        label=(
            "synthetic_whatsapp_app_secret:"
            f"{target.whatsapp_phone_number_id}"
        ),
    )
    await context.conn.execute(
        """
        UPDATE whatsapp_installations
           SET app_secret = NULL,
               app_secret_ref = $2,
               updated_at = now()
         WHERE id = $1
        """,
        rows[0]["id"],
        app_secret_ref,
    )


async def seed_facebook_pages_live_install(
    context: LiveInstallContext, target: Any,
) -> None:
    rows = await context.conn.fetch(
        """
        SELECT id, app_secret_ref
          FROM facebook_page_installations
         WHERE tenant_id = $1
           AND page_id = $2
           AND enabled = true
        """,
        target.tenant_id,
        target.facebook_page_id,
    )
    if len(rows) != 1:
        raise RuntimeError(
            "facebook_pages live certification requires exactly one enabled "
            f"installation for tenant={target.tenant_id} "
            f"page_id={target.facebook_page_id}; found {len(rows)}",
        )
    app_secret_ref = await _ensure_secret(
        context,
        current_ref=rows[0]["app_secret_ref"],
        plaintext=context.secrets.facebook_pages,
        tenant_id=target.tenant_id,
        label=f"synthetic_facebook_pages_app_secret:{target.facebook_page_id}",
    )
    await context.conn.execute(
        """
        UPDATE facebook_page_installations
           SET app_secret_ref = $2,
               updated_at = now()
         WHERE id = $1
        """,
        rows[0]["id"],
        app_secret_ref,
    )


seed_slack_live_install = seed_noop_live_install
seed_github_live_install = seed_noop_live_install
seed_discord_live_install = seed_noop_live_install
seed_gmail_live_install = seed_noop_live_install
seed_notion_live_install = seed_noop_live_install
seed_telegram_live_install = seed_noop_live_install
seed_signal_live_install = seed_noop_live_install
seed_aws_live_install = seed_noop_live_install
seed_miro_live_install = seed_noop_live_install
seed_carta_live_install = seed_noop_live_install
seed_linkedin_live_install = seed_noop_live_install
seed_jira_live_install = seed_provider_live_install
seed_mercury_live_install = seed_provider_live_install
seed_quickbooks_live_install = seed_provider_live_install
seed_grafana_live_install = seed_provider_live_install
seed_brex_live_install = seed_provider_live_install
seed_ramp_live_install = seed_provider_live_install
seed_gusto_live_install = seed_provider_live_install
seed_deel_live_install = seed_provider_live_install
seed_fireflies_live_install = seed_provider_live_install
seed_figma_live_install = seed_provider_live_install
seed_hibob_live_install = seed_provider_live_install
seed_ashby_live_install = seed_provider_live_install


async def bootstrap_whatsapp_live_only(
    pool: asyncpg.Pool,
    *,
    tenants_per_source: int,
    source: str,
) -> list[Any]:
    targets: list[Any] = []
    for tenant_index in range(tenants_per_source):
        tenant_id = uuid4()
        slug = f"val-{source}-live-{tenant_index}"
        phone_number_id = f"x3-{slug}-whatsapp"
        await pool.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            tenant_id,
            f"x3-{slug}-{tenant_id.hex[:8]}",
        )
        await pool.execute(
            """
            INSERT INTO tenant_flags
                (tenant_id, flag_name, flag_value, set_by)
            VALUES ($1, $2, TRUE, 'contract-live-only-runner')
            """,
            tenant_id,
            KAFKA_PATH_ENABLED,
        )
        await pool.execute(
            """
            INSERT INTO whatsapp_installations (
                tenant_id, phone_number_id, waba_id,
                display_phone_number, app_secret, verify_token,
                access_token, app_secret_ref, verify_token_ref,
                access_token_ref, enabled
            )
            VALUES (
                $1, $2, $3, '+15550000000',
                NULL, NULL, NULL, NULL, NULL, NULL, TRUE
            )
            """,
            tenant_id,
            phone_number_id,
            f"waba-{phone_number_id}",
        )
        targets.append(
            build_whatsapp_live_target(
                tenant_id,
                source,
                slug,
                {"phone_number_id": phone_number_id},
            )
        )
    return targets
