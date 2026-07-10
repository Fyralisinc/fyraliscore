"""Instagram installation persistence, contact discovery, and onboarding."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import InstallationCollisionError
from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.instagram.records import business_endpoint_ids


log = structlog.get_logger("integrations.instagram.onboarding")


def _customer_candidate_threshold() -> int:
    try:
        return max(2, int(os.environ.get("INSTAGRAM_CUSTOMER_PROMOTION_MIN_MESSAGES", "3")))
    except ValueError:
        return 3


def _parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _conversation_participant(
    conversation: dict[str, Any],
    *,
    ig_business_account_id: str,
    page_id: str | None = None,
    webhook_delivery_account_id: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    participants = conversation.get("participants")
    data = participants.get("data") if isinstance(participants, dict) else participants
    if not isinstance(data, list):
        return None, None, None
    business_ids = business_endpoint_ids(
        ig_business_account_id,
        page_id,
        webhook_delivery_account_id,
    )
    for item in data:
        if not isinstance(item, dict):
            continue
        participant_id = str(item.get("id") or "").strip()
        if participant_id and participant_id not in business_ids:
            return participant_id, item.get("username"), item.get("name")
    return None, None, None


def _latest_message_at(conversation: dict[str, Any]) -> datetime | None:
    return _parse_time(conversation.get("updated_time"))


def _contact_ref(ig_business_account_id: str, instagram_scoped_user_id: str) -> str:
    return f"instagram:{ig_business_account_id}:user:{instagram_scoped_user_id}"


async def _upsert_contact(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    ig_business_account_id: str,
    instagram_scoped_user_id: str,
    username: str | None = None,
    display_name: str | None = None,
    seen_at: datetime | None = None,
    increment_inbound: bool = False,
) -> UUID:
    row = await conn.fetchrow(
        """
        INSERT INTO instagram_contacts (
            id, tenant_id, instagram_installation_id, instagram_scoped_user_id,
            source_actor_ref, username, display_name, first_seen_at, last_seen_at,
            inbound_message_count, promotion_state
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9,
                  CASE WHEN $10 AND 1 >= $11 THEN 'candidate' ELSE 'unresolved' END)
        ON CONFLICT (instagram_installation_id, instagram_scoped_user_id) DO UPDATE
            SET username = COALESCE(EXCLUDED.username, instagram_contacts.username),
                display_name = COALESCE(EXCLUDED.display_name, instagram_contacts.display_name),
                last_seen_at = GREATEST(
                    COALESCE(instagram_contacts.last_seen_at, EXCLUDED.last_seen_at),
                    COALESCE(EXCLUDED.last_seen_at, instagram_contacts.last_seen_at)
                ),
                inbound_message_count = instagram_contacts.inbound_message_count
                    + CASE WHEN $10 THEN 1 ELSE 0 END,
                promotion_state = CASE
                    WHEN instagram_contacts.promotion_state = 'suppressed' THEN 'suppressed'
                    WHEN instagram_contacts.inbound_message_count
                         + CASE WHEN $10 THEN 1 ELSE 0 END >= $11 THEN 'candidate'
                    ELSE instagram_contacts.promotion_state
                END,
                updated_at = now()
        RETURNING id
        """,
        uuid7(),
        tenant_id,
        installation_id,
        instagram_scoped_user_id,
        _contact_ref(ig_business_account_id, instagram_scoped_user_id),
        username,
        display_name,
        seen_at,
        1 if increment_inbound else 0,
        increment_inbound,
        _customer_candidate_threshold(),
    )
    assert row is not None
    return row["id"]


async def upsert_discovered_conversations(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    ig_business_account_id: str,
    conversations: list[dict[str, Any]],
    page_id: str | None = None,
    webhook_delivery_account_id: str | None = None,
) -> int:
    """Persist a page of authoritative Conversations API discovery results."""
    count = 0
    async with tenant_transaction(tenant_id, pool=pool) as conn:
        for conversation in conversations:
            provider_conversation_id = str(conversation.get("id") or "").strip()
            if not provider_conversation_id:
                continue
            participant_id, username, display_name = _conversation_participant(
                conversation,
                ig_business_account_id=ig_business_account_id,
                page_id=page_id,
                webhook_delivery_account_id=webhook_delivery_account_id,
            )
            thread_key = f"{ig_business_account_id}:{participant_id or provider_conversation_id}"
            contact_id = None
            if participant_id:
                contact_id = await _upsert_contact(
                    conn,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    ig_business_account_id=ig_business_account_id,
                    instagram_scoped_user_id=participant_id,
                    username=username if isinstance(username, str) else None,
                    display_name=display_name if isinstance(display_name, str) else None,
                    seen_at=_latest_message_at(conversation),
                )
            latest_at = _latest_message_at(conversation)
            # A live webhook first creates a row keyed by the local thread;
            # Graph discovery later supplies the provider id. Older installs
            # can instead already have a provider-keyed row. Prefer that row,
            # then fall back to the local thread row, so both migrations are
            # idempotent without relying on an ambiguous conflict target.
            existing_rows = await conn.fetch(
                """
                SELECT id, conversation_id, provider_conversation_id
                  FROM instagram_conversations
                 WHERE instagram_installation_id = $1
                   AND (provider_conversation_id = $2 OR conversation_id = $3)
                 FOR UPDATE
                """,
                installation_id,
                provider_conversation_id,
                thread_key,
            )
            provider_row = next(
                (
                    row for row in existing_rows
                    if row["provider_conversation_id"] == provider_conversation_id
                ),
                None,
            )
            thread_row = next(
                (row for row in existing_rows if row["conversation_id"] == thread_key),
                None,
            )
            # A prior discovery could have treated Meta's delivery-scoped
            # business id as a customer. Prefer the live local customer row
            # and discard that obsolete duplicate before assigning the opaque
            # provider conversation id to the canonical thread.
            if (
                provider_row is not None
                and thread_row is not None
                and provider_row["id"] != thread_row["id"]
            ):
                await conn.execute(
                    "DELETE FROM instagram_conversations WHERE id = $1",
                    provider_row["id"],
                )
                existing_id = thread_row["id"]
            else:
                existing = thread_row or provider_row
                existing_id = existing["id"] if existing is not None else None
            if existing_id is None:
                await conn.execute(
                    """
                    INSERT INTO instagram_conversations (
                        id, tenant_id, instagram_installation_id, conversation_id,
                        thread_key, provider_conversation_id, contact_id,
                        participant_id, participant_username, participant_display_name,
                        last_message_at, provider_updated_at, state
                    ) VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, $9, $10, $10, 'active')
                    """,
                    uuid7(), tenant_id, installation_id, thread_key,
                    provider_conversation_id, contact_id, participant_id, username,
                    display_name, latest_at,
                )
            else:
                await conn.execute(
                    """
                    UPDATE instagram_conversations
                       SET conversation_id = $1,
                           thread_key = $1,
                           provider_conversation_id = $2,
                           contact_id = COALESCE($3, contact_id),
                           participant_id = COALESCE($4, participant_id),
                           participant_username = COALESCE($5, participant_username),
                           participant_display_name = COALESCE($6, participant_display_name),
                           last_message_at = GREATEST(
                               COALESCE(last_message_at, $7),
                               COALESCE($7, last_message_at)
                           ),
                           provider_updated_at = GREATEST(
                               COALESCE(provider_updated_at, $7),
                               COALESCE($7, provider_updated_at)
                           ),
                           state = 'active',
                           updated_at = now()
                     WHERE id = $8
                    """,
                    thread_key, provider_conversation_id, contact_id, participant_id,
                    username, display_name, latest_at, existing_id,
                )
            count += 1
        await conn.execute(
            """
            UPDATE instagram_installations
               SET conversation_discovered_at = now(),
                   conversation_discovery_cursor = NULL,
                   last_health_at = now(),
                   connection_status = 'active',
                   last_error_code = NULL,
                   last_error_at = NULL,
                   updated_at = now()
             WHERE tenant_id = $1 AND id = $2
            """,
            tenant_id,
            installation_id,
        )
    return count


async def record_webhook_contact(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    ig_business_account_id: str,
    customer_id: str | None,
    occurred_at: datetime | None,
    inbound: bool,
) -> bool:
    """Record the lightweight identity from a live webhook without a Graph call."""
    if not customer_id:
        return False
    async with tenant_transaction(tenant_id, pool=pool) as conn:
        contact_id = await _upsert_contact(
            conn,
            tenant_id=tenant_id,
            installation_id=installation_id,
            ig_business_account_id=ig_business_account_id,
            instagram_scoped_user_id=customer_id,
            seen_at=occurred_at,
            increment_inbound=inbound,
        )
        thread_key = f"{ig_business_account_id}:{customer_id}"
        status = await conn.execute(
            """
            INSERT INTO instagram_conversations (
                id, tenant_id, instagram_installation_id, conversation_id,
                thread_key, contact_id, participant_id, last_message_at, state
            ) VALUES ($1, $2, $3, $4, $4, $5, $6, $7, 'active')
            ON CONFLICT (instagram_installation_id, conversation_id) DO UPDATE
                SET contact_id = EXCLUDED.contact_id,
                    last_message_at = GREATEST(
                        COALESCE(instagram_conversations.last_message_at, EXCLUDED.last_message_at),
                        COALESCE(EXCLUDED.last_message_at, instagram_conversations.last_message_at)
                    ),
                    state = 'active',
                    updated_at = now()
            """,
            uuid7(),
            tenant_id,
            installation_id,
            thread_key,
            contact_id,
            customer_id,
            occurred_at,
        )
    return status.startswith("INSERT")


async def schedule_conversation_discovery(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    installation_id: UUID,
) -> bool:
    """Queue one coalesced replay when a webhook sees a new DM thread.

    The webhook path never calls Graph before acknowledging Meta. This durable
    trigger gives the normal source-onboarding planner an opportunity to look
    up the newly seen participant and create an authoritative history shard.
    """
    async with tenant_transaction(tenant_id, pool=pool) as conn:
        status = await conn.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind, installation_row_id, payload
            ) VALUES ($1, $2, 'instagram', 'manual_replay', NULL, $3::jsonb)
            ON CONFLICT (tenant_id, source)
              WHERE source = 'instagram'
                AND trigger_kind = 'manual_replay'
                AND consumed_at IS NULL
              DO NOTHING
            """,
            uuid7(),
            tenant_id,
            json.dumps({
                "reason": "instagram_new_conversation",
                "installation_id": str(installation_id),
            }),
        )
    return status.startswith("INSERT")


async def _ensure_business_actor(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    ig_business_account_id: str,
    display_name: str | None,
) -> UUID:
    existing = await conn.fetchval(
        "SELECT business_actor_id FROM instagram_installations WHERE id = $1",
        installation_id,
    )
    if existing is not None:
        return existing
    mapped = await conn.fetchval(
        """
        SELECT aim.actor_id
          FROM actor_identity_mappings aim
          JOIN actors a ON a.id = aim.actor_id
         WHERE aim.source_channel = 'instagram'
           AND aim.source_actor_ref = $1
           AND a.tenant_id = $2
        """,
        f"business:{ig_business_account_id}",
        tenant_id,
    )
    if mapped is not None:
        await conn.execute(
            "UPDATE instagram_installations SET business_actor_id = $1 WHERE id = $2",
            mapped,
            installation_id,
        )
        return mapped
    actor_id = uuid7()
    name = display_name or f"Instagram account {ig_business_account_id}"
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, metadata)
        VALUES ($1, $2, 'ai_agent', $3, $4::jsonb)
        """,
        actor_id,
        tenant_id,
        name,
        json.dumps({"source": "instagram", "ig_business_account_id": ig_business_account_id}),
    )
    await conn.execute(
        """
        INSERT INTO actor_identity_mappings (
            actor_id, source_channel, source_actor_ref, confidence
        ) VALUES ($1, 'instagram', $2, 1.0)
        ON CONFLICT (source_channel, source_actor_ref) DO NOTHING
        """,
        actor_id,
        f"business:{ig_business_account_id}",
    )
    await conn.execute(
        "UPDATE instagram_installations SET business_actor_id = $1 WHERE id = $2",
        actor_id,
        installation_id,
    )
    return actor_id


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    ig_business_account_id: str,
    page_id: str | None = None,
    instagram_username: str | None = None,
    display_name: str | None = None,
    app_id: str | None = None,
    access_token_ref: str,
    webhook_delivery_account_id: str | None = None,
    history_lookback_days: int = 90,
    conversations: list[dict[str, Any]] | None = None,
    token_expires_at: datetime | None = None,
    auth_model: str = "instagram_login_business",
    granted_scopes: list[str] | None = None,
    webhook_subscription_fields: list[str] | None = None,
    webhook_subscribed_at: datetime | None = None,
) -> UUID:
    """Create or refresh the installation without storing app-level secrets."""
    conversations = conversations or []
    lookback = max(1, min(3650, int(history_lookback_days)))
    async with tenant_transaction(tenant_id, pool=pool) as conn:
        collision = await conn.fetchval(
            """
            SELECT tenant_id FROM instagram_webhook_routes
             WHERE ig_business_account_id = $1
            """,
            ig_business_account_id,
        )
        if collision is not None and collision != tenant_id:
            raise InstallationCollisionError(
                "Instagram professional account is already connected to another tenant"
            )
        row = await conn.fetchrow(
            """
            INSERT INTO instagram_installations (
                id, tenant_id, base_url, auth_model, ig_business_account_id, page_id,
                instagram_username, display_name, app_id, access_token_ref,
                token_expires_at, history_lookback_days, granted_scopes,
                access_token_kind, webhook_subscribed_at, webhook_subscription_fields,
                connection_status, disabled_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb,
                      'instagram_user', $14, $15::jsonb, 'active', NULL)
            ON CONFLICT (tenant_id, ig_business_account_id) DO UPDATE
                SET base_url = EXCLUDED.base_url,
                    auth_model = EXCLUDED.auth_model,
                    page_id = COALESCE(EXCLUDED.page_id, instagram_installations.page_id),
                    instagram_username = COALESCE(EXCLUDED.instagram_username, instagram_installations.instagram_username),
                    display_name = COALESCE(EXCLUDED.display_name, instagram_installations.display_name),
                    app_id = COALESCE(EXCLUDED.app_id, instagram_installations.app_id),
                    access_token_ref = EXCLUDED.access_token_ref,
                    token_expires_at = EXCLUDED.token_expires_at,
                    history_lookback_days = EXCLUDED.history_lookback_days,
                    granted_scopes = EXCLUDED.granted_scopes,
                    webhook_subscribed_at = EXCLUDED.webhook_subscribed_at,
                    webhook_subscription_fields = EXCLUDED.webhook_subscription_fields,
                    connection_status = 'active',
                    disabled_at = NULL,
                    last_error_code = NULL,
                    last_error_at = NULL,
                    updated_at = now()
            RETURNING id
            """,
            uuid7(),
            tenant_id,
            base_url.rstrip("/"),
            auth_model,
            ig_business_account_id,
            page_id,
            instagram_username,
            display_name,
            app_id,
            access_token_ref,
            token_expires_at,
            lookback,
            json.dumps(sorted(set(granted_scopes or []))),
            webhook_subscribed_at,
            json.dumps(sorted(set(webhook_subscription_fields or []))),
        )
        assert row is not None
        installation_id: UUID = row["id"]
        await _ensure_business_actor(
            conn,
            tenant_id=tenant_id,
            installation_id=installation_id,
            ig_business_account_id=ig_business_account_id,
            display_name=display_name,
        )
        await conn.execute(
            """
            INSERT INTO instagram_webhook_routes (
                id, tenant_id, instagram_installation_id, ig_business_account_id,
                page_id, app_id, webhook_delivery_account_id, enabled
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
            ON CONFLICT (ig_business_account_id) DO UPDATE
                SET instagram_installation_id = EXCLUDED.instagram_installation_id,
                    page_id = COALESCE(EXCLUDED.page_id, instagram_webhook_routes.page_id),
                    app_id = COALESCE(EXCLUDED.app_id, instagram_webhook_routes.app_id),
                    webhook_delivery_account_id = COALESCE(
                        EXCLUDED.webhook_delivery_account_id,
                        instagram_webhook_routes.webhook_delivery_account_id
                    ),
                    enabled = TRUE,
                    updated_at = now()
            """,
            uuid7(),
            tenant_id,
            installation_id,
            ig_business_account_id,
            page_id,
            app_id,
            webhook_delivery_account_id,
        )
        await conn.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind, installation_row_id, payload
            ) VALUES ($1, $2, 'instagram', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL DO NOTHING
            """,
            uuid7(),
            tenant_id,
            installation_id,
            json.dumps({"ig_business_account_id": ig_business_account_id, "history_lookback_days": lookback}),
        )

    if conversations:
        await upsert_discovered_conversations(
            pool,
            tenant_id=tenant_id,
            installation_id=installation_id,
            ig_business_account_id=ig_business_account_id,
            conversations=conversations,
            page_id=page_id,
            webhook_delivery_account_id=webhook_delivery_account_id,
        )
    log.info("instagram_install_finalized", installation_id=str(installation_id))
    return installation_id


__all__ = [
    "finalize_install",
    "record_webhook_contact",
    "schedule_conversation_discovery",
    "upsert_discovered_conversations",
]
