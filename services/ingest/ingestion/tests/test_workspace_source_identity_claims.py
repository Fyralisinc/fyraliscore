from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.source_identity_bindings import SourceIdentityBindingRepo
from services.ingest.ingestion.core import ingest
from services.workers.entity_resolver.context import build_context


DRIVE_FILE_ID = "drive-file-1"
DRIVE_NATIVE_ID = f"google_drive:file:{DRIVE_FILE_ID}"
DRIVE_FILE_NAME = "Q3 Planning"
GMAIL_INSTALLATION_ID = "00000000-0000-0000-0000-000000000002"
GMAIL_THREAD_ID = "thread-1"
GMAIL_NATIVE_ID = (
    f"gmail:{GMAIL_INSTALLATION_ID}:thread:{GMAIL_THREAD_ID}"
)
GMAIL_SUBJECT = "Executive Planning"
EVENT_TIME = datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc)


def _drive_payload(record_type: str) -> dict[str, Any]:
    if record_type == "file":
        return {
            "id": DRIVE_FILE_ID,
            "name": DRIVE_FILE_NAME,
            "version": "7",
            "modifiedTime": EVENT_TIME.isoformat().replace("+00:00", "Z"),
            "_fyralis_extracted_text": (
                "SALES is untrusted free text, not file identity."
            ),
        }
    if record_type == "comment":
        return {
            "_fyralis_record_type": "comment",
            "_fyralis_file_id": DRIVE_FILE_ID,
            "_fyralis_file_name": DRIVE_FILE_NAME,
            "id": "comment-1",
            "content": "SALES is untrusted free text.",
            "modifiedTime": EVENT_TIME.isoformat().replace("+00:00", "Z"),
        }
    return {
        "_fyralis_record_type": "revision",
        "_fyralis_file_id": DRIVE_FILE_ID,
        "_fyralis_file_name": DRIVE_FILE_NAME,
        "id": "revision-1",
        "modifiedTime": EVENT_TIME.isoformat().replace("+00:00", "Z"),
        "lastModifyingUser": {"displayName": "SALES"},
    }


def _gmail_payload(
    *,
    message_id: str = "message-1@example.com",
    thread_id: str | None = GMAIL_THREAD_ID,
    subject: str = GMAIL_SUBJECT,
) -> dict[str, Any]:
    headers = [
        {"name": "Message-ID", "value": f"<{message_id}>"},
        {"name": "From", "value": "Alice <alice@example.com>"},
        {"name": "To", "value": "bob@example.com"},
        {"name": "Subject", "value": subject},
    ]
    return {
        "message_resource": {
            "id": f"gmail-{message_id}",
            "threadId": thread_id,
            "snippet": "Q3 Planning is untrusted free text.",
            "internalDate": str(int(EVENT_TIME.timestamp() * 1000)),
            "payload": {"headers": headers},
        },
        "mailbox_email": "alice@example.com",
        "scope_used": "gmail.metadata",
        "read_path": "push",
        "gmail_installation_id": GMAIL_INSTALLATION_ID,
        "thread_canonical_id": str(uuid7()),
    }


async def _seed_binding(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source_system: str,
    native_id: str,
    identity: str,
):
    resource_id = uuid7()
    await pool.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value, metadata
        ) VALUES (
            $1, $2, 'capacity', $3,
            jsonb_build_object('name', $3::text),
            '{"semantic_kind":"source_object"}'::jsonb
        )
        """,
        resource_id,
        tenant_id,
        identity,
    )
    binding = await SourceIdentityBindingRepo(pool).bind(
        tenant_id=tenant_id,
        source_system=source_system,
        source_native_identifier=native_id,
        source_identity_authority_ref=(
            f"{source_system}-structured-object-contract-v1"
        ),
        canonical_ref={"type": "resource", "id": str(resource_id)},
        evidence_refs=(f"source-object:{native_id}",),
        valid_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    return resource_id, binding


@pytest.mark.parametrize("record_type", ["file", "comment", "revision"])
async def test_drive_records_attach_exact_file_identity(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
    record_type: str,
) -> None:
    resource_id, binding = await _seed_binding(
        gateway_pool,
        tenant_id=tenant_id,
        source_system="google_drive",
        native_id=DRIVE_NATIVE_ID,
        identity=DRIVE_FILE_NAME,
    )
    result = await ingest(
        "google_drive:file",
        _drive_payload(record_type),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )

    attachment = await gateway_pool.fetchrow(
        """
        SELECT binding_id, binding_version, source_surface
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        result.observation.id,
    )
    assert str(attachment["binding_id"]) == binding.binding_id
    assert attachment["binding_version"] == binding.binding_version
    assert attachment["source_surface"] == DRIVE_FILE_NAME

    exact = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase=DRIVE_FILE_NAME,
    )
    forged = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase="SALES",
    )
    assert exact.source_identity_binding is not None
    assert exact.source_identity_binding.canonical_ref["id"] == str(
        resource_id
    )
    assert forged.source_identity_binding is None


async def test_gmail_message_attaches_exact_thread_subject_identity(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    resource_id, binding = await _seed_binding(
        gateway_pool,
        tenant_id=tenant_id,
        source_system="gmail",
        native_id=GMAIL_NATIVE_ID,
        identity=GMAIL_SUBJECT,
    )
    result = await ingest(
        "gmail:",
        _gmail_payload(),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )

    attachment = await gateway_pool.fetchrow(
        """
        SELECT binding_id, binding_version, source_surface
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        result.observation.id,
    )
    assert str(attachment["binding_id"]) == binding.binding_id
    assert attachment["binding_version"] == binding.binding_version
    assert attachment["source_surface"] == GMAIL_SUBJECT

    exact = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase=GMAIL_SUBJECT,
    )
    forged = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase=DRIVE_FILE_NAME,
    )
    assert exact.source_identity_binding is not None
    assert exact.source_identity_binding.canonical_ref["id"] == str(
        resource_id
    )
    assert forged.source_identity_binding is None


async def test_missing_binding_cross_source_and_foreign_tenant_are_inert(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    other_tenant = uuid7()
    await gateway_pool.execute(
        "INSERT INTO tenants (id) VALUES ($1)",
        other_tenant,
    )
    await _seed_binding(
        gateway_pool,
        tenant_id=other_tenant,
        source_system="google_drive",
        native_id=DRIVE_NATIVE_ID,
        identity=DRIVE_FILE_NAME,
    )
    await _seed_binding(
        gateway_pool,
        tenant_id=tenant_id,
        source_system="gmail",
        native_id=(
            f"gmail:{GMAIL_INSTALLATION_ID}:thread:{DRIVE_FILE_ID}"
        ),
        identity=DRIVE_FILE_NAME,
    )

    drive = await ingest(
        "google_drive:file",
        _drive_payload("file"),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    gmail = await ingest(
        "gmail:",
        _gmail_payload(message_id="missing-binding@example.com"),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )

    assert await gateway_pool.fetchval(
        """
        SELECT count(*)
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1
          AND observation_id=ANY($2::uuid[])
        """,
        tenant_id,
        [drive.observation.id, gmail.observation.id],
    ) == 0


async def test_missing_ids_names_and_subjects_create_no_authority(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    await _seed_binding(
        gateway_pool,
        tenant_id=tenant_id,
        source_system="google_drive",
        native_id=DRIVE_NATIVE_ID,
        identity=DRIVE_FILE_NAME,
    )
    await _seed_binding(
        gateway_pool,
        tenant_id=tenant_id,
        source_system="gmail",
        native_id=GMAIL_NATIVE_ID,
        identity=GMAIL_SUBJECT,
    )

    unnamed_drive = await ingest(
        "google_drive:file",
        {
            **_drive_payload("file"),
            "name": "",
            "version": "8",
        },
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    subjectless_gmail = await ingest(
        "gmail:",
        _gmail_payload(
            message_id="subjectless@example.com",
            subject="",
        ),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    threadless_gmail = await ingest(
        "gmail:",
        _gmail_payload(
            message_id="threadless@example.com",
            thread_id=None,
        ),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )

    assert await gateway_pool.fetchval(
        """
        SELECT count(*)
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1
          AND observation_id=ANY($2::uuid[])
        """,
        tenant_id,
        [
            unnamed_drive.observation.id,
            subjectless_gmail.observation.id,
            threadless_gmail.observation.id,
        ],
    ) == 0

    before = await gateway_pool.fetchval(
        """
        SELECT count(*) FROM observations WHERE tenant_id=$1
        """,
        tenant_id,
    )
    with pytest.raises(Exception, match="missing id"):
        await ingest(
            "google_drive:file",
            {
                "name": DRIVE_FILE_NAME,
                "version": "9",
                "modifiedTime": EVENT_TIME.isoformat(),
            },
            pool=gateway_pool,
            tenant_id=tenant_id,
            embedder=_DeterministicEmbedder(),
            enqueue_trigger=False,
        )
    assert await gateway_pool.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id=$1",
        tenant_id,
    ) == before
