"""Tests for services/ingest/ingestion/handlers/google_drive.py (IN-16)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.google_drive import handle_google_drive_file


pytestmark = pytest.mark.asyncio


def _file(**over):
    base = {
        "id": "file-1",
        "name": "Q3 Planning",
        "mimeType": "application/vnd.google-apps.document",
        "version": "7",
        "trashed": False,
        "createdTime": "2026-04-01T09:00:00.000Z",
        "modifiedTime": "2026-04-20T10:00:00.000Z",
        "webViewLink": "https://docs.google.com/document/d/file-1",
        "owners": [{"emailAddress": "alice@acme.com", "displayName": "Alice"}],
        "lastModifyingUser": {"emailAddress": "bob@acme.com"},
        "permissions": [
            {"emailAddress": "alice@acme.com", "role": "owner", "type": "user"},
            {"emailAddress": "investor@vc.com", "role": "reader", "type": "user"},
        ],
        "shared": True,
        "_fyralis_drive_id": "my-drive",
        "_fyralis_drive_kind": "my_drive",
        "_fyralis_owner_email": "alice@acme.com",
        "_fyralis_removed": False,
        "_fyralis_extracted_text": "Roadmap: ship Atlas, plan Helios.",
    }
    base.update(over)
    return base


async def test_channel_registered_and_authoritative():
    assert get_handler("google_drive:file") is not None
    assert CHANNEL_TRUST_MAP["google_drive:file"] == "authoritative"


async def test_edit_is_signal_with_versioned_external_id_and_content():
    draft = await handle_google_drive_file(_file(), {})
    assert draft.source_channel == "google_drive:file"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    # external_id is versioned on Drive's monotonic `version`.
    assert draft.external_id == "gdrive:file-1:7"
    assert draft.content["file_id"] == "file-1"
    assert draft.content["object_type"] == "file"
    assert draft.content["name"] == "Q3 Planning"
    assert draft.content["has_extracted_text"] is True
    assert draft.content["primary_owner_email"] == "alice@acme.com"
    assert draft.content["last_modifying_user"] == "bob@acme.com"
    # Extracted text is embedded in content_text after the activity line.
    assert "Roadmap: ship Atlas" in draft.content_text
    assert draft.source_actor_ref == "email:bob@acme.com"
    assert draft.occurred_at.isoformat().startswith("2026-04-20T10:00:00")


async def test_version_bump_changes_external_id():
    a = await handle_google_drive_file(_file(version="7"), {})
    b = await handle_google_drive_file(_file(version="8"), {})
    assert a.external_id != b.external_id
    assert b.external_id == "gdrive:file-1:8"


async def test_identical_refetch_dedups():
    a = await handle_google_drive_file(_file(), {})
    b = await handle_google_drive_file(_file(), {})
    assert a.external_id == b.external_id


async def test_trashed_is_state_change():
    draft = await handle_google_drive_file(_file(trashed=True), {})
    assert draft.kind == "state_change"
    assert draft.content["removed"] is True
    assert "was removed" in draft.content_text


async def test_removed_change_without_version():
    draft = await handle_google_drive_file(
        {
            "id": "file-9",
            "_fyralis_removed": True,
            "_fyralis_change_time": "2026-04-21T08:00:00.000Z",
        },
        {},
    )
    assert draft.kind == "state_change"
    assert draft.external_id == "gdrive:file-9:removed:2026-04-21T08:00:00.000Z"
    assert draft.occurred_at.isoformat().startswith("2026-04-21T08:00:00")


async def test_external_sharing_recipient_flagged():
    draft = await handle_google_drive_file(_file(), {})
    by_id = {e["id"]: e for e in draft.entities_hint}
    assert by_id["investor@vc.com"]["role"] == "shared_with"
    assert by_id["investor@vc.com"]["external"] is True
    assert by_id["alice@acme.com"]["external"] is False
    assert by_id["bob@acme.com"]["role"] == "editor"
    # The document itself is an entity.
    assert any(e["type"] == "document" and e["id"] == "file-1" for e in draft.entities_hint)


async def test_pdf_file_extracts_metadata_only_label():
    draft = await handle_google_drive_file(
        _file(mimeType="application/pdf", name="contract.pdf",
              _fyralis_extracted_text="This agreement is between..."), {})
    assert draft.kind == "signal"
    assert draft.content["mime_type"] == "application/pdf"
    assert "This agreement is between" in draft.content_text
    assert draft.content["has_extracted_text"] is True


# --- comment records ---------------------------------------------------------

def _comment(**over):
    base = {
        "_fyralis_record_type": "comment",
        "_fyralis_file_id": "file-1",
        "_fyralis_file_name": "Q3 Planning",
        "_fyralis_owner_email": "alice@acme.com",
        "_fyralis_drive_id": "my-drive",
        "id": "cmt-1",
        "content": "Can we cut scope here?",
        "author": {"displayName": "Bob", "emailAddress": "bob@acme.com"},
        "createdTime": "2026-04-21T08:00:00.000Z",
        "modifiedTime": "2026-04-21T08:30:00.000Z",
        "resolved": False,
        "quotedFileContent": {"value": "Phase 2 deliverables"},
        "replies": [
            {"id": "r1", "content": "agreed",
             "author": {"displayName": "Alice", "emailAddress": "alice@acme.com"},
             "createdTime": "2026-04-21T08:30:00.000Z"},
        ],
    }
    base.update(over)
    return base


async def test_comment_is_signal_with_replies_and_versioned_id():
    draft = await handle_google_drive_file(_comment(), {})
    assert draft.source_channel == "google_drive:file"  # shared routing channel
    assert draft.content["object_type"] == "comment"
    assert draft.kind == "signal"
    # versioned on modifiedTime so edits / new replies land fresh observations.
    assert draft.external_id == "gdrive-comment:file-1:cmt-1:2026-04-21T08:30:00.000Z"
    assert draft.content["reply_count"] == 1
    assert "Can we cut scope" in draft.content_text
    assert "agreed" in draft.content_text  # reply rendered
    assert draft.source_actor_ref == "email:bob@acme.com"
    # commenter + replier + the document are entities.
    roles = {e["id"]: e.get("role") for e in draft.entities_hint}
    assert roles.get("bob@acme.com") == "commenter"
    assert roles.get("alice@acme.com") == "commenter"
    assert any(e["type"] == "document" and e["id"] == "file-1" for e in draft.entities_hint)


async def test_resolved_comment_is_state_change():
    draft = await handle_google_drive_file(_comment(resolved=True), {})
    assert draft.kind == "state_change"
    assert draft.content["resolved"] is True
    assert "resolved a comment" in draft.content_text


async def test_comment_without_author_email_falls_back_to_name():
    draft = await handle_google_drive_file(
        _comment(author={"displayName": "Anon"}, replies=[]), {})
    assert "Anon commented on" in draft.content_text
    assert draft.source_actor_ref is None  # no email to anchor an actor ref


# --- revision records --------------------------------------------------------

def _revision(**over):
    base = {
        "_fyralis_record_type": "revision",
        "_fyralis_file_id": "file-1",
        "_fyralis_file_name": "Q3 Planning",
        "_fyralis_owner_email": "alice@acme.com",
        "id": "rev-9",
        "modifiedTime": "2026-04-20T10:00:00.000Z",
        "lastModifyingUser": {"emailAddress": "bob@acme.com", "displayName": "Bob"},
        "size": "12345",
        "keepForever": True,
    }
    base.update(over)
    return base


async def test_revision_is_signal_with_stable_id():
    draft = await handle_google_drive_file(_revision(), {})
    assert draft.content["object_type"] == "revision"
    assert draft.kind == "signal"
    assert draft.external_id == "gdrive-revision:file-1:rev-9"
    assert draft.content["last_modifying_user"] == "bob@acme.com"
    assert draft.content["keep_forever"] is True
    assert "saved a revision of 'Q3 Planning'" in draft.content_text
    assert draft.source_actor_ref == "email:bob@acme.com"
    assert draft.occurred_at.isoformat().startswith("2026-04-20T10:00:00")


async def test_comment_missing_file_id_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_google_drive_file(
            {"_fyralis_record_type": "comment", "id": "cmt-1"}, {})


async def test_missing_id_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_google_drive_file({"name": "no id"}, {})
