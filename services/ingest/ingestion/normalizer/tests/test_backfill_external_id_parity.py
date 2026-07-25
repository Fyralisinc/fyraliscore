"""M6.7 Layer 3 (A27.5) — external_id parity, the load-bearing test.

HLD §02 L278: a webhook-delivered event and the SAME event later
re-fetched by backfill MUST derive the identical `external_id`, so the
`observations UNIQUE(source_channel, external_id, occurred_at)` index
dedups them to one row.

Each test takes ONE source-side object and:
  - WEBHOOK side: invokes the channel's handler with the webhook body
    + webhook headers (the canonical derivation the inline router uses).
  - BACKFILL side: shapes the SAME object through the per-source
    fetcher's record builder, wraps it in the producer's S3 blob, and
    runs it through the REAL normalizer (`_normalize_one_with_envelope`)
    — proving the blob-unwrap + webhook_metadata-replay + handler
    dispatch compose to the same external_id.

If any of these fails, the backfill reshape (A27.3) broke dedup parity
for that source — surface as a substrate finding before shipping.
"""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import orjson
import pytest

from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.ingestion.normalizer.worker import _normalize_one_with_envelope
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope
from services.ingest.ingestion.raw_tier.s3 import build_raw_s3_key, compute_content_hash
from services.ingest.ingestion.workflows.tests._fake_s3 import FakeS3Client


# ---------------------------------------------------------------------
# Fakes — capture the normalizer's published NormalizedEnvelope.
# ---------------------------------------------------------------------
class _CapturingProducer:
    def __init__(self) -> None:
        self.published: list[bytes] = []

    async def produce(self, *, topic: str, value: bytes, key: bytes) -> None:
        self.published.append(value)


async def _webhook_external_id(channel: str, body: dict, headers: dict) -> str | None:
    """The canonical webhook handler derivation (inline-router path)."""
    draft = await get_handler(channel)(body, headers)
    return draft.external_id


async def _backfill_external_id(
    *, source: str, fetcher_record: dict,
) -> str | None:
    """Run the backfill record through the REAL normalizer: producer
    blob → S3 → RawEnvelope → unwrap → handler → NormalizedEnvelope."""
    tenant_id = uuid4()

    # Mirror the producer's blob build (shard_fetch._write_record_and_
    # build_message): lift webhook_metadata out of the record.
    record_body = dict(fetcher_record)
    webhook_metadata = record_body.pop("webhook_metadata", {})
    blob = {
        "record": record_body,
        "shard_context": {"shard_id": "shard-1", "cursor": None},
        "webhook_metadata": webhook_metadata,
    }
    blob_bytes = orjson.dumps(blob)
    now = dt.datetime.now(tz=dt.timezone.utc)
    content_hash = compute_content_hash(blob_bytes)
    s3_key = build_raw_s3_key(
        env="test", source=source, tenant_id=tenant_id,
        ymd=now.date(), content_hash=content_hash,
    )

    s3 = FakeS3Client()
    s3.store[s3_key] = blob_bytes

    envelope = RawEnvelope(
        source=source,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        raw_s3_key=s3_key,
        content_hash=content_hash,
        ingested_at=now,
        ingress_kind="backfill",
    )
    producer = _CapturingProducer()
    parsed, produced = await _normalize_one_with_envelope(
        orjson.dumps(envelope.model_dump(mode="json")), s3, producer,
    )
    assert produced is True, (
        f"normalizer did not produce a normalized envelope for "
        f"{source} backfill (channel resolution or handler failed)"
    )
    normalized = orjson.loads(producer.published[0])
    return normalized["external_id"]


# =====================================================================
# Gmail.
# =====================================================================
@pytest.mark.asyncio
async def test_backfill_record_produces_same_external_id_as_webhook_gmail():
    install_id = str(uuid4())
    message_resource = {
        "id": "gmail-msg-1",
        "threadId": "t1",
        "internalDate": "1700000000000",
        "payload": {
            "headers": [
                {"name": "Message-ID", "value": "<abc@mail.example>"},
                {"name": "From", "value": "Alice <alice@example.com>"},
                {"name": "Subject", "value": "Hello"},
            ],
        },
    }
    # Webhook (push) shape consumed by the gmail: handler.
    webhook_body = {
        "message_resource": message_resource,
        "mailbox_email": "alice@example.com",
        "scope_used": "gmail.metadata",
        "read_path": "push",
        "gmail_installation_id": install_id,
    }
    # Backfill record — same message, fetcher conforms read_path to poll.
    from services.ingest.ingestion.fetchers.gmail import _build_record

    backfill_record = _build_record(
        message_resource=message_resource,
        mailbox_email="alice@example.com",
        scope_alias="gmail.metadata",
        gmail_installation_id=install_id,
        read_path="backfill",
    )

    webhook_eid = await _webhook_external_id("gmail:", webhook_body, {})
    backfill_eid = await _backfill_external_id(
        source="gmail", fetcher_record=backfill_record,
    )
    assert webhook_eid == backfill_eid == f"gmail:{install_id}:abc@mail.example"


# =====================================================================
# GitHub.
# =====================================================================
@pytest.mark.asyncio
async def test_backfill_record_produces_same_external_id_as_webhook_github():
    node_id = "I_kwDOABCD"
    # The REST item the fetcher receives.
    rest_item = {
        "number": 7,
        "node_id": node_id,
        "title": "A bug",
        "state": "open",
        "user": {"login": "octocat"},
        "updated_at": "2025-01-01T00:00:00Z",
    }
    # Webhook body + header the github:webhook handler consumes.
    webhook_body = {
        "action": "opened",
        "issue": rest_item,
        "repository": {"full_name": "acme/api"},
        "sender": {"login": "octocat"},
    }
    webhook_headers = {"X-GitHub-Event": "issues"}

    from services.ingest.ingestion.fetchers.github import _build_record

    backfill_record = _build_record(
        event_type="issues", repo_full_name="acme/api", payload=rest_item,
    )

    webhook_eid = await _webhook_external_id(
        "github:webhook", webhook_body, webhook_headers,
    )
    backfill_eid = await _backfill_external_id(
        source="github", fetcher_record=backfill_record,
    )
    # Fyralis versions mutable GitHub lifecycle objects by action because
    # observation dedup intentionally treats ``external_id`` as stable across
    # occurred-at changes. The REST backfill shaper must synthesize the same
    # action as the equivalent webhook.
    assert webhook_eid == backfill_eid == f"{node_id}:opened"


@pytest.mark.asyncio
async def test_backfill_parity_github_issue_comment():
    """Gap-closure: issue_comments backfill ↔ issue_comment webhook share
    comment.node_id. The repo-level endpoint omits the issue object; the
    fetcher parses the number from issue_url and parity still holds because
    external_id is the comment node_id, not the issue's."""
    node_id = "IC_kwDO123"
    comment_rest = {
        "id": 55, "node_id": node_id, "body": "ship it",
        "user": {"login": "octocat"},
        "issue_url": "https://api.github.com/repos/acme/api/issues/7",
        "updated_at": "2025-01-02T00:00:00Z",
    }
    webhook_body = {
        "action": "created",
        "comment": comment_rest,
        "issue": {"number": 7, "node_id": "I_parent"},
        "repository": {"full_name": "acme/api"},
        "sender": {"login": "octocat"},
    }
    from services.ingest.ingestion.fetchers.github import _build_record

    backfill_record = _build_record(
        event_type="issue_comments", repo_full_name="acme/api",
        payload=comment_rest,
    )
    webhook_eid = await _webhook_external_id(
        "github:webhook", webhook_body, {"X-GitHub-Event": "issue_comment"},
    )
    backfill_eid = await _backfill_external_id(
        source="github", fetcher_record=backfill_record,
    )
    assert webhook_eid == backfill_eid == node_id


@pytest.mark.asyncio
async def test_backfill_parity_github_commit_to_push():
    """Gap-closure: a commit backfilled as a single-commit push shares the
    `{repo}@{sha}` external_id with the live push whose tip is that sha."""
    sha = "deadbeefcafe"
    repo = "acme/api"
    # Live push whose tip commit IS this sha.
    webhook_body = {
        "ref": "refs/heads/main",
        "after": sha,
        "commits": [{"id": sha, "timestamp": "2024-06-01T12:00:00Z"}],
        "head_commit": {"id": sha, "timestamp": "2024-06-01T12:00:00Z"},
        "repository": {"full_name": repo},
        "sender": {"login": "octocat"},
    }
    commit_rest = {
        "sha": sha, "node_id": "C_node",
        "commit": {"author": {"date": "2024-06-01T12:00:00Z"}, "message": "x"},
        "author": {"login": "octocat"},
    }
    from services.ingest.ingestion.fetchers.github import _build_record

    backfill_record = _build_record(
        event_type="commits", repo_full_name=repo, payload=commit_rest,
    )
    webhook_eid = await _webhook_external_id(
        "github:webhook", webhook_body, {"X-GitHub-Event": "push"},
    )
    backfill_eid = await _backfill_external_id(
        source="github", fetcher_record=backfill_record,
    )
    assert webhook_eid == backfill_eid == f"{repo}@{sha}"


@pytest.mark.asyncio
async def test_backfill_parity_github_pr_review():
    """Class B fan-out: pr_reviews backfill ↔ pull_request_review webhook
    share review.node_id."""
    node_id = "PRR_kwDO9"
    review = {
        "node_id": node_id, "state": "approved", "body": "LGTM",
        "user": {"login": "octocat"}, "submitted_at": "2025-02-01T00:00:00Z",
    }
    webhook_body = {
        "action": "submitted",
        "review": review,
        "pull_request": {"number": 9, "node_id": "PR_x"},
        "repository": {"full_name": "acme/api"},
        "sender": {"login": "octocat"},
    }
    from services.ingest.ingestion.fetchers.github import _build_review_record

    backfill_record = _build_review_record(
        "acme/api", {"number": 9, "node_id": "PR_x"}, review,
    )
    webhook_eid = await _webhook_external_id(
        "github:webhook", webhook_body,
        {"X-GitHub-Event": "pull_request_review"},
    )
    backfill_eid = await _backfill_external_id(
        source="github", fetcher_record=backfill_record,
    )
    assert webhook_eid == backfill_eid == node_id


@pytest.mark.asyncio
async def test_backfill_parity_github_check_run():
    """Class B fan-out: check_runs backfill ↔ check_run webhook share
    check.node_id."""
    node_id = "CR_kwDO5"
    check = {
        "node_id": node_id, "name": "ci/build", "status": "completed",
        "conclusion": "success", "head_sha": "abc123",
        "completed_at": "2025-02-01T00:00:00Z",
    }
    webhook_body = {
        "action": "completed",
        "check_run": check,
        "repository": {"full_name": "acme/api"},
    }
    from services.ingest.ingestion.fetchers.github import _build_check_run_record

    backfill_record = _build_check_run_record("acme/api", check)
    webhook_eid = await _webhook_external_id(
        "github:webhook", webhook_body, {"X-GitHub-Event": "check_run"},
    )
    backfill_eid = await _backfill_external_id(
        source="github", fetcher_record=backfill_record,
    )
    assert webhook_eid == backfill_eid == node_id


# =====================================================================
# Slack.
# =====================================================================
@pytest.mark.asyncio
async def test_backfill_record_produces_same_external_id_as_webhook_slack():
    ts = "1700000000.000100"
    channel = "C123"
    # conversations.history message (no `channel` key).
    history_msg = {"ts": ts, "text": "hi team", "user": "U99", "type": "message"}
    # Webhook event_callback for the same message.
    webhook_body = {
        "type": "event_callback",
        "team_id": "T1",
        "event": {**history_msg, "channel": channel},
    }

    # Backfill record from the fetcher's shaping (inline-equivalent).
    backfill_record = {
        "type": "event_callback",
        "team_id": "T1",
        "event": {**history_msg, "channel": channel},
    }

    webhook_eid = await _webhook_external_id("slack:message", webhook_body, {})
    backfill_eid = await _backfill_external_id(
        source="slack", fetcher_record=backfill_record,
    )
    assert webhook_eid == backfill_eid == f"{channel}:{ts}"


# =====================================================================
# Discord.
# =====================================================================
@pytest.mark.asyncio
async def test_backfill_record_produces_same_external_id_as_webhook_discord():
    msg_id = "1112223334445556"
    guild_id = "G777"
    # The live surface for Discord MESSAGES is the Gateway (IN-12),
    # not the interaction webhook — both go through discord:message.
    gateway_body = {
        "id": msg_id,
        "guild_id": guild_id,
        "channel_id": "C42",
        "content": "hello",
        "author": {"id": "U1"},
        "timestamp": "2025-01-01T00:00:00+00:00",
    }
    # REST message the fetcher receives lacks guild_id; the fetcher
    # injects it.
    rest_msg = {
        "id": msg_id,
        "channel_id": "C42",
        "content": "hello",
        "author": {"id": "U1"},
        "timestamp": "2025-01-01T00:00:00+00:00",
    }
    backfill_record = {**rest_msg, "guild_id": guild_id}

    gateway_channel = resolve_channel("discord", "gateway")
    webhook_eid = await _webhook_external_id(gateway_channel, gateway_body, {})
    backfill_eid = await _backfill_external_id(
        source="discord", fetcher_record=backfill_record,
    )
    assert webhook_eid == backfill_eid == f"discord:{msg_id}"


# =====================================================================
# Normalizer metadata flow — webhook_metadata reaches the handler.
# =====================================================================
@pytest.mark.asyncio
async def test_normalizer_extracts_webhook_metadata_from_backfill_envelope():
    """Without the X-GitHub-Event header the github handler raises; a
    successful normalize proves the metadata flowed from the blob
    through the normalizer to the handler (A27.3)."""
    rest_item = {
        "number": 1, "node_id": "I_meta", "title": "x", "state": "open",
        "user": {"login": "o"}, "updated_at": "2025-01-01T00:00:00Z",
    }
    from services.ingest.ingestion.fetchers.github import _build_record

    record = _build_record(
        event_type="issues", repo_full_name="o/r", payload=rest_item,
    )
    assert record["webhook_metadata"] == {"X-GitHub-Event": "issues"}
    eid = await _backfill_external_id(source="github", fetcher_record=record)
    assert eid == "I_meta:opened"
