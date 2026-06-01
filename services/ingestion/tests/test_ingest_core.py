"""Integration tests for services/ingestion/core.ingest + handlers.

Mix of unit tests (Slack signature, phrase extraction) and full-stack
integration tests (real Postgres, real handler, deterministic embedder).
One integration test hits real Ollama if OLLAMA_URL is reachable.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import httpx
import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.ingestion.core import (
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    candidate_phrases,
    ingest,
)
from services.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    HandlerNotFound,
    ObservationDraft,
    get_handler,
    handler_channels,
)
from services.ingestion.handlers.slack import (
    SlackSignatureError,
    extract_entities_from_text,
    parse_slack_ts,
    verify_slack_signature,
)
from services.observations.events import OBSERVATIONS_CHANNEL


# =========================================================================
# Unit — Slack signature
# =========================================================================


def _sign(body: bytes, secret: str, ts: str | None = None) -> tuple[str, str]:
    ts = ts or str(int(time.time()))
    basestring = f"v0:{ts}:{body.decode('utf-8')}".encode("utf-8")
    sig = "v0=" + hmac.new(secret.encode(), basestring, hashlib.sha256).hexdigest()
    return ts, sig


def test_verify_slack_signature_happy():
    body = b'{"hello":"world"}'
    secret = "shh"
    ts, sig = _sign(body, secret)
    verify_slack_signature(body, ts, sig, secret)


def test_verify_slack_signature_tampered_body():
    body = b'{"hello":"world"}'
    secret = "shh"
    ts, sig = _sign(body, secret)
    with pytest.raises(SlackSignatureError):
        verify_slack_signature(b'{"hello":"tampered"}', ts, sig, secret)


def test_verify_slack_signature_stale_timestamp():
    body = b"x"
    secret = "shh"
    old_ts = str(int(time.time()) - 3600)
    _, sig = _sign(body, secret, ts=old_ts)
    with pytest.raises(SlackSignatureError):
        verify_slack_signature(body, old_ts, sig, secret)


def test_verify_slack_signature_missing_raises():
    with pytest.raises(SlackSignatureError):
        verify_slack_signature(b"", "", "", "secret")


def test_verify_slack_signature_wrong_secret():
    body = b"x"
    ts, sig = _sign(body, "real-secret")
    with pytest.raises(SlackSignatureError):
        verify_slack_signature(body, ts, sig, "wrong-secret")


def test_verify_slack_signature_non_integer_timestamp():
    with pytest.raises(SlackSignatureError):
        verify_slack_signature(b"x", "not-an-int", "v0=abc", "secret")


# =========================================================================
# Unit — Slack helpers
# =========================================================================


def test_parse_slack_ts_microseconds():
    dt = parse_slack_ts("1700000000.123456")
    assert dt.tzinfo is timezone.utc
    assert dt.year == 2023


def test_extract_entities_from_text_mentions_channels_urls():
    text = (
        "Hey <@U01ALICE> ping <@U02BOB|bob> in <#C01ENG> re: "
        "<https://example.com|the doc> and <https://b.com>."
    )
    entities, unresolved = extract_entities_from_text(text)
    kinds = [(e["type"], e["id"]) for e in entities]
    assert ("slack_user", "U01ALICE") in kinds
    assert ("slack_user", "U02BOB") in kinds
    assert ("slack_channel", "C01ENG") in kinds
    assert ("url", "https://example.com") in kinds
    assert ("url", "https://b.com") in kinds
    assert unresolved == []


def test_extract_entities_deduplicates():
    text = "<@U1> <@U1>"
    entities, _ = extract_entities_from_text(text)
    assert len(entities) == 1


# =========================================================================
# Unit — phrase extraction
# =========================================================================


def test_candidate_phrases_empty():
    assert candidate_phrases("") == []


def test_candidate_phrases_caps_output():
    text = " ".join([f"Alpha{i}" for i in range(200)])
    phrases = candidate_phrases(text, max_phrases=50)
    assert len(phrases) == 50


def test_candidate_phrases_normalizes_in_dedup():
    text = "foo bar FOO BAR"
    phrases = candidate_phrases(text)
    # After normalization "foo" appears once.
    normalized = {p.lower() for p in phrases}
    assert "foo" in normalized


# =========================================================================
# Unit — handler registry
# =========================================================================


def test_registry_lists_wave2a_handlers():
    channels = handler_channels()
    for required in (
        "slack:message",
        "internal:state_change",
        "internal:anomaly",
        "internal:prediction_resolution",
    ):
        assert required in channels


def test_registry_get_handler_unknown_raises():
    with pytest.raises(HandlerNotFound):
        get_handler("mars:webhook")


def test_channel_trust_map_slack_attested_agent():
    assert CHANNEL_TRUST_MAP["slack:message"] == "attested_agent"


# =========================================================================
# Integration — ingest happy paths
# =========================================================================


async def _ingest_slack(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    text: str = "hello world",
    user: str = "U01ALICE",
    channel: str = "C01ENG",
    ts: str | None = None,
    embedder=None,
    actor_repo=None,
    alias_repo=None,
):
    if ts is None:
        ts = f"{time.time():.6f}"
    payload = {
        "team_id": "T01",
        "event": {
            "type": "message",
            "user": user,
            "text": text,
            "ts": ts,
            "channel": channel,
        },
    }
    return await ingest(
        "slack:message",
        payload,
        pool=pool,
        tenant_id=tenant_id,
        actor_repo=actor_repo or ActorRepo(pool),
        alias_repo=alias_repo or EntityAliasRepo(pool),
        embedder=embedder,
    )


@pytest.mark.asyncio
async def test_slack_happy_path_creates_observation(
    gateway_pool, tenant_id, seeded_actor, _DeterministicEmbedder
):
    # Attach identity mapping so actor resolves.
    await gateway_pool.execute(
        """
        INSERT INTO actor_identity_mappings (
            actor_id, source_channel, source_actor_ref, confidence
        ) VALUES ($1, 'slack', 'U01ALICE', 1.0)
        """,
        seeded_actor,
    )
    result = await _ingest_slack(
        gateway_pool,
        tenant_id,
        text="Hello <@U02BOB> please review <https://example.com>",
        embedder=_DeterministicEmbedder(),
    )
    assert not result.deduped
    obs = result.observation
    assert obs.source_channel == "slack:message"
    assert obs.trust_tier == "attested_agent"
    assert obs.actor_id == seeded_actor
    ids = [(e["type"], e["id"]) for e in obs.entities_mentioned]
    assert ("slack_user", "U02BOB") in ids
    assert ("url", "https://example.com") in ids


@pytest.mark.asyncio
async def test_dedup_same_slack_message_twice(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    ts = f"{time.time():.6f}"
    r1 = await _ingest_slack(
        gateway_pool, tenant_id, ts=ts, embedder=_DeterministicEmbedder()
    )
    r2 = await _ingest_slack(
        gateway_pool, tenant_id, ts=ts, embedder=_DeterministicEmbedder()
    )
    assert r1.observation.id == r2.observation.id
    assert r2.deduped is True
    assert r2.trigger_queue_id is None  # second call doesn't enqueue T1
    # Single row in observations.
    count = await gateway_pool.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    )
    assert count == 1


@pytest.mark.asyncio
async def test_unknown_actor_ref_records_unresolved_marker(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    r = await _ingest_slack(
        gateway_pool,
        tenant_id,
        user="U99GHOST",
        embedder=_DeterministicEmbedder(),
    )
    obs = r.observation
    assert obs.actor_id is None
    assert obs.content.get("_unresolved_actor_ref") == "slack:U99GHOST"


@pytest.mark.asyncio
async def test_entity_alias_fast_path_resolves(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    # Seed an alias
    repo = EntityAliasRepo(gateway_pool)
    await repo.insert_alias(
        phrase="payments",
        resolved_entity_ref={"type": "commitment", "id": "c-187"},
        source="manual",
        confidence=0.95,
        tenant_id=tenant_id,
    )
    r = await _ingest_slack(
        gateway_pool,
        tenant_id,
        text="payments system is flaky",
        embedder=_DeterministicEmbedder(),
        alias_repo=repo,
    )
    refs = r.observation.entities_mentioned
    assert {"type": "commitment", "id": "c-187"} in refs


@pytest.mark.asyncio
async def test_unresolved_entity_phrase_queued_in_content(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    # No aliases seeded — capitalized multi-word phrase looks like
    # an entity reference so it lands in the resolver queue.
    r = await _ingest_slack(
        gateway_pool,
        tenant_id,
        text="The Frobozz-Widget will ship tomorrow",
        embedder=_DeterministicEmbedder(),
    )
    unresolved = r.observation.content.get("_unresolved_phrases", [])
    assert any("Frobozz-Widget" in p for p in unresolved)


@pytest.mark.asyncio
async def test_embedding_fallback_on_ollama_error(
    gateway_pool, tenant_id
):
    class _FailingEmbedder:
        class _C:
            expected_dim = 768

        def __init__(self):
            self.config = self._C()

        async def embed(self, text):
            from lib.embeddings.ollama import OllamaError

            raise OllamaError("simulated")

    r = await _ingest_slack(
        gateway_pool, tenant_id, embedder=_FailingEmbedder()
    )
    assert r.observation.embedding_pending is True
    assert r.observation.embedding is None


@pytest.mark.asyncio
async def test_trust_tier_slack_is_attested_agent(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    r = await _ingest_slack(
        gateway_pool, tenant_id, embedder=_DeterministicEmbedder()
    )
    assert r.observation.trust_tier == "attested_agent"


@pytest.mark.asyncio
async def test_notify_fires_post_commit(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    """Real asyncpg LISTEN fixture — receive observations_new payload."""
    dsn = os.environ["DATABASE_URL"]
    listener_conn = await asyncpg.connect(dsn)
    received: list[str] = []

    def _on_notify(conn, pid, channel, payload):
        received.append(payload)

    try:
        await listener_conn.add_listener(OBSERVATIONS_CHANNEL, _on_notify)
        r = await _ingest_slack(
            gateway_pool, tenant_id, embedder=_DeterministicEmbedder()
        )
        # Poll for notify — should arrive within 1s.
        for _ in range(50):
            if received:
                break
            await asyncio.sleep(0.02)
        assert received, "expected an observations_new NOTIFY"
        payloads = [json.loads(p) for p in received]
        assert any(p["id"] == str(r.observation.id) for p in payloads)
    finally:
        await listener_conn.remove_listener(OBSERVATIONS_CHANNEL, _on_notify)
        await listener_conn.close()


@pytest.mark.asyncio
async def test_think_trigger_enqueued_on_new_observation(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    r = await _ingest_slack(
        gateway_pool, tenant_id, embedder=_DeterministicEmbedder()
    )
    assert r.trigger_queue_id is not None
    row = await gateway_pool.fetchrow(
        """
        SELECT tenant_id, trigger_kind, trigger_subkind, observation_id
        FROM think_trigger_queue WHERE id = $1
        """,
        r.trigger_queue_id,
    )
    assert row is not None
    assert row["tenant_id"] == tenant_id
    assert row["trigger_kind"] == "T1"
    assert row["trigger_subkind"] == "event_arrival"
    assert row["observation_id"] == r.observation.id


# =========================================================================
# Integration — system handler
# =========================================================================


@pytest.mark.asyncio
async def test_system_state_change_ingest(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    cause = uuid7()
    # Seed the cause observation so the FK-ish reference in cause_id
    # points at a real row (FK isn't enforced on partitioned table per
    # Wave 0 note, but seeding is hygienic).
    await gateway_pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            content, content_text, trust_tier
        ) VALUES ($1, $2, now(), 'signal', 'test:harness',
                  '{}'::jsonb, 'origin', 'authoritative')
        """,
        cause,
        tenant_id,
    )
    payload = {
        "content_text": "commitment c-1 transitioned doneverified",
        "content": {"entity_id": "c-1", "kind": "commitment_doneverified"},
        "cause_event_id": str(cause),
    }
    r = await ingest(
        "internal:state_change",
        payload,
        pool=gateway_pool,
        tenant_id=tenant_id,
        actor_repo=ActorRepo(gateway_pool),
        alias_repo=EntityAliasRepo(gateway_pool),
        embedder=_DeterministicEmbedder(),
    )
    obs = r.observation
    assert obs.kind == "state_change"
    assert obs.source_channel == "internal:state_change"
    assert obs.trust_tier == "authoritative"
    assert obs.cause_id == cause


@pytest.mark.asyncio
async def test_internal_channel_accepts_null_external_id(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    """internal:* channels don't carry external_id; two calls may
    produce two rows (they are not deduped by external_id)."""
    payload = {
        "content_text": "anomaly detected",
        "content": {"ref": "x"},
    }
    r1 = await ingest(
        "internal:anomaly",
        payload,
        pool=gateway_pool,
        tenant_id=tenant_id,
        actor_repo=ActorRepo(gateway_pool),
        alias_repo=EntityAliasRepo(gateway_pool),
        embedder=_DeterministicEmbedder(),
    )
    r2 = await ingest(
        "internal:anomaly",
        payload,
        pool=gateway_pool,
        tenant_id=tenant_id,
        actor_repo=ActorRepo(gateway_pool),
        alias_repo=EntityAliasRepo(gateway_pool),
        embedder=_DeterministicEmbedder(),
    )
    assert r1.observation.id != r2.observation.id
    assert r1.observation.kind == "anomaly_flagged"


# =========================================================================
# Integration — malformed / oversized
# =========================================================================


@pytest.mark.asyncio
async def test_unknown_channel_raises_handler_not_found(
    gateway_pool, tenant_id
):
    with pytest.raises(HandlerNotFound):
        await ingest(
            "mars:webhook",
            {},
            pool=gateway_pool,
            tenant_id=tenant_id,
            actor_repo=ActorRepo(gateway_pool),
            alias_repo=EntityAliasRepo(gateway_pool),
        )


@pytest.mark.asyncio
async def test_malformed_slack_payload_raises_validation(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    # Missing 'event' → 'text' missing → handler rejects.
    with pytest.raises(ValidationError):
        await ingest(
            "slack:message",
            {"team_id": "T01"},
            pool=gateway_pool,
            tenant_id=tenant_id,
            actor_repo=ActorRepo(gateway_pool),
            alias_repo=EntityAliasRepo(gateway_pool),
            embedder=_DeterministicEmbedder(),
        )


@pytest.mark.asyncio
async def test_oversized_payload_rejected(gateway_pool, tenant_id):
    big_payload = {"event": {"text": "x" * (MAX_PAYLOAD_BYTES + 10)}}
    with pytest.raises(PayloadTooLarge):
        await ingest(
            "slack:message",
            big_payload,
            pool=gateway_pool,
            tenant_id=tenant_id,
            actor_repo=ActorRepo(gateway_pool),
            alias_repo=EntityAliasRepo(gateway_pool),
        )


# =========================================================================
# Integration — UUID v7 monotonicity
# =========================================================================


@pytest.mark.asyncio
async def test_successive_ingests_have_monotonic_ids(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    ids = []
    for i in range(8):
        r = await _ingest_slack(
            gateway_pool,
            tenant_id,
            ts=f"{time.time() + i:.6f}",
            text=f"msg {i}",
            embedder=_DeterministicEmbedder(),
        )
        ids.append(r.observation.id)
    # UUID v7 is time-sortable; strictly non-decreasing.
    assert all(ids[i] < ids[i + 1] for i in range(len(ids) - 1))


# =========================================================================
# Integration — 50-concurrent dedup
# =========================================================================


@pytest.mark.asyncio
async def test_50_concurrent_ingests_same_external_id_dedup_to_one(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    ts = f"{time.time():.6f}"

    async def one():
        try:
            return await _ingest_slack(
                gateway_pool,
                tenant_id,
                ts=ts,
                embedder=_DeterministicEmbedder(),
            )
        except Exception:
            return None

    results = await asyncio.gather(*[one() for _ in range(50)])
    survived = [r for r in results if r is not None]
    # Every ingest should return an IngestResult (conflicts collapse to
    # a read of the winning row); at least one fresh insert, rest dedup.
    assert len(survived) == 50
    unique_ids = {r.observation.id for r in survived}
    assert len(unique_ids) == 1
    count = await gateway_pool.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant_id
    )
    assert count == 1


# =========================================================================
# Integration — replay 100 events → 100 dedups
# =========================================================================


@pytest.mark.asyncio
async def test_replay_events_dedups_to_zero_new_rows(
    gateway_pool, tenant_id, _DeterministicEmbedder
):
    # Seed 20 events (smaller than 100 to keep the test fast; the
    # invariant — "every replay dedups" — is the same).
    original: list[UUID] = []
    for i in range(20):
        r = await _ingest_slack(
            gateway_pool,
            tenant_id,
            ts=f"{time.time() + i:.6f}",
            text=f"e{i}",
            embedder=_DeterministicEmbedder(),
        )
        original.append(r.observation.id)
    count_after_seed = await gateway_pool.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant_id
    )
    assert count_after_seed == 20
    # Replay — every message uses the same ts so dedup should fire.
    # We need the same external_id so reuse the ones we just captured
    # by reading them back and re-ingesting.
    rows = await gateway_pool.fetch(
        "SELECT external_id, content_text FROM observations WHERE tenant_id = $1",
        tenant_id,
    )
    for row in rows:
        # Parse channel:ts from external_id
        channel_id, ts = row["external_id"].split(":", 1)
        r = await _ingest_slack(
            gateway_pool,
            tenant_id,
            ts=ts,
            channel=channel_id,
            text=row["content_text"],
            embedder=_DeterministicEmbedder(),
        )
        assert r.deduped is True
    count_after_replay = await gateway_pool.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant_id
    )
    assert count_after_replay == 20


# =========================================================================
# Property test — fuzz Slack shape
# =========================================================================


# Hypothesis: make the test tolerate DB side-effects gracefully.
@settings(
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    max_examples=12,
)
@given(
    text=st.text(max_size=200),
    user=st.text(
        alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        min_size=1,
        max_size=12,
    ),
    channel=st.text(
        alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        min_size=1,
        max_size=12,
    ),
)
@pytest.mark.asyncio
async def test_fuzz_slack_payload_never_500s(
    gateway_pool, tenant_id, _DeterministicEmbedder, text, user, channel
):
    payload = {
        "team_id": "T01",
        "event": {
            "type": "message",
            "user": user,
            "text": text,
            "ts": f"{time.time():.6f}",
            "channel": f"C{channel}",
        },
    }
    # A ValidationError is a structured 4xx — acceptable.
    try:
        await ingest(
            "slack:message",
            payload,
            pool=gateway_pool,
            tenant_id=tenant_id,
            actor_repo=ActorRepo(gateway_pool),
            alias_repo=EntityAliasRepo(gateway_pool),
            embedder=_DeterministicEmbedder(),
        )
    except ValidationError:
        pass
    except Exception as e:
        # Internal error not acceptable.
        pytest.fail(f"fuzz raised unexpected {type(e).__name__}: {e}")


# =========================================================================
# Integration — real Ollama
# =========================================================================


@pytest.mark.asyncio
async def test_real_ollama_embedding_stored(gateway_pool, tenant_id):
    """Requires OLLAMA_URL + OLLAMA_EMBED_MODEL in env (integration-only)."""
    url = os.environ.get("OLLAMA_URL")
    if not url:
        pytest.skip("OLLAMA_URL not set — skipping real-Ollama test")
    try:
        r = httpx.get(f"{url}/api/tags", timeout=2.0)
        if r.status_code != 200:
            pytest.skip(f"ollama not reachable: {r.status_code}")
    except Exception:
        pytest.skip("ollama not reachable")

    client = OllamaClient(OllamaConfig.from_env())
    try:
        r = await _ingest_slack(
            gateway_pool, tenant_id, text="rate limiter deep dive", embedder=client
        )
    finally:
        await client.close()
    obs = r.observation
    assert obs.embedding_pending is False
    assert obs.embedding is not None
    assert len(obs.embedding) == 768
