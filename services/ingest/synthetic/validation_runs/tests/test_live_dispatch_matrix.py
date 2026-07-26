"""Exhaustive routing tests for the configured synthetic live generators."""

from __future__ import annotations

from collections import Counter
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from services.ingest.synthetic.live_generators import HMAC_PROVIDERS
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS
from services.ingest.synthetic.validation_runs import composition
from services.ingest.synthetic.validation_runs.composition import (
    LiveTarget,
    SigningSecrets,
    _dispatch_regular,
    dispatch_live_concurrent,
    prepare_live_drivers,
)


_CORE_SOURCES = ("gmail", "slack", "github", "discord")
_GOOGLE_SOURCES = ("google_calendar", "google_drive")
_DIRECT_SOURCES = ("telegram", "signal", "aws", "carta", "linkedin")
_META_SOURCES = ("whatsapp", "facebook_pages")
_CONFIGURED_SOURCES = (
    *_CORE_SOURCES,
    *HMAC_PROVIDERS,
    *_GOOGLE_SOURCES,
    "notion",
    *_DIRECT_SOURCES,
    *_META_SOURCES,
)


class _FakeResult:
    http_status = 202


class _FakeGenerator:
    def __init__(self, source: str, calls: list[str]) -> None:
        self._source = source
        self._calls = calls

    async def _record(self, **kwargs: Any) -> _FakeResult:
        target = kwargs.get("target")
        source = target.source if self._source == "google_push" else self._source
        self._calls.append(source)
        return _FakeResult()

    async def simulate_push(self, **kwargs: Any) -> _FakeResult:
        return await self._record(**kwargs)

    async def simulate_message(self, **kwargs: Any) -> _FakeResult:
        return await self._record(**kwargs)

    async def simulate_message_create(self, **kwargs: Any) -> _FakeResult:
        return await self._record(**kwargs)

    async def simulate_issue_event(self, **kwargs: Any) -> _FakeResult:
        return await self._record(**kwargs)

    async def simulate_event(self, **kwargs: Any) -> _FakeResult:
        return await self._record(**kwargs)


def _fake_drivers(calls: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        gmail_pubsub=_FakeGenerator("gmail", calls),
        slack_webhook=_FakeGenerator("slack", calls),
        github_webhook=_FakeGenerator("github", calls),
        discord_gateway=_FakeGenerator("discord", calls),
        hmac={
            source: _FakeGenerator(source, calls)
            for source in HMAC_PROVIDERS
        },
        google_push=_FakeGenerator("google_push", calls),
        notion_webhook=_FakeGenerator("notion", calls),
        telegram_gateway=_FakeGenerator("telegram", calls),
        signal_gateway=_FakeGenerator("signal", calls),
        aws_poll=_FakeGenerator("aws", calls),
        carta_poll=_FakeGenerator("carta", calls),
        linkedin_poll=_FakeGenerator("linkedin", calls),
        whatsapp_webhook=_FakeGenerator("whatsapp", calls),
        facebook_pages_webhook=_FakeGenerator("facebook_pages", calls),
    )


def _target(source: str) -> LiveTarget:
    return LiveTarget(
        tenant_id=uuid4(),
        source=source,
        slug=f"dispatch-{source}",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("source", _CONFIGURED_SOURCES)
async def test_regular_dispatch_invokes_exact_configured_generator(
    source: str,
) -> None:
    calls: list[str] = []

    await _dispatch_regular(
        _fake_drivers(calls),  # type: ignore[arg-type]
        _target(source),
        1,
    )

    assert calls == [source]


@pytest.mark.asyncio
async def test_concurrent_dispatch_invokes_every_configured_generator_once() -> None:
    calls: list[str] = []
    targets = [_target(source) for source in _CONFIGURED_SOURCES]

    assert len(_CONFIGURED_SOURCES) == 27
    assert set(_CONFIGURED_SOURCES) == set(CANONICAL_SOURCE_IDS)
    result = await dispatch_live_concurrent(
        _fake_drivers(calls),  # type: ignore[arg-type]
        targets,
        events_per_tenant=1,
    )

    assert Counter(calls) == Counter({source: 1 for source in _CONFIGURED_SOURCES})
    assert result.dispatched_by_source == {
        source: 1
        for source in _CONFIGURED_SOURCES
    }
    assert result.dispatched_by_tenant == {
        target.tenant_id: 1
        for target in targets
    }


@pytest.mark.asyncio
async def test_regular_dispatch_rejects_unsupported_source() -> None:
    with pytest.raises(ValueError, match="unsupported live source 'unknown'"):
        await _dispatch_regular(
            _fake_drivers([]),  # type: ignore[arg-type]
            _target("unknown"),
            1,
        )


@pytest.mark.asyncio
async def test_concurrent_dispatch_rejects_unsupported_source() -> None:
    with pytest.raises(ValueError, match="unsupported live source 'unknown'"):
        await dispatch_live_concurrent(
            _fake_drivers([]),  # type: ignore[arg-type]
            [_target("unknown")],
            events_per_tenant=1,
        )


@pytest.mark.asyncio
async def test_prepare_live_drivers_seeds_before_building(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, Any]] = []
    pool = object()
    targets = [_target("slack")]
    secrets = SigningSecrets()
    expected = object()
    producer = object()
    s3 = object()
    flags = object()

    async def _seed(
        got_pool: Any,
        got_targets: Any,
        *,
        secrets: Any,
    ) -> None:
        calls.append(("seed", (got_pool, got_targets, secrets)))

    async def _build(
        got_pool: Any,
        got_targets: Any,
        got_secrets: Any,
        **kwargs: Any,
    ) -> Any:
        calls.append(
            (
                "build",
                (got_pool, got_targets, got_secrets, kwargs),
            ),
        )
        return expected

    monkeypatch.setattr(composition, "seed_live_installs", _seed)
    monkeypatch.setattr(composition, "build_live_drivers", _build)

    actual = await prepare_live_drivers(
        pool,  # type: ignore[arg-type]
        targets,
        secrets,
        kafka_producer=producer,
        s3_raw_client=s3,
        tenant_flags=flags,
    )

    assert actual is expected
    assert calls == [
        ("seed", (pool, targets, secrets)),
        (
            "build",
            (
                pool,
                targets,
                secrets,
                {
                    "kafka_producer": producer,
                    "s3_raw_client": s3,
                    "tenant_flags": flags,
                },
            ),
        ),
    ]
