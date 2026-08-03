from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import orjson
import pytest

from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.worker import _normalize_one_with_envelope
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope


class _S3:
    def __init__(self, body: bytes) -> None:
        self._body = body

    async def get(self, _key: str) -> bytes:
        return self._body


class _Producer:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def produce(self, **message: Any) -> None:
        self.messages.append(message)


class _RegistryRouter:
    def __init__(self) -> None:
        self.calls = 0

    def supports(self, source: str) -> bool:
        return source == "slack"

    async def normalize(self, source, install, request, legacy_call):
        assert source == "slack"
        assert install["tenant_id"]
        assert request.ingress_kind == "webhook"
        self.calls += 1
        return await get_handler("slack:message")(
            dict(request.record.payload),
            {str(key): str(value) for key, value in request.ingress_metadata.items()},
        )


@pytest.mark.asyncio
async def test_migrated_source_normalization_resolves_through_registry_router() -> None:
    payload = {
        "event": {
            "type": "message",
            "channel": "C1",
            "user": "U1",
            "text": "registry path",
            "ts": "1747483200.001000",
            "team": "T1",
        }
    }
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
    content_hash = "a" * 40
    tenant_id = uuid4()
    key = (
        f"dev/slack/{tenant_id}/{ingested_at:%Y-%m}/aa/"
        f"{content_hash}.json"
    )
    envelope = RawEnvelope(
        source="slack",
        tenant_id=tenant_id,
        raw_s3_key=key,
        content_hash=content_hash,
        ingested_at=ingested_at,
        ingress_kind="webhook",
    )
    producer = _Producer()
    router = _RegistryRouter()

    parsed, produced = await _normalize_one_with_envelope(
        orjson.dumps(envelope.model_dump(mode="json")),
        _S3(orjson.dumps(payload)),  # type: ignore[arg-type]
        producer,  # type: ignore[arg-type]
        connector_router=router,
    )

    assert parsed == envelope
    assert produced is True
    assert router.calls == 1
    assert orjson.loads(producer.messages[0]["value"])["external_id"] == (
        "C1:1747483200.001000"
    )
