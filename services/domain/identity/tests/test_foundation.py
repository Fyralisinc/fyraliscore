from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError as PydanticValidationError

from lib.shared.ids import uuid7
from services.domain.identity.foundation import (
    EntityMentionCreate,
    ResolutionRunCreate,
    SourceReferenceCreate,
)


def test_source_reference_key_is_scoped_and_stable() -> None:
    tenant = uuid7()
    evidence = uuid7()
    base = SourceReferenceCreate(
        tenant_id=tenant,
        installation_scope="slack:alpen",
        source="slack",
        native_type="user",
        native_id="U42",
        reference_kind="principal",
        evidence_id=evidence,
    )
    replay = base.model_copy(update={"evidence_id": uuid7()})
    other_installation = base.model_copy(
        update={"installation_scope": "slack:other"}
    )

    assert base.computed_stable_key == replay.computed_stable_key
    assert base.computed_stable_key != other_installation.computed_stable_key


def test_mentions_are_content_addressed_and_require_provenance() -> None:
    occurred_at = datetime.now(UTC)
    value = EntityMentionCreate(
        tenant_id=uuid7(),
        observation_id=uuid7(),
        observation_occurred_at=occurred_at,
        evidence_id=uuid7(),
        mention_kind="text",
        text="the authentication audit",
        span_start=4,
        span_end=28,
        expected_types=("audit",),
    )
    assert value.computed_mention_key == value.computed_mention_key
    assert len(value.computed_mention_key) == 64

    with pytest.raises(PydanticValidationError, match="require observation"):
        EntityMentionCreate(
            tenant_id=uuid7(), mention_kind="text", text="unbound"
        )


def test_query_mentions_may_be_transient() -> None:
    mention = EntityMentionCreate(
        tenant_id=uuid7(), mention_kind="query", text="the audit"
    )
    assert mention.observation_id is None


def test_non_query_runs_require_an_observation() -> None:
    with pytest.raises(PydanticValidationError, match="require an observation"):
        ResolutionRunCreate(
            tenant_id=uuid7(),
            input_kind="observation",
            input_hash=hashlib.sha256(b"input").hexdigest(),
            resolver_name="fyralis",
            resolver_version="1",
            policy_version="1",
            capability_snapshot={"schema_version": 1},
        )


def test_source_reference_rejects_reversed_valid_time() -> None:
    now = datetime.now(UTC)
    with pytest.raises(PydanticValidationError, match="reversed"):
        SourceReferenceCreate(
            tenant_id=uuid7(),
            installation_scope="notion:alpen",
            source="notion",
            native_type="page",
            native_id="page-1",
            reference_kind="artifact",
            evidence_id=uuid7(),
            valid_from=now,
            valid_to=now - timedelta(seconds=1),
        )
