from __future__ import annotations

from datetime import datetime, timezone

from lib.shared.ids import uuid7
from services.execution.contracts import SignalEnvelope
from services.execution.routing import decide_route


def _signal(
    text: str,
    *,
    source_channel: str = "slack:message",
    trust_tier: str = "attested_agent",
    observation_kind: str = "signal",
    entities: list[dict] | None = None,
    trigger_type: str = "T1_EVENT",
) -> SignalEnvelope:
    return SignalEnvelope(
        tenant_id=uuid7(),
        signal_ref_type="observation",
        signal_id=uuid7(),
        source_channel=source_channel,
        occurred_at=datetime(2026, 5, 28, tzinfo=timezone.utc),
        summary=text,
        trust_tier=trust_tier,
        explicit_entities=tuple(entities or []),
        trigger_type=trigger_type,
        observation_kind=observation_kind,
        signal_type=f"{source_channel}/{observation_kind}",
    )


def test_low_value_chatter_routes_to_archive():
    decision = decide_route(_signal("thanks"))

    assert decision.route == "IGNORE_OR_ARCHIVE"
    assert decision.risk_level == "low"
    assert "low-value chatter" in decision.reason


def test_authoritative_state_change_routes_to_deterministic_update():
    decision = decide_route(
        _signal(
            "Alice moved ENG-123 from In Progress to Done",
            source_channel="linear:webhook",
            trust_tier="authoritative",
            observation_kind="state_change",
            entities=[{"type": "linear_issue", "id": "ENG-123"}],
        )
    )

    assert decision.route == "DETERMINISTIC_UPDATE"
    assert decision.score_breakdown["signal_kind"] > 0


def test_customer_blocker_routes_to_deep_inquiry():
    decision = decide_route(
        _signal(
            "Acme cannot launch without SSO, and Sales promised go-live this month.",
            entities=[
                {"type": "customer", "id": str(uuid7())},
                {"type": "commitment", "id": str(uuid7())},
            ],
        )
    )

    assert decision.route == "DEEP_INQUIRY_PATH"
    assert decision.risk_level in {"medium", "high"}
    assert decision.score_breakdown["risk_language"] > 0


def test_anomaly_routes_to_background_path():
    decision = decide_route(
        _signal(
            "silent disagreement cluster detected",
            source_channel="internal:anomaly",
            trust_tier="authoritative",
            observation_kind="anomaly_flagged",
        )
    )

    assert decision.route == "BACKGROUND_PATH"


def test_missing_offline_decision_routes_to_human_validation():
    decision = decide_route(
        _signal(
            "No recorded decision exists, but several changes suggest offline alignment.",
            trust_tier="attested_agent",
            entities=[{"type": "commitment", "id": str(uuid7())}],
        )
    )

    assert decision.route == "HUMAN_VALIDATION_PATH"
    assert decision.risk_level == "high"


def test_user_query_routes_to_fast_path():
    signal = SignalEnvelope(
        tenant_id=uuid7(),
        signal_ref_type="query",
        signal_id=uuid7(),
        source_channel="ui:query",
        summary="What happened with Acme?",
        trust_tier="authoritative",
        trigger_type="USER_QUERY",
    )

    decision = decide_route(signal)

    assert decision.route == "FAST_PATH"
