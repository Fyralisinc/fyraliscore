from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.platform.execution import inquiry
from services.platform.execution.config import InquiryConfig
from services.platform.execution import routing
from services.reasoning.retrieval.primary import TriggerContext


def _trigger(kind: str = "T1", text: str = "") -> TriggerContext:
    return TriggerContext(
        kind=kind,
        tenant_id=uuid4(),
        seed_entity_ids=[],
        scope_actors=[],
        seed_natural_text=text,
        seed_occurred_at=datetime.now(timezone.utc),
    )


def test_routing_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._adaptive_baseline_top_n is routing.adaptive_baseline_top_n
    assert inquiry._adaptive_evidence_limit is routing.adaptive_evidence_limit
    assert inquiry._cold_weak_noop_gate is routing.cold_weak_noop_gate
    assert inquiry._declares_no_material_update is routing.declares_no_material_update
    assert inquiry._route_for_trigger is routing.route_for_trigger
    assert inquiry._signal_class_for_trigger is routing.signal_class_for_trigger
    assert inquiry._trigger_text is routing.trigger_text


def test_route_for_trigger_keeps_background_and_deep_defaults() -> None:
    assert routing.route_for_trigger(_trigger("T2")) == "DETERMINISTIC_UPDATE"
    assert routing.route_for_trigger(_trigger("T3")) == "BACKGROUND_PATH"
    assert routing.route_for_trigger(_trigger("T4")) == "BACKGROUND_PATH"
    assert routing.route_for_trigger(_trigger("T1")) == "DEEP_INQUIRY_PATH"


def test_signal_class_for_trigger_distinguishes_weak_broad_and_material() -> None:
    assert (
        routing.signal_class_for_trigger(
            _trigger(
                text=(
                    "Workspace chatter: lunch notes, travel plans, and general team "
                    "coordination. No blocker, no owner change, no decision."
                )
            )
        )
        == "weak"
    )
    assert (
        routing.signal_class_for_trigger(
            _trigger(text="Portfolio renewal risk across all customer accounts")
        )
        == "broad"
    )
    assert (
        routing.signal_class_for_trigger(
            _trigger(text="Customer launch is blocked by a missing owner decision")
        )
        == "material"
    )


def test_cold_weak_noop_gate_requires_weak_no_update_chatter() -> None:
    trigger = _trigger(
        text=(
            "Workspace chatter: lunch notes, travel plans, and general team "
            "coordination. No blocker, no owner change, no decision."
        )
    )

    assert routing.cold_weak_noop_gate(trigger, "material") == {
        "used": False,
        "reason": "not_weak_signal",
    }
    assert routing.cold_weak_noop_gate(trigger, "weak")["used"] is True


def test_adaptive_evidence_limit_preserves_budgets() -> None:
    cfg = InquiryConfig(evidence_reservoir_limit=700, fast_path_evidence_limit=80)

    assert routing.adaptive_baseline_top_n(220, "weak") == 80
    assert (
        routing.adaptive_evidence_limit(
            cfg,
            route="DEEP_INQUIRY_PATH",
            mode="deep",
            signal_class="broad",
        )
        == 560
    )
