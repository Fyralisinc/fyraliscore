"""sql_emit tests — exercise emitter against a tiny bundle."""
from __future__ import annotations

from demo.generation.schemas import (
    EntityMention, GeneratedActor, GeneratedBundle, GeneratedCommitment,
    GeneratedCustomer, GeneratedDecision, GeneratedGoal,
    GeneratedRecommendation, GeneratedSignal, TargetActRef,
)
from demo.generation.sql_emit import PLACEHOLDER_TENANT_ID, emit_sql, write_sql


def _bundle() -> GeneratedBundle:
    return GeneratedBundle(
        company_id="test", ceo_actor_id="a-ceo",
        actors=[
            GeneratedActor(id="a-ceo", name="Maya P", role="founder"),
            GeneratedActor(id="a-eng", name="Bob B", role="engineer", manager_id="a-ceo"),
        ],
        customers=[
            GeneratedCustomer(id="c1", company_name="Acme'X", arr_usd=42.0,
                              segment="enterprise", current_health="healthy"),
        ],
        goals=[GeneratedGoal(id="g1", title="G", owner_id="a-ceo")],
        decisions=[GeneratedDecision(
            id="d1", title="T", decision_text="x", rationale="r",
            revisit_triggers=["t1"],
        )],
        commitments=[GeneratedCommitment(
            id="cm1", title="C", owner_id="a-eng",
            contributors=["a-ceo"], contributes_to_goal_id="g1",
            constrained_by_decision_ids=["d1"], served_by_customer_id="c1",
        )],
        signals=[GeneratedSignal(
            id="s1", source_channel="slack", source_ref="ts-1",
            author_id="a-eng", occurred_at="2026-04-01T00:00:00+00:00",
            content_text="hello", entities_mentioned=[
                EntityMention(type="commitment", id="cm1"),
            ],
        )],
        recommendations=[GeneratedRecommendation(
            id="r1", proposition_text="rec",
            target_act_ref=TargetActRef(type="commitment", id="cm1"),
            expected_impact_usd=100.0,
            supporting_observation_ids=["s1"],
            target_actor_id="a-ceo",
        )],
    )


def test_emit_contains_placeholder_tenant_and_quoted_apostrophe():
    sql = emit_sql(_bundle())
    assert PLACEHOLDER_TENANT_ID in sql
    # Apostrophes in customer names are escaped (Acme'X -> Acme''X).
    assert "Acme''X" in sql
    # Idempotency: every identified-PK insert has ON CONFLICT.
    for table in ("actors", "resources", "goals", "decisions", "commitments", "models"):
        assert f"INTO {table}" in sql
    assert "ON CONFLICT" in sql


def test_emit_dependency_order():
    sql = emit_sql(_bundle())
    # Resources before observations-with-signals; goals before commitments.
    assert sql.index("INTO actors") < sql.index("INTO observations")
    assert sql.index("INTO resources") < sql.index("INTO commitments")
    assert sql.index("INTO goals") < sql.index("INTO commitments")
    assert sql.index("INTO commitments") < sql.index("INTO models")


def test_write_sql_plain(tmp_path):
    out = tmp_path / "snap.sql"
    written = write_sql(_bundle(), out)
    assert written == out
    text = out.read_text()
    assert "BEGIN;" in text and text.rstrip().endswith("COMMIT;")
