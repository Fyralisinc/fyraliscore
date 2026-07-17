from pathlib import Path

from services.domain.models.read_shapes import ACCEPTED_MODEL_ROWS_SQL
from services.reasoning.think import reconciler


def test_deterministic_supporter_membership_does_not_read_legacy_status() -> None:
    source = Path("services/reasoning/think/deterministic.py").read_text()
    query = source.split("JOIN accepted_current_models supporter", 1)[1].split(
        '"""', 1
    )[0]
    assert "supporter.status" not in query


def test_reconciler_hydrates_payload_only_through_accepted_adapter() -> None:
    source = Path(reconciler.__file__).read_text()
    candidate_query = source.split("async def _find_candidates", 1)[1].split(
        "async def _record_event", 1
    )[0]
    assert "FROM accepted_current_models" not in candidate_query
    assert "FROM {ACCEPTED_MODEL_ROWS_SQL} AS accepted_model" in candidate_query
    assert "status = 'active'" not in candidate_query
    assert "JOIN models legacy" in ACCEPTED_MODEL_ROWS_SQL
