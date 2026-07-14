from __future__ import annotations

import asyncio

from services.domain.models.repo import ModelsRepo


def test_bulk_phase_timing_sink_receives_metrics() -> None:
    events: list[dict[str, object]] = []
    repo = ModelsRepo(bulk_timing_sink=events.append)

    async def op() -> dict[str, int]:
        return {"row_count": 3}

    result = asyncio.run(
        repo._time_bulk_phase(
            "models_insert",
            model_count=2,
            stratum_index=1,
            op=op,
            metrics_from_result=lambda value: dict(value),
        )
    )

    assert result == {"row_count": 3}
    assert len(events) == 1
    assert events[0]["phase"] == "models_insert"
    assert events[0]["model_count"] == 2
    assert events[0]["stratum_index"] == 1
    assert events[0]["status"] == "ok"
    assert events[0]["row_count"] == 3
    assert isinstance(events[0]["elapsed_ms"], float)
