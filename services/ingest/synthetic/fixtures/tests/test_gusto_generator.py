from __future__ import annotations

import datetime as dt

import pytest

from services.ingest.ingestion.handlers.gusto import handle_gusto_object
from services.ingest.synthetic.fixtures.gusto_generator import make_gusto


@pytest.mark.asyncio
async def test_default_employee_occurrence_fits_certification_partitions() -> None:
    fixture = make_gusto(entities=["employee"], rows_per_entity=2)

    for employee in fixture["entities"]["employee"]:
        draft = await handle_gusto_object(
            {
                "_fyralis_record_type": "employee",
                "_fyralis_company_uuid": fixture["company_uuid"],
                "entity": employee,
            },
            {},
        )
        assert draft.occurred_at >= dt.datetime(
            2025,
            7,
            1,
            tzinfo=dt.timezone.utc,
        )
